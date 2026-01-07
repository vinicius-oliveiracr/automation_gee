import ee
import geemap
import pandas as pd
import os
import sys
from datetime import timedelta
from shapely.geometry import mapping

class PrecipitationDownloader:    
    def __init__(self, config):
        self.config = config
        self.csv_folder = os.path.join(self.config.exit_path, "csv_blocks")
        os.makedirs(self.csv_folder, exist_ok=True)
        print("PrecipitationDownloader initilized.")

    def download_data(self, gdf_wgs84):
        try:
            print("\n--- Data extraction from Google Earth Engine initialized---")
            
            print("Preparing FeatureCollection for GEE...")
            fc = self._create_feature_collection(gdf_wgs84)
            
            current_start = self.config.start_date

            while current_start < self.config.end_date:
                current_end = min(current_start + timedelta(days=30), self.config.end_date)
                print(f"Processing precipitation from {current_start.date()} to {current_end.date()}...")
                
                csv_filename = f"precip_{current_start.date()}_{(current_end - timedelta(days=1)).date()}.csv"
                csv_path = os.path.join(self.csv_folder, csv_filename)

                if os.path.exists(csv_path):
                    print(f"Data already collected, skipping {csv_filename}")
                    current_start = current_end
                    continue
                image_collection = self._get_image_collection(fc, current_start, current_end)
                
                all_stats = image_collection.map(lambda img: self._zonal_mean(img, fc)).flatten()
                
                df_block = geemap.ee_to_df(all_stats)
                df_block.to_csv(csv_path, index=False)
                print(f"CSV salvo: {csv_path}")
                
                current_start = current_end
            
            self._merge_csvs()

        except Exception as e:
            print(f"FATAL ERROR during download from GEE: {e}")
            sys.exit(1)

    def _create_feature_collection(self, gdf_wgs84):
        features = []
        for _, row in gdf_wgs84.iterrows():
            geom = ee.Geometry(mapping(row.geometry))
            props = row.drop("geometry").to_dict()
            features.append(ee.Feature(geom, props))
        return ee.FeatureCollection(features)

    def _get_image_collection(self, fc, start_date, end_date):
        return (
            ee.ImageCollection("UCSB-CHG/CHIRPS/DAILY")
            .filterBounds(fc)
            .filterDate(start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))
            .select("precipitation")
            .map(lambda img: img.set("date", img.date().format("YYYY-MM-dd")))
        )

    def _zonal_mean(self, img, fc):
        stats = img.reduceRegions(
            collection=fc,
            reducer=ee.Reducer.mean(),
            scale=10000
        )
        stats = stats.map(lambda f: f.set("date", img.get("date")))
        return stats.map(lambda f: ee.Feature(f.geometry(), {
            "raster_val": f.get("raster_val"),
            "date": f.get("date"),
            "precipitation": f.get("mean")
        }))

    def _merge_csvs(self):
        print("Unificando arquivos CSV...")
        csv_files = [os.path.join(self.csv_folder, f) for f in os.listdir(self.csv_folder) if f.endswith(".csv")]
        
        if not csv_files:
            print("Aviso: Nenhum CSV de precipitação foi gerado.")
            return

        all_data = pd.concat([pd.read_csv(f) for f in csv_files], ignore_index=True)
        final_csv_path = os.path.join(self.config.exit_path, "precipitacao_diaria_subbacias_unificado.csv")
        all_data.to_csv(final_csv_path, index=False)
        print(f"✅ CSV Unificado salvo em: {final_csv_path}")