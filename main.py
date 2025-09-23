import pandas as pd
import numpy as np
import geopandas as gpd
import rasterio
from shapely.geometry import mapping
from dotenv import load_dotenv
from rasterio.features import rasterize
from rasterio.transform import from_bounds
from rasterstats import zonal_stats
import ee
import os
import json
from google.oauth2 import service_account
from datetime import datetime, timedelta
import geemap



# CREDENTIALS

load_dotenv()
api_key = os.getenv("API_KEY")
account = os.getenv("EE_ACCOUNT")
private_key_path = os.getenv("PRIVATE_KEY_PATH")
shp_path = os.getenv("FILE_PATH")
project_name = os.getenv("PROJECT_NAME")
exit_path = os.getenv("EXIT_PATH")
folder_id = os.getenv("FOLDER_ID")
folder_name = os.getenv("FOLDER_NAME")

#AUTH
try:
    credentials = service_account.Credentials.from_service_account_file(
        private_key_path,
        scopes=[
            'https://www.googleapis.com/auth/earthengine',
            'https://www.googleapis.com/auth/cloud-platform'
        ]
    )

    ee.Initialize(credentials, project=project_name)
    print("✅ Earth Engine autenticado com sucesso usando google-auth!")

except Exception as e:
    print(f"Erro na autenticação do EE: {e}")

try:
    # CREATING GEODATAFRAME WITH GEOPANDAS
    gdf = gpd.read_file(shp_path)
    crs = gdf.to_crs("EPSG:4326")
    gdf['geometry'] = gdf['geometry'].buffer(0)

    #USING WGS84 TO EXTRACT DATA FROM GEE

    gdf_wgs84 = gdf.to_crs("EPSG:4326")
    gdf_wgs84["geometry"] = gdf_wgs84["geometry"].buffer(0)

    if "raster_val" not in gdf_wgs84.columns:
        gdf_wgs84['raster_val'] = np.arange(1, len(gdf_wgs84) + 1)

    #SAVING AS GEOJSON

    geojson_filename = "subbasins-pds.geojson"
    gdf_wgs84.to_file(geojson_filename, driver="GeoJSON")
    gdf_wgs84["geometry"] = gdf_wgs84["geometry"].simplify(
        tolerance=0.001, preserve_topology=True
    )

    geojson = gdf_wgs84.to_json()

    geojson_dict = json.loads(geojson)

    features = [
        ee.Feature(f["geometry"], f["properties"]) for f in geojson_dict["features"]
    ]

    #TRANSFORMING geoJSON INTO raster FILE
    gdf_m = gdf_wgs84.to_crs(gdf_wgs84.estimate_utm_crs())

    resolution = 1000

    minx, miny, maxx, maxy = gdf_m.total_bounds
    width = int((maxx - minx) / resolution)
    height = int((maxy - miny) / resolution)
    transform = from_bounds(minx, miny, maxx, maxy, width, height)


    shapes = [(geom, value) for geom,value in zip(gdf_m.geometry, gdf_m['raster_val'])]

    #RASTERIZE

    raster = rasterize(
        shapes,
        out_shape=(height, width),
        transform= transform,
        fill=0,
        dtype='uint16'
    )

    #SAVE GEOTIF

    output_tif = f"{exit_path}/subbasin_mask.tif"

    with rasterio.open(
        output_tif,
        'w',
        driver="GTiff",
        height= height,
        width=width,
        count=1,
        dtype= raster.dtype,
        crs=gdf_wgs84.crs,
        transform=transform,
    ) as dst:
        dst.write(raster, 1)

    print(f"✅ Raster criado e salvo como: {output_tif}")

    gcn_raster_path = os.path.join("assets", "GCN.tif")

    with rasterio.open(gcn_raster_path) as src:
        raster_crs = src.crs

    gdf_proj = gdf_wgs84.to_crs(raster_crs).copy()

    gcn_stats = zonal_stats(
        gdf_proj,
        gcn_raster_path,
        stats=['mean'],
        geojson_out = True,
        nodata = 0
    )

    gdf_proj['gcn'] = [stat['properties']['mean'] if stat['properties']['mean'] is not None else 0 for stat in gcn_stats]
    
    gdf.to_file("saidas/subbacias_corrigidas_com_gcn.geojson", driver="GeoJSON")

    
except Exception as e:
    print(f"Error:{e}.")


#GETTING PRECIPITATION DATA FROM GOOGLE EARTH ENGINE

start_date = datetime(2024,1,1)
end_date = datetime(2025,1,1)

    
csv_folder = os.path.join(exit_path, "csv_blocks")
os.makedirs(csv_folder, exist_ok=True)

current_start = start_date

while current_start < end_date:
    # Define o fim do bloco de 30 dias
    current_end = min(current_start + timedelta(days=30), end_date)
    print(f"Processando dados de precipitação entre {current_start.date()} e {current_end.date()}")

    # Cria FeatureCollection apenas uma vez
    fc = ee.FeatureCollection([
        ee.Feature(ee.Geometry(mapping(row.geometry)), row.drop("geometry").to_dict())
        for _, row in gdf_wgs84.iterrows()
    ])

    # Cria ImageCollection filtrada para o bloco de datas
    image_collection = (
        ee.ImageCollection("UCSB-CHG/CHIRPS/DAILY")
        .filterBounds(fc)
        .filterDate(current_start.strftime("%Y-%m-%d"), current_end.strftime("%Y-%m-%d"))
        .select("precipitation")
        .map(lambda img: img.set("date", img.date().format("YYYY-MM-dd")))
    )

    # Função para calcular média zonal
    def zonal_mean(img):
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

    # Aplica zonal_mean a todos os dias do bloco
    all_stats = image_collection.map(zonal_mean).flatten()

    # Converte para pandas e salva CSV
    csv_path = os.path.join(csv_folder, f"precip_{current_start.date()}_{(current_end - timedelta(days=1)).date()}.csv")
    df_block = geemap.ee_to_df(all_stats)
    df_block.to_csv(csv_path, index=False)
    print(f"CSV salvo: {csv_path}")

    # Avança para o próximo bloco
    current_start = current_end

csv_files = [os.path.join(csv_folder, f) for f in os.listdir(csv_folder) if f.endswith(".csv")]
all_data = pd.concat([pd.read_csv(f) for f in csv_files], ignore_index=True)

final_csv_path = os.path.join(exit_path, "precipitacao_diaria_subbacias_unificado.csv")
all_data.to_csv(final_csv_path, index=False)
print(f"✅ CSV final unificado criado: {final_csv_path}")

