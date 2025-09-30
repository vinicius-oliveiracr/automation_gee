import pandas as pd
from jinja2 import Template
import geopandas as gpd
import os
import dotenv
import xml.etree.ElementTree as ET
from pathlib import Path
import rasterio

dotenv.load_dotenv()

csv_file = os.getenv("CSV_FILE")
dss_file = os.getenv("DSS_FILE")
gage_file = os.getenv("GAGE_FILE")
geotiff_path = os.getenv("GEOTIFF_PATH")
subbasin_file = os.getenv("SUBBASIN_FILE")

#Reading data

df = pd.read_csv(csv_file)
gdf = gpd.read_file(subbasin_file)

gdf['area_sqkm'] = gdf['geometry'].area/1e6

def create_terrain_file(geotiff_path, terrain_path, name="terrain"):

    geotiff_path = geotiff_path

    with rasterio.open(geotiff_path) as src:
        crs = src.crs.to_string() if src.crs else "Unknown"

    root = ET.Element("TerrainData")
    ET.SubElement(root, "Name").text = name
    ET.SubElement(root, "Description").text = f"Terrain generated from {geotiff_path}"
    ET.SubElement(root, "FileFormat").text = "GeoTIFF"
    ET.SubElement(root, "FileName").text = str(geotiff_path)
    ET.SubElement(root, "SpatialReference").text = crs

    tree = ET.ElementTree(root)
    tree.write(terrain_path, encoding="utf-8", xml_declaration=True)

    print(f"Arquivo .terrain salvo em {terrain_path}.")

