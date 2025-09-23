import pandas as pd
from jinja2 import Template
import geopandas as gpd

csv_file = r"saidas/precipitacao_diaria_subbacias_unificado.csv"
gage_file = r"C:/vinicius/automacao_gee/saidas/gage.gage"
subbasin_file = r"C:/vinicius/automacao_gee/saidas/subbacias_corrigidas_com_gcn.geojson"

df = pd.read_csv(csv_file)

gdf = gpd.read_file(subbasin_file)

gdf['area_sqkm'] = gdf['geometry'].area/1e6

