import os
import sys
from dotenv import load_dotenv
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

class Config:
    def __init__(self):
        load_dotenv()

        self.old_gage_file = os.getenv("OLD_GAGE_FILE")
        self.dss_file = os.getenv("DSS_FILE")
        self.shp_path = os.getenv("FILE_PATH")
        self.exit_path = os.getenv("EXIT_PATH")
        self.gcn_raster_path = os.path.join(BASE_DIR, "assets", "GCN.tif")
        self.api_key = os.getenv("API_KEY")
        self.account = os.getenv("EE_ACCOUNT")
        self.private_key_path = os.getenv("PRIVATE_KEY_PATH")
        self.project_name = os.getenv("PROJECT_NAME")

        self.start_date = datetime(2018, 1, 1)
        self.end_date = datetime(2022, 12, 31)

        if not all([self.private_key_path, self.project_name, self.shp_path, self.exit_path]):
            print("ERROR: Essencial variables (PRIVATE_KEY_PATH, PROJECT_NAME, FILE_PATH, EXIT_PATH) not found in .env file.")
            sys.exit(1)


class DssConfig:
    def __init__(self):
        load_dotenv()

        self.csv_file = os.getenv("CSV_FILE")
        self.dss_file = os.getenv("DSS_FILE")
        self.gage_file = os.getenv("GAGE_FILE")
        self.met_file = os.getenv("MET_FILE")

        if not all ([self.csv_file, self.dss_file, self.gage_file, self.met_file]):
            print("ERROR: Variables missing (CSV_FILE, DSS_FILE, GAGE_FILE, MET_FILE) at .env file.")

        self.B_PART = "PRECIP"
        self.C_PART = "OBS"
        self.E_PART = "1DAY"
        self.F_PART = "GPM-AUTOMATION"
        self.INTERVAL_MINUTES = 1440
        self.DATA_TYPE = "PER-INC"
        self.UNITS = "MM"

        self.MET_MODEL_NAME = "met_automatico"
        self.BASIN_MODEL_NAME = "bacia_automatica"

        self.GAGE_TEMPLATE = """<?xml version="1.0" encoding="UTF-8"?>
<Gages>
{% for g in gages -%}
    <Gage id="{{ g.id }}" name="{{ g.name }}">
        <timeSeries file="{{ g.file }}" pathname="{{ g.pathname }}"/>
    </Gage>
{% endfor %}
</Gages>
"""

        self.MET_TEMPLATE = """Meteorology: {{ met_name }}
    Description: Met model gerado automaticamente
    Last Modified Date: {{ dt.strftime('%d %B %Y') }}
    Last Modified Time: {{ dt.strftime('%H:%M:%S') }}
    Version: 4.11
    Unit System: Metric
    Set Missing Data to Default: No
    Precipitation Method: Specified Hyetograph
    Air Temperature Method: None
    Atmospheric Pressure Method: None
    Dew Point Method: None
    Wind Speed Method: None
    Shortwave Radiation Method: None
    Longwave Radiation Method: None
    Snowmelt Method: None
    Evapotranspiration Method: No Evapotranspiration
    Use Basin Model: {{ basin_name }}
End:

{% for item in subbasins %}
Subbasin: {{ item.subbasin }}
    Gage: {{ item.gage }}
End:
{% endfor %}
"""