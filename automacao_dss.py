import pandas as pd
from pydsstools.heclib.dss import HecDss
from pydsstools.core import TimeSeriesContainer
from jinja2 import Template
import os
import dotenv
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

dotenv.load_dotenv()

#PATHS
csv_file = os.getenv("CSV_FILE")
dss_file = os.getenv("DSS_FILE")
gage_file = os.getenv("GAGE_FILE")
met_file = os.getenv("MET_FILE")

#DSS PATH CONSTANTS

B_PART = 'PRECIP'
C_PART = 'OBS'
E_PART = '1DAY'
F_PART = 'GPM-AUTOMATION'
INTERVAL_MINUTES = 1440
DATA_TYPE = 'PER-INC'
UNITS = 'MM'

#GAGE TEMPLATE

GAGE_TEMPLATE = """<?xml version="1.0" encoding="UTF-8"?>
<Gages>
{% for g in gages -%}
    <Gage id="{{ g.id }}" name="{{ g.name }}">
        <timeSeries file="{{ g.file }}" pathname="{{ g.pathname }}"/>
    </Gage>
{% endfor %}
</Gages>
"""

MET_TEMPLATE = """Meteorology: {{ met_name }}
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

def create_dss_and_gage_data (csv_path:str, dss_path:str) -> list:
    try:
        df = pd.read_csv(csv_path, parse_dates=['date'])
    except FileNotFoundError:
            logging.error(f"CSV file not found at: {csv_path}")

    try:
        os.remove(dss_path)
    except FileNotFoundError:
        logging.info("No previous dss file to remove.")
    except PermissionError:
        logging.error(f"Not allowed to remove file {dss_path}, as it must be in use.")
        return []
    
    gage_entries = []

    with HecDss.Open(dss_path, version=7) as dss:
        for i, (subbasin, sub_df) in enumerate(df.groupby("raster_val")):
            if sub_df.empty:
                continue

            sub_df = sub_df.sort_values("date")
            values = sub_df['precipitation'].values.astype(float)
            start_date = sub_df['date'].iloc[0]

            pathname = f"/{subbasin}/{B_PART}/{C_PART}/{start_date.strftime('%d%b%Y').upper()}/{E_PART}/{F_PART}/"

            tsc = TimeSeriesContainer()
            tsc.pathname = pathname
            tsc.startDateTime = start_date.strftime("%d%b%Y %H:%M:%S").upper()
            tsc.numberValues = len(values)
            tsc.values = values
            tsc.units = UNITS
            tsc.type = DATA_TYPE
            tsc.interval = INTERVAL_MINUTES

            try:
                dss.put_ts(tsc)
                print(f"Successfully saving data for the sub-basin {subbasin} with pathname: {pathname}")

                gage_entries.append({
                    "id": f"Gage-{i}",
                    "name": f"S_{subbasin}",
                    "file": os.path.basename(dss_file),
                    "pathname": pathname
                })

            except Exception as e:
                print(f"Error while saving data for sub-basin {subbasin}: {e}")
                continue

    return gage_entries

def generate_gage_file (gage_path: str, gage_data: list):
    if not gage_data:
        logging.warning("No entries for gage file. Task will not continue.")
        return
    template = Template(GAGE_TEMPLATE)
    output = template.render(gages=gage_data)

    try:
        with open(gage_path, 'w') as f:
            f.write(output)
            logging.info(f"Gage file created successfully at {gage_path}.")
    except IOError as e:
        logging.error(f"Failed writting gage file: {e}.")

def generate_met_file (output_path, met_model_name, basin_model_name, subbasin_count):
    assignments = []
    for i in range(1, subbasin_count + 1):
        assignments.append({
            "subbasin": f"Subbasin-{i}",
            "gage": f"S_{i}"
        })

    template = Template(MET_TEMPLATE)
    output_context = template.render(
        met_name = met_model_name,
        basin_name = basin_model_name,
        subbasins = assignments,
        dt = datetime.now()
    )

    try:
        with open(output_path, 'w') as f:
            f.write(output_context)
            print(f"Met file created successfully at {output_path}")
    except IOError as e:
            print(f"Failed to write met file: {e}")

if __name__ == "__main__":
    logging.info("Automation process initiated.")

    gage_data_list = create_dss_and_gage_data(csv_file, dss_file)

    generate_gage_file(gage_file, gage_data_list)

    if gage_data_list:
        met_name = "met_model"
        subbasin_count = len(gage_data_list)
        basin_name = "PDS"

    generate_met_file(met_file, met_name, basin_name, subbasin_count)


    logging.info(f"Generating .met file for {subbasin_count} subbasins.")

    logging.info("Process finalized.")