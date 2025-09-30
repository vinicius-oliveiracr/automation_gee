import pandas as pd
from pydsstools.heclib.dss import HecDss
from pydsstools.core import TimeSeriesContainer
from jinja2 import Template
import os
import dotenv
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

dotenv.load_dotenv()

#PATHS
csv_file = os.getenv("CSV_FILE")
dss_file = os.getenv("DSS_FILE")
gage_file = os.getenv("GAGE_FILE")

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
        with open(gage_path, 'w', encoding='utf-8') as f:
            f.write(output)
            logging.info(f"Gage file created successfully at {gage_path}.")
    except IOError as e:
        logging.error(f"Failed writting gage file: {e}.")

if __name__ == "__main__":
    logging.info("Automation process initiated.")

    gage_data_list = create_dss_and_gage_data(csv_file, dss_file)

    generate_gage_file(gage_file, gage_data_list)

    logging.info("Process finalized.")