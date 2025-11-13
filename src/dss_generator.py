import pandas as pd
from pydsstools.heclib.dss import HecDss
from pydsstools.core import TimeSeriesContainer
import os
import logging

class DssGenerator:
    def __init__(self, config):
        self.config = config
        logging.info("DssGenerator initialized.")

    def create_dss_file(self) -> list:
        try:
            df = pd.read_csv(self.config.csv_file, parse_dates=['date'])
        except FileNotFoundError:
            logging.error(f"CSV file not found at {self.config.csv_file}.")
            return []
        
        logging.info(f"Original data on csv file: {len(df)} lines.")

        filtered_df = df[
            (df['date'] >= self.config.start_date) & (df['date'] <= self.config.end_date)
        ]

        logging.info(f"Data after filtering ({self.config.start_date} to {self.config.end_date}): {len(filtered_df)} lines.")

        if filtered_df.empty:
            logging.warning("No data found in CSV for the time period.")
            return []
        
            
        self.remove_old_dss()

        gage_entries = []
        dss_path = self.config.dss_file

        with HecDss.Open(dss_path, version=7) as dss:
            for i, (subbasin_id, sub_df) in enumerate(filtered_df.groupby("raster_val")):
                if sub_df.empty:
                    continue
                sub_df = sub_df.sort_values("date")
                values = sub_df['precipitation'].values.astype(float)
                start_date = sub_df['date'].iloc[0]

                pathname = (
                            f"/{subbasin_id}/{self.config.B_PART}/{self.config.C_PART}/"
                            f"{start_date.strftime('%d%b%Y').upper()}/"
                            f"{self.config.E_PART}/{self.config.F_PART}/"
                        )
                tsc = self.build_tsc(pathname, start_date, values)

                try:
                    dss.put_ts(tsc)
                    logging.info(f"Data saved for subbasin {subbasin_id}.")

                    gage_entries.append({
                        "id": f"Gage-{i}",
                        "name": f"S_{subbasin_id}",
                        "file": os.path.basename(dss_path),
                        "pathname": pathname
                        })
                except Exception as e:
                    logging.error(f"Error while saving data for subbasin {subbasin_id}: {e}")
                    continue

            logging.info(f"DSS file created at {dss_path} with {len(gage_entries)} entries ")
            return gage_entries
        
    def remove_old_dss(self):
        try:
            os.remove(self.config.dss_file)
            logging.info(f"Old dss file removed: {self.config.dss_file}")
        except FileNotFoundError:
            logging.info("No previous DSS file to remove.")
        except PermissionError:
            logging.error(f"Permission denied: It was not possible to remove {self.config.dss_file}. The file is under use.")
            raise

    def build_tsc(self, pathname, start_date, values) -> TimeSeriesContainer:
        tsc = TimeSeriesContainer()
        tsc.pathname = pathname
        tsc.startDateTime = start_date.strftime("%d%b%Y 24:00:00").upper()
        tsc.numberValues = len(values)
        tsc.values = values
        tsc.units = self.config.UNITS
        tsc.type = self.config.DATA_TYPE
        tsc.interval = self.config.INTERVAL_MINUTES

        return tsc