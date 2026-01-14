import sys
import logging
from config import Config, DssConfig
from auth import initialize_gee
from geoprocessor import Geoprocessor
from data import PrecipitationDownloader
from dss_generator import DssGenerator
from hms_file_generator import HmsFileGenerator


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    force=True)

def gee_workflow():
    print("---Initializing hydrologic automation ---")

    try:
        config = Config()

        initialize_gee(config)

        processor = Geoprocessor(config)

        gdf_wgs84 = processor.run_all()

        downloader = PrecipitationDownloader(config)
        downloader.download_data(gdf_wgs84)

        print("\n Script finalized successfully.")
    except Exception as e:
        print(f"Fatal error at workflow: {e}")
        sys.exit(1)

def dss_workflow():
    logging.info("DSS/HMS automation proccess initialized.")

    try:
        config = DssConfig()

        dss_generator = DssGenerator(config)
        gage_data_list = dss_generator.get_dss()

        if not gage_data_list:
            logging.warning("No gage data was created. Proccess will be interrupted.")
            return

        file_generator = HmsFileGenerator(config)
        file_generator.generate_gage_file(gage_data_list)
        file_generator.generate_met_file(gage_data_list)
        file_generator.generate_control_file()

        logging.info(f"Generating all files for {len(gage_data_list)} subbasins.")
        print("Process finalized successfully.")

    except Exception as e:
        logging.error(f"An error has ocurred during workflow: {e}")
        sys.exit(1)

if __name__ == "__main__":
    gee_workflow()
    dss_workflow()

    print("\n ---- Full process finalized! ----")