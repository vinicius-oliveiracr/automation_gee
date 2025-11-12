from jinja2 import Template
from datetime import datetime
import logging

class HmsFileGenerator:
    def __init__(self, config):
        self.config = config
        logging.info("HmsFileGenerator initialized.")

    def generate_gage_file(self, gage_data: list):
        if not gage_data:
            logging.warning("No entry for gage file. ")
            return

        template = Template(self.config.GAGE_TEMPLATE)
        output = template.render(gages= gage_data)

        try:
            with open(self.config.gage_file, 'w') as f:
                f.write(output)
            logging.info(f"Gage file created successfully at {self.config.gage_file}.")
        except IOError as e:
            logging.error(f"Unable to write gage file: {e}")

    def generate_met_file(self, gage_data: list):
        subbasin_count = len(gage_data)
        if subbasin_count == 0:
            logging.warning("No subbasin found. Met file will not be created.")
            return
        
        assignments = []
        for entry in gage_data:
            try:
                subbasin_id = entry['name'].split('_')[-1]
                assignments.append({
                    'subbasin': f"Subbasin-{subbasin_id}",
                    "gage": entry['name']
                })
            except Exception as e:
                logging.warning(f"It was not possible to proccess gage name '{entry['name']}': {e}.")
        template = Template(self.config.MET_TEMPLATE)
        output_context = template.render(
            met_name = self.config.MET_MODEL_NAME,
            basin_name = self.config.BASIN_MODEL_NAME,
            subbasins = assignments,
            dt = datetime.now()
        )

        try:
            with open(self.config.met_file, 'w') as file:
                file.write(output_context)
                logging.info(f"Met file created successfully at {self.config.met_file}.")

        except IOError as e:
            logging.error(f"Writing met file failed: {e}.")
            