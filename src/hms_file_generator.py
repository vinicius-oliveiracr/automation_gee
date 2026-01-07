from jinja2 import Template
from datetime import datetime
import logging
import locale
import os

class HmsFileGenerator:
    def __init__(self, config):
        self.config = config
        logging.info("HmsFileGenerator initialized.")

    def _format_hec_date(self, date_obj):
        months = {
            1: "January", 2: "February", 3: "March", 4: "April", 5: "May", 6: "June",
            7: "July", 8: "August", 9: "September", 10: "October", 11: "November", 12: "December"
        }

        return f"{date_obj.day:02d}{months[date_obj.month]}{date_obj.year}"

    def generate_gage_file(self, gage_data: list):
        if not gage_data:
            logging.warning("No entry for gage file. ")
            return
        
        now = datetime.now()
        str_date = self._format_hec_date(now)
        str_time = now.strftime("%H:%M")

        for g in gage_data:
            g['date'] = str_date
            g['time'] = str_time

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
                logging.warning(f"Error processing gage '{entry['name']}': {e}")
        

        template = Template(self.config.MET_TEMPLATE)
        output_context = template.render(
            met_name = self.config.MET_MODEL_NAME,
            basin_name = self.config.BASIN_MODEL_NAME,
            subbasins = assignments,
            dt = datetime.now()
        )

        output_context = output_context.replace('\t', '     ')

        try:
            with open(self.config.met_file, 'w') as file:
                file.write(output_context)
                logging.info(f"Met file created successfully at {self.config.met_file}.")
        except IOError as e:
            logging.error(f"Writing met file failed: {e}")

    def generate_control_file(self):
        start_str = self._format_hec_date(self.config.start_date)
        end_str = self._format_hec_date(self.config.end_date)

        control_content = f"""Control: Control_file
    Start Date: {start_str}
    Start Time: 00:00
    End Date: {end_str}
    End Time: 00:00
    Time Interval: 1440
End:
"""
            
        try:
            with open(self.config.control_file, 'w') as f:
                f.write(control_content)
            logging.info(f"Control file created successfully: {self.config.control_file}")
        except IOError as e:
            logging.error(f"Failed to create control file: {e}.")