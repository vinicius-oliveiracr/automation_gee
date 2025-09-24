import pandas as pd
from pydsstools.heclib.dss import HecDss
from pydsstools.core import TimeSeriesContainer
from jinja2 import Template
import os
import dotenv

dotenv.load_dotenv()

#PATHS
csv_file = os.getenv("CSV_FILE")
dss_file = os.getenv("DSS_FILE")
gage_file = os.getenv("GAGE_FILE")

if os.path.exists(dss_file):
    try:
        os.remove(dss_file)
    except Exception as e:
        print(f"Unable to delete dss file: {e}")

df = pd.read_csv(csv_file, parse_dates=['date'])

with HecDss.Open(dss_file) as dss:

    dss = HecDss.Open(dss_file, version=7)

    try:
        paths_to_delete = dss.getPathnameList("")
        print("Searching and deleting existing paths...")
        for path in paths_to_delete:
            if "/PRECIP//" in path.upper():
                print(f"Deletando {path}")
                dss.deletePathname(path)
    except Exception as e:
        print(f"Unable to delete existing paths: {e}")

    gage_entries = []

    for i, (subbasin, sub_df) in enumerate(df.groupby("raster_val")):
        sub_df = sub_df.sort_values("date")
        values = sub_df["precipitation"].values.astype(float)
        start_date = sub_df['date'].iloc[0]  # PRIMEIRA DATA DA SUB-BACIA

        pathname = f"/{subbasin}/PRECIP/OBS/{start_date.strftime('%d%b%Y').upper()}/1DAY/OBS/"

        tsc = TimeSeriesContainer()
        tsc.pathname = pathname
        tsc.startDateTime = start_date.strftime("%d%b%Y %H:%M:%S").upper()
        tsc.numberValues = len(values)
        tsc.values = values
        tsc.units = "MM"
        tsc.type = "PER-INC"
        tsc.interval = 1440

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

    if gage_entries:
        last_pathname = gage_entries[-1]["pathname"]
        ts_test = dss.read_ts(last_pathname)
        print(f"\nSuccessfully reading test data: {ts_test.numberValues}")
        print(f"First 5 values: {ts_test.values[:5]}")

dss.close()

template_str = """
    <?xml version="1.0" encoding="UTF-8"?>
<Gages>
{% for g in gages -%}
    <Gage id="{{ g.id }}" name="{{ g.name }}">
        <timeSeries file="{{ g.file }}" pathname="{{ g.pathname }}"/>
    </Gage>
{% endfor %}
</Gages>
"""

gage_template = """<?xml version="1.0" encoding="UTF-8"?>
<Gages>
{% for g in gages -%}
    <Gage id="{{ g.id }}" name="{{ g.name }}">
        <timeSeries file="{{ g.file }}" pathname="{{ g.pathname }}"/>
    </Gage>
{% endfor %}
</Gages>
"""

if gage_entries:
    template = Template(template_str)
    output = template.render(gages=gage_entries)

    with open(gage_file,"w", encoding="utf-8") as f:
        f.write(output)
        print(f"Gage file created: {gage_file}")
else:
    print("No entries at the gage file. Check your CSV.")

print(csv_file, dss_file, gage_file)