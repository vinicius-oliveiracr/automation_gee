import pandas as pd
from pydsstools.heclib.dss import HecDss
from pydsstools.core import TimeSeriesContainer
from jinja2 import Template
import os

#PATHS
csv_file = r"saidas/precipitacao_diaria_subbacias_unificado.csv"
dss_file = r"C:/vinicius/automacao_gee/saidas/precipitation.dss"
gage_file = r"C:/vinicius/automacao_gee/saidas/gage.gage"

if os.path.exists(dss_file):
    try:
        os.remove(dss_file)
    except Exception as e:
        print(f"Não foi possível apagar o arquivo dss. Causa: {e}")

df = pd.read_csv(csv_file, parse_dates=['date'])

with HecDss.Open(dss_file) as dss:

    dss = HecDss.Open(dss_file, version=7)

    try:
        paths_to_delete = dss.getPathnameList("")
        print(f"Buscando e deletando caminhos existentes...")
        for path in paths_to_delete:
            if "/PRECIP//" in path.upper():
                print(f"Deletando {path}")
                dss.deletePathname(path)
    except Exception as e:
        print(f"Aviso: Não foi possível deletar caminhos antigos. Causa: {e}")

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
            print(f"Dados salvos com sucesso para a sub-bacia {subbasin} com o pathname: {pathname}")

            gage_entries.append({
                "id": f"Gage-{i}",
                "name": f"S_{subbasin}",
                "file": os.path.basename(dss_file),
                "pathname": pathname
            })

        except Exception as e:
            print(f"Erro ao salvar dados para a sub-bacia {subbasin}: {e}")
            continue

    if gage_entries:
        last_pathname = gage_entries[-1]["pathname"]
        ts_test = dss.read_ts(last_pathname)
        print(f"\nLeitura de teste bem-sucedida. Número de valores: {ts_test.numberValues}")
        print(f"Primeiros 5 valores: {ts_test.values[:5]}")

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

# ---------------- CRIAÇÃO DO ARQUIVO GAGE ----------------
gage_template = """<?xml version="1.0" encoding="UTF-8"?>
<Gages>
{% for g in gages -%}
    <Gage id="{{ g.id }}" name="{{ g.name }}">
        <timeSeries file="{{ g.file }}" pathname="{{ g.pathname }}"/>
    </Gage>
{% endfor %}
</Gages>
"""

output_file = "C:/vinicius/automacao_gee/saidas/gage.gage"

if gage_entries:
    template = Template(template_str)
    output = template.render(gages=gage_entries)

    with open(output_file,"w", encoding="utf-8") as f:
        f.write(output)
        print(f"Arquivo Gage criado: {output_file}")
else:
    print("Nenhuma entrada de gage foi criada. Verifique o CSV.")