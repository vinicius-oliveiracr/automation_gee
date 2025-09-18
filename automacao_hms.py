import pandas as pd
from pydsstools.heclib.dss import HecDss
from pydsstools.core import TimeSeriesContainer
from jinja2 import Template
import os

#PATHS
csv_file = r"saidas/precipitacao_diaria_subbacias_unificado.csv"
dss_file = r"C:/vinicius/automacao_gee/saidas/precipitation.dss"
gage_file = r"C:/vinicius/automacao_gee/saidas/gage.gage"

df = pd.read_csv(csv_file, parse_dates=['date'])

period = pd.date_range(start="2025-01-01", end="2025-08-01")

with HecDss.Open(dss_file) as dss:

    try:
        paths_to_delete = dss.getPathnameList("")
        print(f"Buscando e deletando caminhos existentes...")
        for path in paths_to_delete:
            if "/PRECIP//" in path:
                dss.deletePathname(path)
    except Exception as e:
        print(f"Aviso: Não foi possível deletar caminhos antigos. Causa: {e}")

    gage_entries = []

    for i, (subbasin, sub_df) in enumerate(df.groupby("raster_val")):
        sub_df = sub_df.set_index("date").reindex(period, fill_value=0).reset_index()
        sub_df.rename(columns={"index": "date"}, inplace=True)
        values = sub_df["precipitation"].values.astype(float)
        start_date = period[0]

        pathname = f"/{subbasin}/PRECIP/OBS//1DAY/OBS/"

        tsc = TimeSeriesContainer()
        tsc.pathname = pathname
        tsc.startDateTime = start_date.strftime("%d%b%Y %H:%M:%S").upper()
        tsc.numberValues = len(values)
        tsc.values = values
        tsc.units = "MM"
        tsc.type = "PER-CUM"
        tsc.interval = 1440
        
        try:
            dss.put_ts(tsc)
            print(f"Dados salvos com sucesso para a sub-bacia {subbasin} com o pathname: {pathname}")

        except Exception as e:
            print(f"Erro ao salvar dados para a sub-bacia {subbasin}: {e}")
            # Pular para a próxima sub-bacia se houver um erro de escrita
            continue

        gage_entries.append({
            "id": f"Gage-{i}",
            "name": f"S_{subbasin}",
            "file": os.path.basename(dss_file),
            "pathname": pathname
        })

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

if gage_entries:
    last_pathname = gage_entries[-1]["pathname"]
    with HecDss.Open(dss_file) as dss:
        ts_test = dss.read_ts(last_pathname)
        print(f"\nLeitura de teste bem-sucedida. Número de valores: {ts_test.numberValues}")
        print(f"Primeiros 5 valores: {ts_test.values[:5]}")

# ---------------- CRIAÇÃO DO ARQUIVO GAGE ----------------
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

if gage_entries:
    template = Template(template_str)
    output = template.render(gages=gage_entries)

    with open(gage_file,"w", encoding="utf-8") as f:
        f.write(output)
        print(f"Arquivo Gage criado: {gage_file}")
else:
    print("Nenhuma entrada de gage foi criada. Verifique o CSV.")