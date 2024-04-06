"""
Ingestión de datos - Reporte de clusteres
-----------------------------------------------------------------------------------------

Construya un dataframe de Pandas a partir del archivo 'clusters_report.txt', teniendo en
cuenta que los nombres de las columnas deben ser en minusculas, reemplazando los espacios
por guiones bajos; y que las palabras clave deben estar separadas por coma y con un solo 
espacio entre palabra y palabra.


"""

import pandas as pd
import re


def ingest_data():
    df = pd.read_fwf("clusters_report.txt", widths=[8, 16, 16, 80], header=[0, 1])
    df.columns = [
        re.sub(
            r"\s+(?=\w)", "_", " ".join([x for x in col if "Unnamed" not in x]).lower()
        ).strip()
        for col in df.columns
    ]
    df = df.ffill()
    duplicates_mask = df.duplicated(subset="cluster")
    for index, row in df[duplicates_mask].iterrows():
        original_index = df.index[df["cluster"] == row["cluster"]].tolist()[0]
        df.at[original_index, "principales_palabras_clave"] += (
            ", " + row["principales_palabras_clave"]
        )
    df = df.drop_duplicates(subset="cluster")
    df["principales_palabras_clave"] = df["principales_palabras_clave"].apply(
        lambda x: re.sub(
            r"\s+", " ", ", ".join([word.strip() for word in x.split(",") if word])
        )
    )
    return df
