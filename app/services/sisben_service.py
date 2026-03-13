import pandas as pd
from pathlib import Path

COLUMNAS = [
    "nom_dpto", "nom_mpio", "cod_mpio", "nom_vereda", "nom_barrio",
    "Dir_vivienda", "pri_apellido", "seg_apellido", "pri_nombre", "seg_nombre",
    "sexo_persona", "tip_documento", "num_documento", "num_tel_contacto",
    "fec_nacimiento", "edad_calculada", "cod_mpio_documento",
    "cod_dpto_documento", "cod_pais_documento", "Grupo", "Nivel", "Clasificacion"
]

def procesar_sisben(input_path: Path, output_path: Path) -> Path:
    df = pd.read_csv(
        input_path,
        sep=';',
        skipinitialspace=True,
        encoding='utf-8',
        quotechar='"',
        on_bad_lines='warn',
        low_memory=False,
        index_col=0,
    )

    encabezados = df.columns.tolist()
    df2 = df.reset_index(drop=True)
    df2 = df2.iloc[:, :-1]

    nuevos_encabezados = encabezados[1:]

    df2.columns = nuevos_encabezados

    df2 = df2[COLUMNAS].copy()
    df2.to_csv(output_path)

    return output_path