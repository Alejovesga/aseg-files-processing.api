import pandas as pd
from pathlib import Path

COLUMNAS = [
    "nom_dpto", "nom_mpio", "cod_mpio", "nom_vereda", "nom_barrio",
    "Dir_vivienda", "pri_apellido", "seg_apellido", "pri_nombre", "seg_nombre",
    "sexo_persona", "tip_documento", "num_documento", "num_tel_contacto",
    "fec_nacimiento", "edad_calculada", "cod_mpio_documento",
    "cod_dpto_documento", "cod_pais_documento", "Grupo", "Nivel", " Clasificacion"
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
    )

    df = df[COLUMNAS].copy()
    df.to_csv(output_path, index=False)

    return output_path