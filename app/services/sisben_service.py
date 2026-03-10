import polars as pl
from pathlib import Path

COLUMNAS = [
    "nom_dpto", "nom_mpio", "cod_mpio", "nom_vereda", "nom_barrio",
    "Dir_vivienda", "pri_apellido", "seg_apellido", "pri_nombre", "seg_nombre",
    "sexo_persona", "tip_documento", "num_documento", "num_tel_contacto",
    "fec_nacimiento", "edad_calculada", "cod_mpio_documento",
    "cod_dpto_documento", "cod_pais_documento", "Grupo", "Nivel", "Clasificacion"
]

def procesar_sisben(input_path: Path, output_path: Path) -> Path:
    df = pl.read_csv(
        input_path,
        separator=';',
        infer_schema_length=0,
        truncate_ragged_lines=True,
    )

    df = df[:, 1:]

    nuevos_nombres = df.columns[1:]
    df = df[1:]
    df.columns = nuevos_nombres

    df = df.select(COLUMNAS)

    df.write_csv(output_path)

    return output_path