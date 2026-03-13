import pandas as pd
from pathlib import Path

COLS_SUBSIDIADO = [
    "codigo_entidad", "codigo_eps", "tipo_documento_titular",
    "numero_documento_titular", "tipo_documento", "numero_documento",
    "primer_apellido", "segundo_apellido", "primer_nombre", "segundo_nombre",
    "fecha_nacimiento", "sexo", "pais", "municipio", "nacionalidad",
    "sexo_identifica", "nivel_sisben", "tipo_afiliado", "tipo_poblacion_especial",
    "Por identificar 1", "Por identificar 2", "por iden", "Fecha afiliacion",
    "cod_dpto", "cod_mpio", "Zona", "Por identificar 3", "Por identificar 4",
    "Por identificar 5", "Etnia", "Modalidad de subsidio", "Estado de afiliacion",
    "Fecha inicio novedad", "Fecha inicio poliza", "Ips primaria",
    "Tipo de actualizacion documento", "Numero de poliza", "Metodologia poblacional",
    "Sisben IV", "Codigo sisben grupo", "Condicion de portabilidad",
    "Por identificar 6", "Por identificar 7"
]

COLS_CONTRIBUTIVO = [
    "codigo_entidad", "codigo_eps", "tipo_documento_aportante",
    "numero_documento_aportante", "tipo_documento", "numero_documento",
    "primer_apellido", "segundo_apellido", "primer_nombre", "segundo_nombre",
    "fecha_nacimiento", "sexo_biologico", "nacionalidad", "municipio_afiliacion",
    "pais_residencia", "sexo_identifica", "tipo_cotizante", "nivel_sisben",
    "grupo_poblacional", "subgrupo_sisben_iv", "na1", "na2", "na3",
    "cod_dpto", "cod_mpio", "zona", "na4", "tipo_afiliacion",
    "estado_afiliacion", "fecha_afiliacion", "fecha", "na5", "na6",
    "ficha", "na7", "sisben_iv", "codigo_sisben_grupo", "estado"
]

PARAMS_CSV = dict(
    sep=',',
    index_col=None,
    skipinitialspace=True,
    encoding='latin-1',
    quotechar='"',
    on_bad_lines='warn',
    low_memory=False,
    header=None,          # los archivos NO traen encabezado
)


def _asignar_columnas(df: pd.DataFrame, nombres_base: list[str]) -> pd.DataFrame:
    """
    Asigna nombres a las columnas del dataframe.
    Si el archivo trae más columnas que la resolución define,
    las extra se nombran 'por_identificar_N'.
    """
    n_cols = len(df.columns)
    n_base = len(nombres_base)
    if n_cols > n_base:
        extra = [f"por_identificar_{i}" for i in range(1, n_cols - n_base + 1)]
        nombres = nombres_base + extra
    else:
        nombres = nombres_base[:n_cols]
    df.columns = nombres
    return df


def _construir_municipio(df: pd.DataFrame) -> pd.DataFrame:
    """
    Construye municipio_afiliacion = cod_dpto + cod_mpio (con 3 dígitos).
    Ejemplo: cod_dpto=25, cod_mpio=1 → '25001'
    """
    return df.assign(
        municipio_afiliacion=df["cod_dpto"].astype(str) + df["cod_mpio"].astype(str).str.zfill(3)
    )


def procesar_subsidiado(input_path: Path, output_path: Path) -> Path:
    df = pd.read_csv(
        input_path,
        **PARAMS_CSV
    )
    df = _asignar_columnas(df, COLS_SUBSIDIADO)
    df = _construir_municipio(df)
    df.to_csv(output_path, index=False)
    return output_path


def procesar_contributivo(input_path: Path, output_path: Path) -> Path:
    df = pd.read_csv(
        input_path,
        **PARAMS_CSV
    )
    df = _asignar_columnas(df, COLS_CONTRIBUTIVO)
    df = _construir_municipio(df)
    df.to_csv(output_path, index=False)
    return output_path