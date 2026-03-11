import zipfile
import polars as pl
from pathlib import Path

# ─────────────────────────────────────────────────────────────────────────────
# PROVINCIAS — mapa código DANE → nombre provincia
# ─────────────────────────────────────────────────────────────────────────────
PROVINCIAS = {
    "Alto Magdalena":   [25001, 25307, 25324, 25368, 25483, 25488, 25612, 25815],
    "Bajo Magdalena":   [25148, 25320, 25572],
    "Almeidas":         [25183, 25426, 25436, 25736, 25772, 25807, 25873],
    "Gualivá":          [25019, 25398, 25402, 25489, 25491, 25592, 25658, 25718, 25777, 25851, 25862, 25875],
    "Guavio":           [25293, 25297, 25299, 25322, 25326, 25372, 25377, 25839],
    "Magdalena Centro": [25086, 25095, 25168, 25328, 25580, 25662, 25867],
    "Medina":           [25438, 25530],
    "Oriente":          [25151, 25178, 25181, 25279, 25281, 25335, 25339, 25594, 25841, 25845],
    "Rionegro":         [25258, 25394, 25513, 25518, 25653, 25823, 25871, 25885],
    "Sabana Centro":    [25126, 25175, 25200, 25214, 25295, 25486, 25758, 25785, 25799, 25817, 25899],
    "Sabana Occidente": [25099, 25260, 25269, 25286, 25430, 25473, 25769, 25898],
    "Soacha":           [25740, 25754],
    "Sumapaz":          [25053, 25120, 25290, 25312, 25524, 25535, 25649, 25743, 25805, 25506],
    "Tequendama":       [25035, 25040, 25599, 25123, 25245, 25386, 25596, 25645, 25797, 25878],
    "Ubaté":            [25154, 25224, 25288, 25317, 25407, 25745, 25779, 25781, 25793, 25843],
}

# Diccionario plano: código int → nombre provincia
COD_A_PROVINCIA = {cod: prov for prov, codigos in PROVINCIAS.items() for cod in codigos}


def ejecutar_cruce(
    path_sisben: Path,
    path_contributivo: Path,
    path_subsidiado: Path,
    dir_salida: Path,
) -> Path:
    """
    Ejecuta el cruce Sisbén vs Maestro para Cundinamarca.
    Retorna el path del ZIP con todos los resultados.
    """
    dir_salida.mkdir(parents=True, exist_ok=True)

    # ── Carga ────────────────────────────────────────────────────────────────
    df_sisben = pl.read_csv(path_sisben, infer_schema_length=0)
    df_sisben = df_sisben.with_columns(
        pl.col("cod_mpio").cast(pl.Int64, strict=False)
    )

    # Asignar provincia al Sisbén
    df_sisben = df_sisben.with_columns(
        pl.col("cod_mpio")
          .replace(COD_A_PROVINCIA, default="Sin clasificar")
          .alias("provincia_sisben")
    )

    df_contributivo = pl.read_csv(path_contributivo, infer_schema_length=0)
    df_subsidiado   = pl.read_csv(path_subsidiado,   infer_schema_length=0)

    df_contributivo = df_contributivo.with_columns(pl.lit("Contributivo").alias("Regimen"))
    df_subsidiado   = df_subsidiado.with_columns(pl.lit("Subsidiado").alias("Regimen"))

    # Unir contributivo + subsidiado
    df_maestro = pl.concat([df_contributivo, df_subsidiado], how="diagonal")

    # ── Cruces ───────────────────────────────────────────────────────────────

    # Inner join: afiliados que SÍ están en Sisbén
    df_combinado = df_maestro.join(
        df_sisben,
        left_on="numero_documento",
        right_on="num_documento",
        how="inner"
    )
    df_combinado = df_combinado.with_columns(
        pl.col("municipio_afiliacion").cast(pl.Int64, strict=False)
    )
    df_combinado = df_combinado.with_columns(
        pl.col("municipio_afiliacion")
          .replace(COD_A_PROVINCIA, default="Sin clasificar")
          .alias("provincia_afiliacion")
    )

    # Detectar discordancia de municipio
    df_combinado = df_combinado.with_columns(
        pl.when(
            pl.col("municipio_afiliacion").cast(str) != pl.col("cod_mpio_right").cast(str)
        )
        .then(pl.lit("SÍ"))
        .otherwise(pl.lit("NO"))
        .alias("discordancia_municipio")
    )

    # Left join: todos los afiliados (con y sin Sisbén)
    df_todos = df_maestro.join(
        df_sisben,
        left_on="numero_documento",
        right_on="num_documento",
        how="left"
    )
    df_todos = df_todos.with_columns(
        pl.col("municipio_afiliacion").cast(pl.Int64, strict=False)
    )
    df_todos = df_todos.with_columns(
        pl.col("municipio_afiliacion")
          .replace(COD_A_PROVINCIA, default="Sin clasificar")
          .alias("provincia_afiliacion")
    )

    # ── Archivos departamentales ──────────────────────────────────────────────
    df_discordantes = df_combinado.filter(pl.col("discordancia_municipio") == "SÍ")
    df_no_sisben    = df_todos.filter(pl.col("numero_documento").is_null())

    df_combinado.write_csv(dir_salida / "Cruce_Cundinamarca.csv")
    df_discordantes.write_csv(dir_salida / "Discordantes_Cundinamarca.csv")
    df_sisben.write_csv(dir_salida / "Sisben_Cundinamarca.csv")
    df_no_sisben.write_csv(dir_salida / "NoSisben_Cundinamarca.csv")

    # ── Empaquetar en ZIP ─────────────────────────────────────────────────────
    zip_path = dir_salida / "Cundinamarca_resultado.zip"

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for archivo in dir_salida.rglob("*.csv"):
            zf.write(archivo, archivo.relative_to(dir_salida))

    return zip_path