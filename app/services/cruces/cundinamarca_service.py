import zipfile
import pandas as pd
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
    df_sisben = pd.read_csv(path_sisben)
    df_sisben['cod_mpio'] = df_sisben['cod_mpio'].astype("Int64")

    # Asignar provincia al Sisbén
    df_sisben['provincia_sisben'] = df_sisben['cod_mpio'].map(COD_A_PROVINCIA).fillna("Sin clasificar")

    df_contributivo = pd.read_csv(path_contributivo, dtype=str)
    df_subsidiado   = pd.read_csv(path_subsidiado,   dtype=str)

    df_contributivo["Regimen"] = "Contributivo"
    df_subsidiado["Regimen"] = "Subsidiado"
    df_maestro = pd.concat([df_contributivo, df_subsidiado], ignore_index=True)

    # ── Cruces ───────────────────────────────────────────────────────────────

    # Inner join: afiliados que SÍ están en Sisbén
    df_combinado = df_maestro.merge(
        df_sisben,
        left_on='numero_documento',
        right_on='num_documento',
        how='inner'
    )
    df_combinado['municipio_afiliacion'] = df_combinado['municipio_afiliacion'].astype(float).astype("Int64")

    df_combinado['provincia_afiliacion'] = df_combinado['municipio_afiliacion'].map(COD_A_PROVINCIA).fillna("Sin clasificar")

    # Detectar discordancia de municipio
    df_combinado['discordancia_municipio'] = (
            df_combinado['municipio_afiliacion'].astype(str) != df_combinado['cod_mpio_y'].astype(str)
    ).map({True: "SÍ", False: "NO"})

    # Left join: todos los afiliados (con y sin Sisbén)
    df_todos = df_maestro.merge(
        df_sisben, left_on='numero_documento', right_on='num_documento', how='left'
    )
    df_todos['municipio_afiliacion'] = df_todos['municipio_afiliacion'].astype(float).astype("Int64")
    df_todos['provincia_afiliacion'] = df_todos['municipio_afiliacion'].map(COD_A_PROVINCIA).fillna("Sin clasificar")

    # ── Archivos departamentales ──────────────────────────────────────────────
    df_discordantes = df_combinado[df_combinado['discordancia_municipio'] == 'SÍ']
    df_no_sisben = df_todos[df_todos['num_documento'].isna()].copy()

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