"""
L3_obtencion_datos.py
---------------------
1. Carga los archivos CSV generados por explorar_transformar.py.
2. Unifica las diferentes fuentes en un único DataFrame consolidado.
3. Guarda el DataFrame consolidado en data/.

Estructura de directorios esperada:
    raiz/
    ├── main.py
    ├── src/
    │   ├── creacion_dataset.py
    │   ├── explorar_transformar.py
    │   └── L3_obtencion_datos.py    ← este archivo
    └── data/
        ├── df_clientes.csv
        ├── df_productos.csv
        ├── df_categorias.csv
        ├── df_ventas_2025.csv
        ├── df_ventas_2026.csv
        └── df_consolidado.csv       ← resultado
"""

import os
import warnings

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore", category=pd.errors.DtypeWarning)

# ---------------------------------------------------------------------------
# Rutas
# ---------------------------------------------------------------------------
_DIR_SRC  = os.path.dirname(os.path.abspath(__file__))
_DIR_ROOT = os.path.dirname(_DIR_SRC)
DIR_DATA  = os.path.join(_DIR_ROOT, "data")

SEPARADOR = "-" * 60
SEPARADOR_DOBLE = "=" * 60


# ===========================================================================
# 1. CARGA DE ARCHIVOS CSV
# ===========================================================================

def cargar_csv(nombre: str, parse_dates: list = None) -> pd.DataFrame:
    """
    Carga un CSV desde data/ y retorna un DataFrame.

    Parámetros
    ----------
    nombre       : str   – Nombre del archivo (incluye .csv).
    parse_dates  : list  – Columnas a parsear como fecha.

    Retorna
    -------
    pd.DataFrame
    """
    ruta = os.path.join(DIR_DATA, nombre)
    df = pd.read_csv(ruta, parse_dates=parse_dates, encoding="utf-8")
    print(f"  ✔  {nombre:<30}  {df.shape[0]:>5} filas × {df.shape[1]} cols")
    return df


def cargar_fuentes() -> tuple:
    """
    Carga los cinco archivos CSV y los retorna como DataFrames.

    Retorna
    -------
    tuple: (df_clientes, df_productos, df_categorias, df_ventas_2025, df_ventas_2026)
    """
    print(f"\n{SEPARADOR_DOBLE}")
    print("  1. Cargando archivos CSV...")
    print(SEPARADOR_DOBLE)

    df_clientes   = cargar_csv("df_clientes.csv",   parse_dates=["fecha_registro"])
    df_productos  = cargar_csv("df_productos.csv")
    df_categorias = cargar_csv("df_categorias.csv")
    df_ventas_2025 = cargar_csv("df_ventas_2025.csv", parse_dates=["fecha_venta"])
    df_ventas_2026 = cargar_csv("df_ventas_2026.csv", parse_dates=["fecha_venta"])

    return df_clientes, df_productos, df_categorias, df_ventas_2025, df_ventas_2026


# ===========================================================================
# 2. UNIFICACIÓN EN UN ÚNICO DATAFRAME
# ===========================================================================

def unificar_fuentes(
    df_clientes: pd.DataFrame,
    df_productos: pd.DataFrame,
    df_categorias: pd.DataFrame,
    df_ventas_2025: pd.DataFrame,
    df_ventas_2026: pd.DataFrame,
) -> pd.DataFrame:
    """
    Une las cinco fuentes en un único DataFrame consolidado.

    Estrategia de unión:
      1. Concatenar ventas_2025 + ventas_2026  →  df_ventas
      2. JOIN ventas  ←→  clientes   (por cliente_id)
      3. JOIN ventas  ←→  productos  (por producto_id)
      4. JOIN ventas  ←→  categorias (por categoria_id)

    Retorna
    -------
    pd.DataFrame consolidado
    """
    print(f"\n{SEPARADOR_DOBLE}")
    print("  2. Unificando fuentes de datos...")
    print(SEPARADOR_DOBLE)

    # ------------------------------------------------------------------
    # Paso 1: Concatenar ventas 2025 y 2026
    # ------------------------------------------------------------------
    df_ventas = pd.concat(
        [df_ventas_2025, df_ventas_2026],
        ignore_index=True,
    )
    print(f"\n  [Paso 1] Ventas 2025 + 2026 concatenadas")
    print(f"           {len(df_ventas_2025)} + {len(df_ventas_2026)} = {len(df_ventas)} registros")

    # ------------------------------------------------------------------
    # Paso 2: Preparar clientes
    #   - Eliminar duplicados manteniendo el primer registro
    #   - Normalizar columna "genero" (M/Masculino → Masculino,  F/Femenino → Femenino)
    #   - Renombrar columnas que colisionan con ventas
    # ------------------------------------------------------------------
    df_cli = df_clientes.drop_duplicates(subset=["cliente_id"], keep="first").copy()

    df_cli["genero"] = df_cli["genero"].replace({
        "M": "Masculino",
        "F": "Femenino",
    })

    df_cli = df_cli.rename(columns={
        "activo": "cliente_activo",
    })

    duplicados_eliminados = len(df_clientes) - len(df_cli)
    print(f"\n  [Paso 2] Clientes preparados")
    print(f"           Duplicados eliminados : {duplicados_eliminados}")
    print(f"           Registros resultantes : {len(df_cli)}")

    # ------------------------------------------------------------------
    # Paso 3: JOIN ventas ←→ clientes  (left join)
    #   LEFT JOIN para conservar todas las ventas aunque un cliente_id
    #   no exista en el catálogo de clientes (integridad referencial débil).
    # ------------------------------------------------------------------
    df = pd.merge(
        df_ventas,
        df_cli,
        on="cliente_id",
        how="left",
        validate="m:1",          # muchas ventas → un cliente
    )
    sin_cliente = df["nombre"].isna().sum()
    print(f"\n  [Paso 3] JOIN ventas ←→ clientes")
    print(f"           Ventas sin cliente en catálogo : {sin_cliente}")

    # ------------------------------------------------------------------
    # Paso 4: JOIN resultado ←→ productos  (left join)
    # ------------------------------------------------------------------
    df = pd.merge(
        df,
        df_productos.rename(columns={"nombre_producto": "producto"}),
        on="producto_id",
        how="left",
        validate="m:1",
    )
    sin_producto = df["producto"].isna().sum()
    print(f"\n  [Paso 4] JOIN ←→ productos")
    print(f"           Ventas sin producto en catálogo : {sin_producto}")

    # ------------------------------------------------------------------
    # Paso 5: JOIN resultado ←→ categorias  (left join)
    # ------------------------------------------------------------------
    df = pd.merge(
        df,
        df_categorias,
        on="categoria_id",
        how="left",
        validate="m:1",
    )
    sin_categoria = df["nombre_categoria"].isna().sum()
    print(f"\n  [Paso 5] JOIN ←→ categorías")
    print(f"           Ventas sin categoría en catálogo : {sin_categoria}")

    # ------------------------------------------------------------------
    # Paso 6: Reordenar columnas para mejor legibilidad
    # ------------------------------------------------------------------
    columnas_orden = [
        # Identificadores
        "venta_id", "fecha_venta",
        # Cliente
        "cliente_id", "nombre", "apellido", "email",
        "genero", "fecha_registro", "region", "pais",
        "edad", "ingreso_mensual", "cliente_activo",
        # Producto / Categoría
        "producto_id", "producto", "categoria_id", "nombre_categoria",
        # Transacción
        "cantidad", "precio_unitario", "total_venta", "canal_venta",
    ]
    # Incluir sólo las columnas que existan (tolerancia a cambios futuros)
    columnas_finales = [c for c in columnas_orden if c in df.columns]
    df = df[columnas_finales]

    print(f"\n  {'─' * 40}")
    print(f"  DataFrame consolidado: {df.shape[0]} filas × {df.shape[1]} columnas")
    print(f"  {'─' * 40}")

    return df


# ===========================================================================
# 3. GUARDAR DATAFRAME CONSOLIDADO
# ===========================================================================

def guardar_consolidado(df: pd.DataFrame, nombre: str = "df_consolidado.csv") -> str:
    """
    Guarda el DataFrame consolidado en data/.

    Retorna
    -------
    str  – Ruta absoluta del archivo guardado.
    """
    print(f"\n{SEPARADOR_DOBLE}")
    print("  3. Guardando DataFrame consolidado...")
    print(SEPARADOR_DOBLE)

    ruta = os.path.join(DIR_DATA, nombre)
    df.to_csv(ruta, index=False, encoding="utf-8")
    print(f"\n  ✔  {nombre}  →  {ruta}")
    print(f"     {df.shape[0]} filas × {df.shape[1]} columnas guardadas")
    return ruta


# ===========================================================================
# FUNCIÓN PRINCIPAL
# ===========================================================================

def obtener_datos() -> pd.DataFrame:
    """
    Orquesta la carga, unificación y guardado del dataset consolidado.

    Retorna
    -------
    pd.DataFrame consolidado
    """
    # 1. Cargar
    df_clientes, df_productos, df_categorias, df_ventas_2025, df_ventas_2026 = (
        cargar_fuentes()
    )

    # 2. Unificar
    df_consolidado = unificar_fuentes(
        df_clientes, df_productos, df_categorias,
        df_ventas_2025, df_ventas_2026,
    )

    # 3. Guardar
    guardar_consolidado(df_consolidado)

    return df_consolidado


# ---------------------------------------------------------------------------
# Punto de entrada directo  (python src/L3_obtencion_datos.py)
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    df = obtener_datos()

    # Vista previa final
    print(f"\n{SEPARADOR_DOBLE}")
    print("  Vista previa del DataFrame consolidado")
    print(SEPARADOR_DOBLE)
    print(f"\n  Primeras 5 filas:")
    print(df.head().to_string(index=True))
    print(f"\n  Últimas 5 filas:")
    print(df.tail().to_string(index=True))
    print(f"\n  Tipos de columnas:")
    print(df.dtypes.to_string())
    print(f"\n  Valores nulos por columna:")
    print(df.isnull().sum().to_string())