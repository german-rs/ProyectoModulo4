"""
explorar_transformar.py
-----------------------
1. Lee los archivos generados en data/ usando NumPy y los convierte en DataFrames.
2. Realiza una exploración inicial:
   a) Primeras y últimas filas.
   b) Estadísticas descriptivas.
   c) Filtros condicionales.

Estructura de directorios esperada:
    raiz/
    ├── main.py
    ├── src/
    │   ├── creacion_dataset.py
    │   └── explorar_transformar.py   ← este archivo
    └── data/
        ├── clientes_ecommerce.csv
        ├── productos.csv
        ├── categorias.csv
        └── ventas_ecommerce_2025_2026.xlsx
"""

import os

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Rutas
# ---------------------------------------------------------------------------
_DIR_SRC  = os.path.dirname(os.path.abspath(__file__))
_DIR_ROOT = os.path.dirname(_DIR_SRC)
DIR_DATA  = os.path.join(_DIR_ROOT, "data")

SEPARADOR = "-" * 60


# ===========================================================================
# 1. LECTURA CON NUMPY → CONVERSIÓN A DATAFRAME
# ===========================================================================

def leer_con_numpy(nombre_archivo: str, skip_header: bool = True) -> pd.DataFrame:
    """
    Lee un archivo CSV usando np.genfromtxt y lo convierte en un DataFrame.

    Parámetros
    ----------
    nombre_archivo : str  – Nombre del archivo dentro de data/.
    skip_header    : bool – Si True, la primera fila se usa como encabezado.

    Retorna
    -------
    pd.DataFrame
    """
    ruta = os.path.join(DIR_DATA, nombre_archivo)

    # np.genfromtxt lee todo como texto (dtype=str) para preservar
    # columnas mixtas (fechas, strings, números).
    datos_numpy = np.genfromtxt(
        ruta,
        delimiter=",",
        dtype=str,
        encoding="utf-8",
    )

    if skip_header:
        columnas = datos_numpy[0]          # primera fila → nombres de columna
        datos    = datos_numpy[1:]         # resto → valores
    else:
        columnas = [f"col_{i}" for i in range(datos_numpy.shape[1])]
        datos    = datos_numpy

    df = pd.DataFrame(datos, columns=columnas)

    # Intentar convertir columnas numéricas automáticamente.
    # errors="ignore" fue eliminado en pandas 2.x, por eso convertimos
    # con "coerce" (inválidos → NaN) y sólo reemplazamos la columna
    # si la conversión no produjo NaN donde antes había un valor real.
    for col in df.columns:
        convertida = pd.to_numeric(df[col], errors="coerce")
        # Sólo aplicar si la columna era realmente numérica:
        # ninguna celda que tenía valor se convirtió en NaN.
        if convertida.notna().sum() == df[col].replace("", np.nan).notna().sum():
            df[col] = convertida

    return df


def leer_excel_ventas() -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Lee el archivo Excel de ventas con pandas (openpyxl).
    Las dos hojas se devuelven como DataFrames independientes.

    Retorna
    -------
    tuple[pd.DataFrame, pd.DataFrame]  →  (ventas_2025, ventas_2026)
    """
    ruta = os.path.join(DIR_DATA, "ventas_ecommerce_2025_2026.xlsx")
    df_2025 = pd.read_excel(ruta, sheet_name="ventas_2025")
    df_2026 = pd.read_excel(ruta, sheet_name="ventas_2026")
    return df_2025, df_2026


# ===========================================================================
# 2a. PRIMERAS Y ÚLTIMAS FILAS
# ===========================================================================

def mostrar_primeras_ultimas(df: pd.DataFrame, nombre: str, n: int = 5) -> None:
    """Imprime las primeras y últimas n filas de un DataFrame."""
    print(f"\n{'=' * 60}")
    print(f"  {nombre}")
    print(f"{'=' * 60}")
    print(f"\n  Dimensiones: {df.shape[0]} filas × {df.shape[1]} columnas")

    print(f"\n{SEPARADOR}")
    print(f"  Primeras {n} filas")
    print(SEPARADOR)
    print(df.head(n).to_string(index=True))

    print(f"\n{SEPARADOR}")
    print(f"  Últimas {n} filas")
    print(SEPARADOR)
    print(df.tail(n).to_string(index=True))


# ===========================================================================
# 2b. ESTADÍSTICAS DESCRIPTIVAS
# ===========================================================================

def mostrar_estadisticas(df: pd.DataFrame, nombre: str) -> None:
    """Imprime estadísticas descriptivas de las columnas numéricas y no numéricas."""
    print(f"\n{'=' * 60}")
    print(f"  Estadísticas descriptivas — {nombre}")
    print(f"{'=' * 60}")

    # Tipos de datos
    print("\n  Tipos de columnas:")
    print(df.dtypes.to_string())

    # Valores nulos
    nulos = df.isnull().sum()
    print(f"\n  Valores nulos por columna:")
    print(nulos[nulos >= 0].to_string())

    # Duplicados
    print(f"\n  Filas duplicadas: {df.duplicated().sum()}")

    # Describe numérico
    numericas = df.select_dtypes(include=[np.number])
    if not numericas.empty:
        print(f"\n  Estadísticas numéricas:")
        print(numericas.describe().round(2).to_string())

    # Describe categórico / string
    # include=["object", "str"] cubre tanto pandas 2.x como 3.x/4.x
    categoricas = df.select_dtypes(include=["object", "str"])
    if not categoricas.empty:
        print(f"\n  Estadísticas categóricas:")
        print(categoricas.describe().to_string())


# ===========================================================================
# 2c. FILTROS CONDICIONALES
# ===========================================================================

def aplicar_filtros(
    df_clientes: pd.DataFrame,
    df_ventas_2025: pd.DataFrame,
    df_ventas_2026: pd.DataFrame,
) -> None:
    """
    Aplica y muestra una serie de filtros condicionales sobre los DataFrames.
    """
    print(f"\n{'=' * 60}")
    print("  Filtros condicionales")
    print(f"{'=' * 60}")

    # ---- Clientes --------------------------------------------------------

    # Filtro 1: Clientes activos de la Región Metropolitana
    f1 = df_clientes[
        (df_clientes["activo"] == "True") &
        (df_clientes["region"] == "RM")
    ]
    print(f"\n{SEPARADOR}")
    print(f"  [Clientes] Activos en RM: {len(f1)} registros")
    print(SEPARADOR)
    print(f1.head(5).to_string(index=True))

    # Filtro 2: Clientes con ingreso mensual > 1.500.000
    df_clientes_num = df_clientes.copy()
    df_clientes_num["ingreso_mensual"] = pd.to_numeric(
        df_clientes_num["ingreso_mensual"], errors="coerce"
    )
    f2 = df_clientes_num[df_clientes_num["ingreso_mensual"] > 1_500_000]
    print(f"\n{SEPARADOR}")
    print(f"  [Clientes] Ingreso mensual > $1.500.000: {len(f2)} registros")
    print(SEPARADOR)
    print(f2[["cliente_id", "nombre", "apellido", "region", "ingreso_mensual"]].head(5).to_string(index=True))

    # Filtro 3: Clientes con nulos en región
    f3 = df_clientes[df_clientes["region"].isnull() | (df_clientes["region"] == "")]
    print(f"\n{SEPARADOR}")
    print(f"  [Clientes] Con región nula: {len(f3)} registros")
    print(SEPARADOR)
    print(f3[["cliente_id", "nombre", "apellido", "region"]].to_string(index=True))

    # ---- Ventas 2025 -----------------------------------------------------

    # Filtro 4: Ventas con total_venta > 1.000.000 (posibles outliers)
    f4 = df_ventas_2025[df_ventas_2025["total_venta"] > 1_000_000]
    print(f"\n{SEPARADOR}")
    print(f"  [Ventas 2025] total_venta > $1.000.000: {len(f4)} registros")
    print(SEPARADOR)
    print(f4.head(5).to_string(index=True))

    # Filtro 5: Ventas por canal "App" en 2025
    f5 = df_ventas_2025[df_ventas_2025["canal_venta"] == "App"]
    print(f"\n{SEPARADOR}")
    print(f"  [Ventas 2025] Canal 'App': {len(f5)} registros")
    print(SEPARADOR)
    print(f5.head(5).to_string(index=True))

    # Filtro 6: Ventas 2026 con cantidad >= 4
    f6 = df_ventas_2026[df_ventas_2026["cantidad"] >= 4]
    print(f"\n{SEPARADOR}")
    print(f"  [Ventas 2026] Cantidad >= 4: {len(f6)} registros")
    print(SEPARADOR)
    print(f6.head(5).to_string(index=True))


# ===========================================================================
# 3. GUARDAR DATAFRAMES EN CSV
# ===========================================================================

def guardar_dataframes(dataframes: dict) -> None:
    """
    Guarda cada DataFrame como CSV en data/.

    Parámetros
    ----------
    dataframes : dict  – Diccionario {nombre_archivo: dataframe}.
                         El nombre debe incluir la extensión .csv.
    """
    print(f"\n{'=' * 60}")
    print("  3. Guardando DataFrames en CSV...")
    print(f"{'=' * 60}\n")

    for nombre, df in dataframes.items():
        ruta = os.path.join(DIR_DATA, nombre)
        df.to_csv(ruta, index=False, encoding="utf-8")
        print(f"  ✔  {nombre:<45} ({df.shape[0]} filas × {df.shape[1]} cols)  →  {ruta}")


# ===========================================================================
# FUNCIÓN PRINCIPAL
# ===========================================================================

def explorar_datos() -> None:
    """
    Orquesta la lectura, conversión, exploración y guardado de todos los datasets.
    """

    # ------------------------------------------------------------------
    # 1. Lectura con NumPy → DataFrame
    # ------------------------------------------------------------------
    print("\n" + "=" * 60)
    print("  1. Leyendo archivos CSV con NumPy...")
    print("=" * 60)

    df_clientes  = leer_con_numpy("clientes_ecommerce.csv")
    df_productos = leer_con_numpy("productos.csv")
    df_categorias = leer_con_numpy("categorias.csv")
    df_ventas_2025, df_ventas_2026 = leer_excel_ventas()

    print("  ✔  clientes_ecommerce.csv   →  DataFrame OK")
    print("  ✔  productos.csv            →  DataFrame OK")
    print("  ✔  categorias.csv           →  DataFrame OK")
    print("  ✔  ventas_ecommerce.xlsx    →  DataFrames OK (2 hojas)")

    # ------------------------------------------------------------------
    # 2a. Primeras y últimas filas
    # ------------------------------------------------------------------
    print("\n\n" + "=" * 60)
    print("  2a. Primeras y últimas filas")
    print("=" * 60)

    mostrar_primeras_ultimas(df_clientes,   "clientes_ecommerce.csv")
    mostrar_primeras_ultimas(df_productos,  "productos.csv")
    mostrar_primeras_ultimas(df_categorias, "categorias.csv")
    mostrar_primeras_ultimas(df_ventas_2025, "ventas_ecommerce_2025_2026.xlsx → ventas_2025")
    mostrar_primeras_ultimas(df_ventas_2026, "ventas_ecommerce_2025_2026.xlsx → ventas_2026")

    # ------------------------------------------------------------------
    # 2b. Estadísticas descriptivas
    # ------------------------------------------------------------------
    print("\n\n" + "=" * 60)
    print("  2b. Estadísticas descriptivas")
    print("=" * 60)

    mostrar_estadisticas(df_clientes,    "clientes_ecommerce.csv")
    mostrar_estadisticas(df_productos,   "productos.csv")
    mostrar_estadisticas(df_categorias,  "categorias.csv")
    mostrar_estadisticas(df_ventas_2025, "ventas_2025")
    mostrar_estadisticas(df_ventas_2026, "ventas_2026")

    # ------------------------------------------------------------------
    # 2c. Filtros condicionales
    # ------------------------------------------------------------------
    print("\n\n" + "=" * 60)
    print("  2c. Filtros condicionales")
    print("=" * 60)

    aplicar_filtros(df_clientes, df_ventas_2025, df_ventas_2026)

    # ------------------------------------------------------------------
    # 3. Guardar DataFrames en CSV
    # ------------------------------------------------------------------
    guardar_dataframes({
        "df_clientes.csv":   df_clientes,
        "df_productos.csv":  df_productos,
        "df_categorias.csv": df_categorias,
        "df_ventas_2025.csv": df_ventas_2025,
        "df_ventas_2026.csv": df_ventas_2026,
    })


# ---------------------------------------------------------------------------
# Punto de entrada directo  (python src/explorar_transformar.py)
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    explorar_datos()