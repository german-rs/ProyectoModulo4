"""
creacion_dataset.py
-------------------
Genera los datasets de e-commerce para el proyecto Módulo 4:
  - clientes_ecommerce.csv
  - productos.csv
  - categorias.csv
  - ventas_ecommerce_2025_2026.xlsx  (hojas: ventas_2025, ventas_2026)

Estructura de directorios esperada:
    raiz/
    ├── main.py
    ├── src/
    │   └── creacion_dataset.py   ← este archivo
    └── data/
        └── (archivos generados)
"""

import os
from datetime import date, timedelta

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Rutas
# ---------------------------------------------------------------------------
# __file__ apunta a src/creacion_dataset.py → subimos un nivel → raíz → data/
_DIR_SRC  = os.path.dirname(os.path.abspath(__file__))
_DIR_ROOT = os.path.dirname(_DIR_SRC)
DIR_DATA  = os.path.join(_DIR_ROOT, "data")


# ---------------------------------------------------------------------------
# Rangos de fechas
# ---------------------------------------------------------------------------
fecha_inicio_2025 = date(2025, 1, 1)
fecha_fin_2025    = date(2025, 12, 31)
fecha_inicio_2026 = date(2026, 1, 1)
fecha_fin_2026    = date(2026, 12, 31)

# Rango para fechas de registro de clientes (antes de 2025)
fecha_inicio_registro = date(2022, 1, 1)
fecha_fin_registro    = date(2024, 12, 31)


# ---------------------------------------------------------------------------
# Funciones auxiliares
# ---------------------------------------------------------------------------

def generar_fecha_registro() -> date:
    """Genera una fecha de registro aleatoria entre 2022 y 2024."""
    dias = (fecha_fin_registro - fecha_inicio_registro).days
    return fecha_inicio_registro + timedelta(days=np.random.randint(0, dias))


def generar_fecha_venta(year: int) -> date:
    """Genera una fecha de venta aleatoria dentro del año indicado (2025 o 2026)."""
    if year == 2025:
        inicio, fin = fecha_inicio_2025, fecha_fin_2025
    else:
        inicio, fin = fecha_inicio_2026, fecha_fin_2026

    dias = (fin - inicio).days
    return inicio + timedelta(days=np.random.randint(0, dias))


# ---------------------------------------------------------------------------
# Generación de clientes
# ---------------------------------------------------------------------------

def generar_clientes(n_clientes: int = 300) -> pd.DataFrame:
    """
    Genera un DataFrame de clientes con ruido (nulos y duplicados).

    Parámetros
    ----------
    n_clientes : int
        Número base de clientes a generar antes de añadir duplicados.

    Retorna
    -------
    pd.DataFrame
    """
    np.random.seed(42)

    nombres = [
        "Juan", "María", "Pedro", "Camila", "Diego", "Valentina",
        "Felipe", "Daniela", "Sebastián", "Francisca",
        "Andrés", "Carolina", "Rodrigo", "Constanza",
    ]

    apellidos = [
        "González", "Muñoz", "Rojas", "Díaz", "Pérez",
        "Soto", "Contreras", "Silva", "Martínez", "López",
        "Morales", "Araya", "Flores", "Espinoza", "Valenzuela",
    ]

    regiones      = ["RM", "Coquimbo", "Valparaíso", "Biobío", "Antofagasta", "Araucanía"]
    pesos_regiones = [0.40, 0.20, 0.15, 0.10, 0.08, 0.07]
    generos       = ["M", "F", "Masculino", "Femenino"]

    clientes = []
    for i in range(1, n_clientes + 1):
        nombre   = np.random.choice(nombres)
        apellido = np.random.choice(apellidos)
        email    = f"{nombre.lower()}.{apellido.lower()}{i}@mail.cl"
        genero   = np.random.choice(generos)
        fecha_registro = generar_fecha_registro()
        region   = np.random.choice(
            regiones,
            p=np.array(pesos_regiones) / sum(pesos_regiones),
        )
        edad     = np.random.randint(18, 70)
        ingreso  = np.random.randint(500_000, 2_500_000)
        activo   = np.random.choice([True, False])

        clientes.append([
            i, nombre, apellido, email, genero,
            fecha_registro,
            region, "Chile", edad, ingreso, activo,
        ])

    df = pd.DataFrame(clientes, columns=[
        "cliente_id", "nombre", "apellido", "email", "genero",
        "fecha_registro", "region", "pais", "edad",
        "ingreso_mensual", "activo",
    ])

    # --- Ruido: nulos ---
    df.loc[np.random.choice(df.index, 15, replace=False), "edad"]            = np.nan
    df.loc[np.random.choice(df.index, 10, replace=False), "ingreso_mensual"] = np.nan
    df.loc[np.random.choice(df.index, 8,  replace=False), "region"]          = np.nan

    # --- Ruido: duplicados (5 filas) ---
    df = pd.concat([df, df.sample(5)], ignore_index=True)

    return df


# ---------------------------------------------------------------------------
# Generación de ventas (+ categorías y productos como efecto secundario)
# ---------------------------------------------------------------------------

def generar_ventas(year: int, n_registros: int, n_clientes: int = 300) -> pd.DataFrame:
    """
    Genera un DataFrame de ventas con ruido (outliers en precio).
    Como efecto secundario, devuelve también los DataFrames de
    categorías y productos (necesarios para guardarlos sólo una vez).

    Parámetros
    ----------
    year       : int  – Año de las ventas (2025 o 2026).
    n_registros: int  – Número de registros a generar.
    n_clientes : int  – Cantidad máxima de cliente_id a sortear.

    Retorna
    -------
    tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]
        (df_ventas, df_categorias, df_productos)
    """
    df_categorias = pd.DataFrame([
        [1, "Tecnología"],
        [2, "Accesorios"],
    ], columns=["categoria_id", "nombre_categoria"])

    df_productos = pd.DataFrame([
        [1, "Notebook",  1],
        [2, "Mouse",     2],
        [3, "Teclado",   2],
        [4, "Monitor",   1],
        [5, "Audífonos", 2],
        [6, "Tablet",    1],
    ], columns=["producto_id", "nombre_producto", "categoria_id"])

    producto_ids = df_productos["producto_id"].tolist()
    canales      = ["Web", "App", "Tienda Física"]

    ventas = []
    for i in range(1, n_registros + 1):
        cliente_id = np.random.randint(1, n_clientes)
        fecha      = generar_fecha_venta(year)
        producto_id = np.random.choice(producto_ids)
        cantidad   = np.random.randint(1, 5)
        precio     = np.random.randint(10_000, 800_000)
        total      = cantidad * precio

        ventas.append([
            f"{year}-{str(i).zfill(3)}",
            cliente_id,
            fecha,
            producto_id,
            cantidad,
            precio,
            total,
            np.random.choice(canales),
        ])

    df = pd.DataFrame(ventas, columns=[
        "venta_id", "cliente_id", "fecha_venta", "producto_id",
        "cantidad", "precio_unitario", "total_venta", "canal_venta",
    ])

    # --- Ruido: 2 % de precios atípicos ---
    idx_outliers = np.random.choice(df.index, max(1, int(len(df) * 0.02)), replace=False)
    for idx in idx_outliers:
        precio_outlier = np.random.randint(2_000_000, 5_000_000)
        df.loc[idx, "precio_unitario"] = precio_outlier
        df.loc[idx, "total_venta"]     = df.loc[idx, "cantidad"] * precio_outlier

    return df, df_categorias, df_productos


# ---------------------------------------------------------------------------
# Función principal
# ---------------------------------------------------------------------------

def crear_dataset() -> None:
    """
    Orquesta la creación de todos los archivos y los guarda en data/.
    """
    os.makedirs(DIR_DATA, exist_ok=True)

    # -- Clientes --
    df_clientes = generar_clientes(n_clientes=300)
    ruta_clientes = os.path.join(DIR_DATA, "clientes_ecommerce.csv")
    df_clientes.to_csv(ruta_clientes, index=False)
    print(f"✔  Clientes generados  →  {ruta_clientes}")

    # -- Ventas 2025 y 2026 --
    df_2025, df_categorias, df_productos = generar_ventas(2025, 1_000)
    df_2026, _, _                        = generar_ventas(2026, 300)

    ruta_excel = os.path.join(DIR_DATA, "ventas_ecommerce_2025_2026.xlsx")
    with pd.ExcelWriter(ruta_excel, engine="openpyxl") as writer:
        df_2025.to_excel(writer, sheet_name="ventas_2025", index=False)
        df_2026.to_excel(writer, sheet_name="ventas_2026", index=False)
    print(f"✔  Ventas generadas    →  {ruta_excel}")

    # -- Categorías y Productos --
    ruta_cat  = os.path.join(DIR_DATA, "categorias.csv")
    ruta_prod = os.path.join(DIR_DATA, "productos.csv")
    df_categorias.to_csv(ruta_cat,  index=False)
    df_productos.to_csv(ruta_prod, index=False)
    print(f"✔  Categorías          →  {ruta_cat}")
    print(f"✔  Productos           →  {ruta_prod}")


# ---------------------------------------------------------------------------
# Punto de entrada directo  (python src/creacion_dataset.py)
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    crear_dataset()












