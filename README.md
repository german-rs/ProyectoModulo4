# Proyecto preparación de datos

## Modelo Relacional – E-commerce

~~~ mermaid
erDiagram

    CLIENTES ||--o{ VENTAS : realiza
    PRODUCTOS ||--o{ VENTAS : contiene
    CATEGORIAS ||--o{ PRODUCTOS : clasifica

    CLIENTES {
        int cliente_id PK
        string nombre
        string apellido
        string email
        string genero
        date fecha_registro
        string region
        int ingreso_mensual
        boolean activo
    }

    VENTAS {
        string venta_id PK
        int cliente_id FK
        date fecha_venta
        int producto_id FK
        int cantidad
        int precio_unitario
        int total_venta
        string canal_venta
    }

    PRODUCTOS {
        int producto_id PK
        string nombre_producto
        int categoria_id FK
    }

    CATEGORIAS {
        int categoria_id PK
        string nombre_categoria
    }
~~~




# Documento Resumen del Flujo de Trabajo
## Proyecto: Preparación de Datos con Python — Módulo 4

---

## 1. Justificación del Uso de NumPy y Pandas

La elección de estas bibliotecas fue central para construir un pipeline de datos automatizado, reproducible y eficiente:

### NumPy
Se utilizó para la **generación masiva de datos sintéticos** mediante arrays y operaciones vectorizadas:
- Generación de semillas aleatorias reproducibles (`np.random.seed(42)`) para garantizar consistencia entre ejecuciones.
- Selección aleatoria de atributos con distribuciones de probabilidad personalizadas (`np.random.choice` con pesos para regiones).
- Introducción controlada de ruido en el dataset: 15 nulos en `edad`, 10 en `ingreso_mensual`, 8 en `region`.
- Inyección de outliers en precios (2% de registros con valores entre $2.000.000 y $5.000.000).
- Cálculo de Z-scores mediante operaciones matriciales directas sobre arrays numéricos.

### Pandas
Actuó como la **herramienta central de manipulación de alto nivel** durante todas las etapas del proyecto:
- Carga e integración de múltiples fuentes de datos heterogéneas (CSV, Excel multi-hoja).
- Inspección rápida de calidad: detección de nulos, duplicados y tipos de datos.
- Aplicación de filtros condicionales complejos con sintaxis legible (`groupby`, `pivot_table`, `melt`, `merge`).
- Exportación de resultados en múltiples formatos (CSV y Excel con múltiples hojas).

---

## 2. Descripción del Dataset Generado y Fuentes Integradas

El proyecto consolidó un ecosistema de datos distribuido en **cinco fuentes** que simulan un entorno real de e-commerce:

| Fuente | Archivo | Descripción |
|---|---|---|
| Clientes | `clientes_ecommerce.csv` | 305 registros (300 base + 5 duplicados) con 11 atributos: `cliente_id`, `nombre`, `apellido`, `email`, `genero`, `fecha_registro`, `region`, `pais`, `edad`, `ingreso_mensual`, `activo` |
| Ventas 2025 | `ventas_ecommerce_2025_2026.xlsx` (hoja `ventas_2025`) | 1.000 transacciones con fechas entre el 01-01-2025 y 31-12-2025 |
| Ventas 2026 | `ventas_ecommerce_2025_2026.xlsx` (hoja `ventas_2026`) | 300 transacciones con fechas entre el 01-01-2026 y 31-12-2026 |
| Productos | `productos.csv` | Catálogo de 6 productos: Notebook, Mouse, Teclado, Monitor, Audífonos, Tablet |
| Categorías | `categorias.csv` | 2 categorías maestras: Tecnología y Accesorios |

### Dataset Consolidado
La integración de todas las fuentes produjo un DataFrame de **1.300 filas y 22 columnas**, que abarca la trazabilidad completa desde el cliente hasta cada transacción individual.

Cada registro de venta incluye: `venta_id`, `cliente_id`, `fecha_venta`, `producto_id`, `cantidad`, `precio_unitario`, `total_venta` y `canal_venta` (Web, App o Tienda Física).

---

## 3. Técnicas Aplicadas para la Limpieza y Transformación

El proceso se implementó de forma modular a través de las clases `DataCleaning` y `DataWrangling`, organizadas en scripts independientes por lección.

### 3.1 Limpieza de Datos (`L4_limpieza_analisis.py`)

**Identificación de valores nulos**
Se utilizó `isnull().sum()` para un diagnóstico inicial completo de cada columna.

**Imputación de valores faltantes**
- Columnas numéricas (`edad`, `ingreso_mensual`): imputación por **media** con `fillna(mean_val)`.
- Columnas categóricas (`region`, `genero`): imputación por **moda** con `fillna(mode_val)`.
- Casos residuales: **forward fill** (`ffill`) como respaldo.

**Detección y eliminación de outliers**
Se implementaron dos métodos complementarios:
- **Z-score** (`threshold=3`): eliminación de registros donde el valor estandarizado supera 3 desviaciones estándar en cualquier columna numérica.
- **IQR (Rango Intercuartílico)**: eliminación de registros fuera del rango `[Q1 - 1.5·IQR, Q3 + 1.5·IQR]` para todas las columnas numéricas.

**Validación de reglas de negocio**
Sistema extensible de validación funcional (`validate_data`) con reglas como verificar que todos los ingresos sean positivos.

### 3.2 Data Wrangling (`L5_DataWrangling.py`)

| Técnica | Descripción |
|---|---|
| Eliminación de duplicados | `drop_duplicates()` redujo el dataset de 305 a 300 registros únicos |
| Conversión de tipos | `cliente_id` → `int`, `ingreso_mensual` → `float`, `region` y `genero` → `category`, `activo` → `bool` |
| Columnas calculadas | `ingreso_anual = ingreso_mensual × 12`; `tipo_cliente` (Premium si ingreso > 10.000, Estándar en caso contrario) |
| Funciones personalizadas | Mapeo `M/F` → `Masculino/Femenino` con `map()`; columna `cliente_activo` con `apply()` y lambda |
| Normalización Min-Max | `ingreso_normalizado = (x - min) / (max - min)`, valores en el rango [0, 1] |
| Discretización | `pd.cut()` sobre `ingreso_mensual` con bins `[0, 3.000, 7.000, 20.000]` → etiquetas `Bajo`, `Medio`, `Alto` |

---

## 4. Principales Decisiones Tomadas y Desafíos Encontrados

### Decisiones de diseño

**Uso de LEFT JOIN en todos los cruces**
Se optó por `merge(..., how='left')` para conservar el 100% de los registros de ventas, incluso cuando algunos `cliente_id` no tenían correspondencia exacta en el catálogo de clientes.

**Normalización de la columna `genero`**
El dataset fue generado con inconsistencias intencionales (mezcla de `"M"/"F"` y `"Masculino"/"Femenino"`). Se resolvió mediante un mapeo explícito con `apply()` antes de los cruces, garantizando uniformidad semántica.

**Renombramiento de columnas ambiguas**
Se renombró `activo` a `cliente_activo` para evitar colisiones con columnas homónimas de otras fuentes y mejorar la legibilidad del dataset consolidado.

**Arquitectura modular por lección**
Cada etapa del pipeline fue encapsulada en clases independientes (`DataCleaning`, `DataWrangling`, `DataAnalysis`), facilitando el mantenimiento y la reutilización del código.

### Desafíos encontrados

**Introducción controlada de ruido**
Diseñar un dataset con deficiencias realistas (nulos, duplicados, outliers) sin comprometer la viabilidad del análisis posterior requirió calibrar cuidadosamente los porcentajes de ruido inyectado.

**Compatibilidad de tipos al aplicar filtros**
La columna `activo` (booleana) generó inconsistencias al aplicar filtros numéricos. Se resolvió mediante conversión explícita de tipos antes de aplicar `DataCleaning`.

**Gestión de rutas entre scripts**
Los scripts pueden ejecutarse desde la raíz del proyecto o desde `src/`. Se utilizó `os.path.abspath(__file__)` para calcular rutas relativas dinámicamente y evitar errores de `FileNotFoundError`.

---

## 5. Resultados Obtenidos y Estado Final del Dataset

### Estado final del dataset consolidado

- **Dimensiones:** 1.300 filas × 22 columnas
- **Orden lógico de columnas:** Identificadores → Atributos del cliente → Producto → Transacción
- **Sin valores nulos:** imputación completa verificada
- **Sin duplicados:** verificado con `drop_duplicates()`
- **Sin outliers graves:** filtrado por IQR y Z-score

### Métricas generadas (`L6_Agrup_Pivot.py`)

| Técnica | Resultado |
|---|---|
| `groupby(['region', 'categoria_ingreso'])` | Estadísticas de ingreso (media, mediana, máx., mín., conteo) por región y segmento |
| `pivot_table(values='ingreso_mensual', index='region', columns='categoria_ingreso')` | Tabla cruzada de ingresos promedio por región y categoría |
| `melt()` | Transformación wide → long sobre `ingreso_mensual` e `ingreso_anual` |
| `merge()` | Integración con tabla de zonas comerciales por región |
| `concat()` | Unión de datasets para análisis ampliado |

### Archivos de salida generados

| Archivo | Contenido |
|---|---|
| `data/df_consolidado.csv` | Dataset final unificado (1.300 registros, 22 columnas) |
| `data/analisis_final.xlsx` | Libro Excel con 5 hojas: Groupby, Pivot, Melt, Merge, Concat |
| `data/metricas_groupby.csv` | Estadísticas agrupadas por región y categoría de ingreso |
| `data/tabla_pivot.csv` | Tabla pivot de ingreso promedio |
| `data/datos_melt.csv` | Dataset en formato largo |
| `data/datos_merge.csv` | Dataset enriquecido con zonas comerciales |

### Estado de preparación del dato

El dataset final está **listo para uso inmediato** en:
- Reportes estratégicos de ventas segmentadas por región y canal.
- Modelos de segmentación de clientes (clustering).
- Análisis predictivos de comportamiento de compra.
- Dashboards de Business Intelligence.

---

*Documento generado como parte del Proyecto Módulo 4 — Preparación de Datos con Python.*