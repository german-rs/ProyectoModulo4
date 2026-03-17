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

# Informe de Procesamiento de Datos con NumPy y Pandas

---

## 1. Justificación del uso de NumPy y Pandas

La elección de estas herramientas fue fundamental para desarrollar un flujo de trabajo automatizado y eficiente:

- **NumPy:** Se utilizó para la generación masiva de datos sintéticos y el manejo eficiente de estructuras numéricas a través de arrays. Su capacidad para realizar operaciones matemáticas básicas con alto rendimiento permitió simular un volumen significativo de transacciones y clientes con ruido controlado.

- **Pandas:** Actuó como la herramienta central para la manipulación de alto nivel. Permitió integrar fuentes de datos heterogéneas, realizar exploraciones rápidas de calidad (nulos, duplicados) y aplicar filtros condicionales complejos de forma intuitiva, reduciendo la complejidad del código.

---

## 2. Descripción del Dataset y Fuentes Integradas

El proyecto consolidó un ecosistema de datos distribuido en cinco fuentes principales que simulan un entorno de negocios real:

- **Clientes** (`clientes_ecommerce.csv`): 305 registros iniciales (antes de limpieza) con 11 atributos demográficos como edad, género, región e ingreso mensual.

- **Productos y Categorías** (`productos.csv` y `categorias.csv`): Catálogos maestros con 6 productos y 2 categorías (Tecnología y Accesorios).

- **Ventas Históricas (Excel):** Un archivo con dos hojas que contienen 1.000 transacciones de 2025 y 300 transacciones de 2026.

- **Estado Consolidado:** El resultado de la integración fue un DataFrame de 1.300 filas y 22 columnas que abarca la trazabilidad completa desde el cliente hasta la transacción.

---

## 3. Técnicas de Limpieza y Transformación

Se implementó un flujo modularizado en Python para garantizar la calidad del dato:

- **Gestión de Nulos:** Identificación de valores faltantes y aplicación de imputación por media para variables numéricas (como ingresos) e imputación por moda para categóricas (como género).

- **Tratamiento de Outliers:** Uso de métodos estadísticos de Z-score y Rango Intercuartílico (IQR) para filtrar registros atípicos, como ingresos de $100.000 o precios unitarios superiores a $2M que distorsionaban los promedios.

- **Data Wrangling:** Eliminación de duplicados (reduciendo a 300 clientes únicos), normalización Min-Max de ingresos y discretización en categorías (Bajo, Medio, Alto).

---

## 4. Principales Decisiones y Desafíos

Durante el desarrollo se tomaron decisiones críticas para asegurar la integridad de la información:

- **Integridad Referencial:** Debido a la falta de restricciones de clave foránea, se optó por un `LEFT JOIN` en todos los cruces. Esto aseguró conservar el 100% de las ventas, identificando registros sin relación en los catálogos en lugar de eliminarlos.

- **Normalización de Formatos:** Se resolvió la inconsistencia en la columna `genero` (mezcla de `"M/F"` y `"Masculino/Femenino"`) estandarizando todos los valores a texto completo antes del JOIN.

- **Resolución de Colisiones:** Se renombraron columnas ambiguas (ej: de `activo` a `cliente_activo`) para mejorar la semántica del dataset final.

---

## 5. Resultados Obtenidos y Estado Final

El proceso culminó con un dataset estructurado y listo para análisis avanzado:

- **Estructura Final:** Una tabla plana de 1.300 registros con 22 atributos normalizados y ordenados lógicamente: Identificadores → Cliente → Producto → Transacción.

- **Métricas Resumidas:** Generación de estadísticas segmentadas por región y categoría de ingreso mediante técnicas de `groupby()` y `pivot_table()`.

- **Disponibilidad:** Los resultados fueron exportados en formatos CSV y Excel (`analisis_final.xlsx`), permitiendo su uso inmediato en reportes estratégicos o modelos de machine learning.