# Módulo de Limpieza de Datos y Análisis Avanzado

## Descripción
Este módulo se encarga de limpiar datos, tratar valores atípicos, imputar valores perdidos y realizar un análisis avanzado basado en segmentos. 

## Funciones

### 1. Tratamiento de Outliers
- **Método Z-score**: Identifica outliers basados en cuántas desviaciones estándar están por encima o por debajo de la media.
- **IQR**: Utiliza el rango intercuartílico para detectar outliers por medio de un enfoque basado en percentiles.

### 2. Imputación de Valores Perdidos
- **Estrategias**: Se implementan las siguientes estrategias de imputación: media, mediana, modo y forward-fill.
- **Logging**: Registra cada paso de la imputación.

### 3. Validación de Datos
Incluye reglas de negocio para asegurar que los datos cumplen con los estándares requeridos.

### 4. Estadísticas Segmentadas
Genera estadísticas avanzadas segmentadas por región, género, canal de venta y categoría de producto.

### 5. Documentación y Estructura
- La función está bien documentada con descripciones claras.
- Se utilizan anotaciones de tipo para mejorar la legibilidad.
- Las funciones son modulares y reutilizables.

## Ejemplo de Código
```python
import pandas as pd
import numpy as np
import logging

# Configuración del logging
logging.basicConfig(level=logging.INFO)

def tratamiento_outliers(data: pd.DataFrame, method: str = 'z_score') -> pd.DataFrame:
    if method == 'z_score':
        mean = np.mean(data)
        std_dev = np.std(data)
        threshold = 3
        return data[(data > mean - threshold * std_dev) & (data < mean + threshold * std_dev)]
    elif method == 'iqr':
        Q1 = data.quantile(0.25)
        Q3 = data.quantile(0.75)
        IQR = Q3 - Q1
        return data[(data >= Q1 - 1.5 * IQR) & (data <= Q3 + 1.5 * IQR)]
    else:
        logging.error('Método no soportado.')

# Más funciones aquí...
``` 

## Módulo Principal
Este módulo es cargado y ejecutado en el entorno de ejecución principal donde se espera que los datos sean pasados como un DataFrame de pandas.
