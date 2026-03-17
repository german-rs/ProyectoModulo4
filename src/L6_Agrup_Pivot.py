import pandas as pd
import logging
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class DataAnalysis:

    def __init__(self, dataframe):
        self.dataframe = dataframe.copy()

    # 1️⃣ Agrupamiento
    def group_statistics(self):

        logging.info("Calculando métricas resumidas con groupby().")

        resumen = self.dataframe.groupby(['region', 'categoria_ingreso']).agg({
            'ingreso_mensual': ['mean', 'median', 'max', 'min', 'count'],
            'ingreso_anual': ['mean']
        })

        return resumen

    # 2️⃣ Pivot
    def pivot_table(self):

        logging.info("Creando tabla pivot.")

        pivot = self.dataframe.pivot_table(
            values='ingreso_mensual',
            index='region',
            columns='categoria_ingreso',
            aggfunc='mean'
        )

        return pivot

    # 3️⃣ Melt (wide → long)
    def melt_dataframe(self):

        logging.info("Transformando dataset con melt().")

        melted = pd.melt(
            self.dataframe,
            id_vars=['cliente_id', 'region'],
            value_vars=['ingreso_mensual', 'ingreso_anual'],
            var_name='tipo_ingreso',
            value_name='valor'
        )

        return melted

    # 4️⃣ Merge con nueva fuente simulada
    def merge_new_source(self):

        logging.info("Combinando nueva fuente de datos con merge().")

        regiones_info = pd.DataFrame({
            'region': ['Norte', 'Sur', 'Este', 'Oeste'],
            'pais': ['Chile', 'Chile', 'Chile', 'Chile'],
            'zona_comercial': ['Zona A', 'Zona B', 'Zona C', 'Zona D']
        })

        merged = pd.merge(
            self.dataframe,
            regiones_info,
            on='region',
            how='left'
        )

        return merged

    # 5️⃣ Concat para unir datasets
    def concat_datasets(self):

        logging.info("Concatenando datasets simulados.")

        df_extra = self.dataframe.sample(frac=0.3, replace=True)

        concatenado = pd.concat(
            [self.dataframe, df_extra],
            ignore_index=True
        )

        return concatenado


def generar_documento_resumen():

    resumen = """
PROYECTO: Preparación de Datos con Python

Flujo de trabajo realizado:

Lección 1 - NumPy
Se generaron datos ficticios de clientes utilizando arrays de NumPy.
Se aplicaron operaciones matemáticas básicas para comprender la manipulación eficiente de datos numéricos.

Lección 2 - Pandas
Los datos fueron convertidos en un DataFrame de Pandas.
Se exploraron mediante estadísticas descriptivas y visualización de registros iniciales.

Lección 3 - Obtención de datos
Se integraron datos desde múltiples fuentes como CSV, Excel y tablas web.
Los datasets fueron unificados en un DataFrame consolidado.

Lección 4 - Limpieza de datos
Se identificaron valores nulos.
Se aplicaron técnicas de imputación para datos faltantes.
Se detectaron y eliminaron outliers utilizando métodos IQR y Z-score.
Se generó un dataset limpio.

Lección 5 - Data Wrangling
Se eliminaron registros duplicados.
Se transformaron tipos de datos.
Se crearon nuevas columnas calculadas como ingreso anual.
Se aplicaron funciones personalizadas (apply, map, lambda).
Se normalizaron y discretizaron columnas.

Lección 6 - Agrupamiento y Pivot
Se aplicaron técnicas de agrupamiento con groupby() para generar métricas resumidas.
Se reorganizaron datos utilizando pivot_table().
Se transformaron estructuras de datos mediante melt().
Se combinaron datasets utilizando merge() y concat().

Resultado Final
Se generó un dataset estructurado, limpio y listo para análisis o uso en modelos analíticos.

Tecnologías utilizadas
- Python
- NumPy
- Pandas
"""

    with open("data/resumen_proyecto.md", "w", encoding="utf-8") as f:
        f.write(resumen)


if __name__ == "__main__":

    logging.info("Iniciando análisis de agrupamiento y pivot.")

    # 1️⃣ Cargar dataset de Lección 5
    try:

        df = pd.read_csv("data/clientes_wrangled.csv")

        logging.info("Dataset cargado desde data/clientes_wrangled.csv")

    except FileNotFoundError:

        df = pd.read_csv("clientes_wrangled.csv")

        logging.warning("Dataset cargado desde directorio actual.")

    analyzer = DataAnalysis(df)

    # 2️⃣ Agrupamiento
    resumen_groupby = analyzer.group_statistics()

    print("\n====== MÉTRICAS AGRUPADAS ======")
    print(resumen_groupby)

    # 3️⃣ Pivot
    pivot_table = analyzer.pivot_table()

    print("\n====== TABLA PIVOT ======")
    print(pivot_table)

    # 4️⃣ Melt
    melted_df = analyzer.melt_dataframe()

    print("\n====== DATASET MELT ======")
    print(melted_df.head())

    # 5️⃣ Merge
    merged_df = analyzer.merge_new_source()

    print("\n====== DATASET MERGE ======")
    print(merged_df.head())

    # 6️⃣ Concat
    concat_df = analyzer.concat_datasets()

    print("\n====== DATASET CONCAT ======")
    print(concat_df.head())

    # 7️⃣ Guardar resultados
    os.makedirs("data", exist_ok=True)

    resumen_groupby.to_csv("data/metricas_groupby.csv")

    pivot_table.to_csv("data/tabla_pivot.csv")

    melted_df.to_csv("data/datos_melt.csv", index=False)

    merged_df.to_csv("data/datos_merge.csv", index=False)

    concat_df.to_csv("data/datos_concat.csv", index=False)

    # Exportar Excel
    with pd.ExcelWriter("data/analisis_final.xlsx") as writer:

        resumen_groupby.to_excel(writer, sheet_name="Groupby")

        pivot_table.to_excel(writer, sheet_name="Pivot")

        melted_df.to_excel(writer, sheet_name="Melt")

        merged_df.to_excel(writer, sheet_name="Merge")

        concat_df.to_excel(writer, sheet_name="Concat")

    # 8️⃣ Generar documento resumen
    generar_documento_resumen()

    logging.info("Análisis final exportado correctamente.")