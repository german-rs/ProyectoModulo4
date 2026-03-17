import pandas as pd
import numpy as np
import logging
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class DataCleaning:
    def __init__(self, dataframe):
        self.dataframe = dataframe.copy()

    def identify_missing_values(self):
        logging.info("Identificando valores nulos en el dataset.")
        return self.dataframe.isnull().sum()

    def impute_missing_values(self):
        logging.info('Imputando valores faltantes según tipo de dato.')

        for col in self.dataframe.columns:

            if self.dataframe[col].isnull().any():

                # columnas numéricas
                if pd.api.types.is_numeric_dtype(self.dataframe[col]) and self.dataframe[col].dtype != bool:
                    mean_val = self.dataframe[col].mean()
                    self.dataframe[col] = self.dataframe[col].fillna(mean_val)

                # columnas categóricas
                else:
                    mode_val = self.dataframe[col].mode()
                    if not mode_val.empty:
                        self.dataframe[col] = self.dataframe[col].fillna(mode_val.iloc[0])

        # forward fill para posibles casos restantes
        self.dataframe.ffill(inplace=True)

        return self.dataframe

    def z_score_outlier_detection(self, threshold=3):

        logging.info('Aplicando detección de outliers por Z-score.')

        numeric_df = self.dataframe.select_dtypes(include=['int64', 'float64'])

        z_scores = np.abs((numeric_df - numeric_df.mean()) / numeric_df.std(ddof=0))

        mask = (z_scores < threshold).all(axis=1)

        self.dataframe = self.dataframe[mask]

        return self.dataframe

    def iqr_outlier_detection(self):

        logging.info('Aplicando detección de outliers por IQR.')

        numeric_df = self.dataframe.select_dtypes(include=['int64', 'float64'])

        q1 = numeric_df.quantile(0.25)
        q3 = numeric_df.quantile(0.75)

        iqr = q3 - q1

        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr

        mask = ~((numeric_df < lower_bound) | (numeric_df > upper_bound)).any(axis=1)

        self.dataframe = self.dataframe[mask]

        return self.dataframe

    def validate_data(self, business_rules):

        logging.info('Validando reglas de negocio.')

        for rule in business_rules:
            if not rule(self.dataframe):
                logging.warning(f'Regla de negocio fallida: {rule.__name__}')
                return False

        return True

    def segmented_statistics(self, segment_by):

        logging.info(f'Calculando estadísticas segmentadas por: {segment_by}')

        return self.dataframe.groupby(segment_by).agg({
            'ingreso_mensual': ['mean', 'median', 'max', 'min', 'count']
        })


if __name__ == "__main__":

    # 1. Simulación de datos
    data = {
        'cliente_id': list(range(5, 12)),
        'ingreso_mensual': [2500, 3200, 15000, np.nan, 2800, 4000, 100000],
        'region': ['Norte', 'Sur', 'Norte', 'Este', 'Oeste', 'Sur', 'Norte'],
        'genero': ['M', 'F', 'F', 'M', np.nan, 'F', 'M'],
        'activo': [True, True, False, True, True, False, True]
    }

    df_ecommerce = pd.DataFrame(data)

    logging.info("Iniciando proceso de limpieza de datos")

    # 2. Instanciar clase
    cleaner = DataCleaning(df_ecommerce)

    # 3. Identificar valores nulos
    print("\nValores nulos detectados:")
    print(cleaner.identify_missing_values())

    # 4. Imputar valores nulos
    cleaner.impute_missing_values()

    # 5. Detectar outliers
    cleaner.iqr_outlier_detection()
    cleaner.z_score_outlier_detection(threshold=2)

    # 6. Reglas de negocio
    def regla_ingresos_positivos(df):
        return (df['ingreso_mensual'] > 0).all()

    reglas = [regla_ingresos_positivos]

    if cleaner.validate_data(reglas):

        logging.info("Validación exitosa")

        # 7. Estadísticas por región
        resumen_stats = cleaner.segmented_statistics('region')

        print("\n" + "=" * 40)
        print("RESULTADO DEL ANÁLISIS")
        print("=" * 40)
        print(resumen_stats)

        # 8. Exportar dataset limpio
        output_path = "data/clientes_limpios.csv"

        try:

            os.makedirs("data", exist_ok=True)

            cleaner.dataframe.to_csv(output_path, index=False)

            logging.info(f"Dataset exportado en {output_path}")

        except Exception as e:

            cleaner.dataframe.to_csv("clientes_limpios.csv", index=False)

            logging.warning("Guardado en directorio actual por error:")
            logging.warning(e)