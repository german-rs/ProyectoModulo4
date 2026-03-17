import pandas as pd
import logging
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class DataWrangling:

    def __init__(self, dataframe):
        self.dataframe = dataframe.copy()

    # 1. Eliminar duplicados
    def remove_duplicates(self):
        logging.info("Eliminando registros duplicados.")
        before = len(self.dataframe)

        self.dataframe = self.dataframe.drop_duplicates()

        after = len(self.dataframe)

        logging.info(f"Registros eliminados: {before - after}")

        return self.dataframe

    # 2. Transformar tipos de datos
    def transform_data_types(self):
        logging.info("Transformando tipos de datos.")

        self.dataframe['cliente_id'] = self.dataframe['cliente_id'].astype(int)
        self.dataframe['ingreso_mensual'] = self.dataframe['ingreso_mensual'].astype(float)
        self.dataframe['region'] = self.dataframe['region'].astype('category')
        self.dataframe['genero'] = self.dataframe['genero'].astype('category')
        self.dataframe['activo'] = self.dataframe['activo'].astype(bool)

        return self.dataframe

    # 3. Crear columnas calculadas
    def create_calculated_columns(self):
        logging.info("Creando columnas calculadas.")

        # ingreso anual
        self.dataframe['ingreso_anual'] = self.dataframe['ingreso_mensual'] * 12

        # clasificación simple de cliente
        self.dataframe['tipo_cliente'] = self.dataframe['ingreso_mensual'].apply(
            lambda x: "Premium" if x > 10000 else "Estandar"
        )

        return self.dataframe

    # 4. Aplicar funciones personalizadas
    def apply_custom_functions(self):
        logging.info("Aplicando funciones personalizadas.")

        # Mapear genero a texto completo
        genero_map = {
            'M': 'Masculino',
            'F': 'Femenino'
        }

        self.dataframe['genero_desc'] = self.dataframe['genero'].map(genero_map)

        # Columna calculada usando apply
        self.dataframe['cliente_activo'] = self.dataframe['activo'].apply(
            lambda x: "Activo" if x else "Inactivo"
        )

        return self.dataframe

    # 5. Normalizar ingreso mensual
    def normalize_income(self):
        logging.info("Normalizando ingreso mensual (Min-Max).")

        min_val = self.dataframe['ingreso_mensual'].min()
        max_val = self.dataframe['ingreso_mensual'].max()

        self.dataframe['ingreso_normalizado'] = (
            (self.dataframe['ingreso_mensual'] - min_val) /
            (max_val - min_val)
        )

        return self.dataframe

    # 6. Discretizar ingresos
    def discretize_income(self):
        logging.info("Discretizando ingresos en categorías.")

        bins = [0, 3000, 7000, 20000]
        labels = ['Bajo', 'Medio', 'Alto']

        self.dataframe['categoria_ingreso'] = pd.cut(
            self.dataframe['ingreso_mensual'],
            bins=bins,
            labels=labels
        )

        return self.dataframe


if __name__ == "__main__":

    logging.info("Iniciando proceso de Data Wrangling.")

    # 1. Cargar dataset limpio de la Lección 4
    try:

        df = pd.read_csv("data/clientes_limpios.csv")

        logging.info("Dataset cargado desde data/clientes_limpios.csv")

    except FileNotFoundError:

        df = pd.read_csv("data/clientes_limpios.csv")

        logging.warning("Archivo cargado desde directorio actual.")

    # 2. Instanciar clase
    wrangler = DataWrangling(df)

    # 3. Aplicar técnicas de wrangling
    wrangler.remove_duplicates()

    wrangler.transform_data_types()

    wrangler.create_calculated_columns()

    wrangler.apply_custom_functions()

    wrangler.normalize_income()

    wrangler.discretize_income()

    df_final = wrangler.dataframe

    # 4. Mostrar resultado
    print("\n" + "=" * 40)
    print("DATAFRAME OPTIMIZADO (LECCIÓN 5)")
    print("=" * 40)

    print(df_final.head())

    # 5. Guardar dataset optimizado
    try:

        os.makedirs("data/data", exist_ok=True)

        df_final.to_csv("data/clientes_wrangled.csv", index=False)

        logging.info("Dataset optimizado guardado en data/clientes_wrangled.csv")

    except Exception as e:

        df_final.to_csv("clientes_wrangled.csv", index=False)

        logging.warning("Guardado en directorio actual.")
        logging.warning(e)