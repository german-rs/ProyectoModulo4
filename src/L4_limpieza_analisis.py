import pandas as pd
import numpy as np
import logging

# Setting up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DataCleaning:
    def __init__(self, dataframe):
        self.dataframe = dataframe

    def z_score_outlier_detection(self, threshold=3):
        """Detect outliers using Z-score."
        logging.info('Applying Z-score outlier detection.')
        z_scores = np.abs((self.dataframe - self.dataframe.mean()) / self.dataframe.std())
        return self.dataframe[(z_scores < threshold).all(axis=1)]

    def iqr_outlier_detection(self):
        """Detect outliers using IQR."
        logging.info('Applying IQR outlier detection.')
        q1 = self.dataframe.quantile(0.25)
        q3 = self.dataframe.quantile(0.75)
        iqr = q3 - q1
        return self.dataframe[~((self.dataframe < (q1 - 1.5 * iqr)) | (self.dataframe > (q3 + 1.5 * iqr))).any(axis=1)]

    def impute_missing_values(self):
        """Impute missing values using mean, mode, and forward-fill."
        logging.info('Imputing missing values.')
        for col in self.dataframe.columns:
            if self.dataframe[col].isnull().any():
                if self.dataframe[col].dtype in ['int64', 'float64']:
                    self.dataframe[col].fillna(self.dataframe[col].mean(), inplace=True)
                else:
                    self.dataframe[col].fillna(self.dataframe[col].mode()[0], inplace=True)
        self.dataframe.fillna(method='ffill', inplace=True)

    def validate_data(self, business_rules):
        """Validate data against business rules."
        logging.info('Validating data with business rules.')
        for rule in business_rules:
            if not rule(self.dataframe):
                logging.warning('Business rule failed.')
                return False
        return True

    def segmented_statistics(self, segment_by):
        """Calculate segmented statistics."
        logging.info('Calculating segmented statistics.')
        return self.dataframe.groupby(segment_by).describe()

# Example usage:
# df = pd.read_csv('data.csv')
# cleaner = DataCleaning(df)
# clean_data = cleaner.z_score_outlier_detection()
# cleaner.impute_missing_values()
# if cleaner.validate_data(business_rules):
#     stats = cleaner.segmented_statistics(['region', 'gender', 'sales_channel', 'product_category'])
