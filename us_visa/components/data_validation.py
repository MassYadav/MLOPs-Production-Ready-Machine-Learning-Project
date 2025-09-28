import json
import sys
import pandas as pd

from evidently.report import Report
from evidently.metric_preset import DataDriftPreset

from pandas import DataFrame

from us_visa.exception import USvisaException
from us_visa.logger import logging
from us_visa.utils.main_utils import read_yaml_file, write_yaml_file
from us_visa.entity.artifact_entity import DataIngestionArtifact, DataValidationArtifact
from us_visa.entity.config_entity import DataValidationConfig
from us_visa.constants import SCHEMA_FILE_PATH


class DataValidation:
    def __init__(self, data_ingestion_artifact: DataIngestionArtifact, data_validation_config: DataValidationConfig):
        """
        :param data_ingestion_artifact: Output reference of data ingestion artifact stage
        :param data_validation_config: configuration for data validation
        """
        try:
            self.data_ingestion_artifact = data_ingestion_artifact
            self.data_validation_config = data_validation_config
            self._schema_config = read_yaml_file(file_path=SCHEMA_FILE_PATH)
        except Exception as e:
            raise USvisaException(e, sys)

    def validate_number_of_columns(self, dataframe: DataFrame) -> bool:
        """Validates the number of columns"""
        try:
            status = len(dataframe.columns) == len(self._schema_config["columns"])
            logging.info(f"Is required column present: [{status}]")
            return status
        except Exception as e:
            raise USvisaException(e, sys)

    def is_column_exist(self, df: DataFrame) -> bool:
        """Validates the existence of numerical and categorical columns"""
        try:
            dataframe_columns = df.columns
            missing_numerical_columns = []
            missing_categorical_columns = []

            for column in self._schema_config["numerical_columns"]:
                if column not in dataframe_columns:
                    missing_numerical_columns.append(column)

            if missing_numerical_columns:
                logging.info(f"Missing numerical column: {missing_numerical_columns}")

            for column in self._schema_config["categorical_columns"]:
                if column not in dataframe_columns:
                    missing_categorical_columns.append(column)

            if missing_categorical_columns:
                logging.info(f"Missing categorical column: {missing_categorical_columns}")

            return not (missing_numerical_columns or missing_categorical_columns)
        except Exception as e:
            raise USvisaException(e, sys) from e

    @staticmethod
    def read_data(file_path) -> DataFrame:
        """Reads CSV data into DataFrame"""
        try:
            return pd.read_csv(file_path)
        except Exception as e:
            raise USvisaException(e, sys)

    def detect_dataset_drift(self, reference_df: DataFrame, current_df: DataFrame) -> bool:
        """Detects dataset drift using Evidently Report API"""
        try:
            # Generate drift report
            data_drift_report = Report(metrics=[DataDriftPreset()])
            data_drift_report.run(reference_data=reference_df, current_data=current_df)

            # Convert report to dict
            json_report = data_drift_report.as_dict()

            # Save as YAML
            write_yaml_file(
                file_path=self.data_validation_config.drift_report_file_path,
                content=json_report
            )

            # Extract metrics
            n_features = json_report["metrics"][0]["result"]["number_of_features"]
            n_drifted_features = json_report["metrics"][0]["result"]["number_of_drifted_features"]

            logging.info(f"{n_drifted_features}/{n_features} features show drift.")

            drift_status = json_report["metrics"][0]["result"]["dataset_drift"]
            return drift_status
        except Exception as e:
            raise USvisaException(e, sys) from e

    def initiate_data_validation(self) -> DataValidationArtifact:
        """Runs all data validation steps"""
        try:
            validation_error_msg = ""
            logging.info("Starting data validation")

            train_df, test_df = (
                DataValidation.read_data(file_path=self.data_ingestion_artifact.trained_file_path),
                DataValidation.read_data(file_path=self.data_ingestion_artifact.test_file_path)
            )

            # Column count check
            if not self.validate_number_of_columns(dataframe=train_df):
                validation_error_msg += "Columns are missing in training dataframe. "
            if not self.validate_number_of_columns(dataframe=test_df):
                validation_error_msg += "Columns are missing in test dataframe. "

            # Schema column check
            if not self.is_column_exist(df=train_df):
                validation_error_msg += "Required columns are missing in training dataframe. "
            if not self.is_column_exist(df=test_df):
                validation_error_msg += "Required columns are missing in test dataframe. "

            validation_status = len(validation_error_msg) == 0

            if validation_status:
                drift_status = self.detect_dataset_drift(train_df, test_df)
                if drift_status:
                    logging.info("Drift detected.")
                    validation_error_msg = "Drift detected"
                else:
                    validation_error_msg = "Drift not detected"
            else:
                logging.info(f"Validation_error: {validation_error_msg}")

            data_validation_artifact = DataValidationArtifact(
                validation_status=validation_status,
                message=validation_error_msg,
                drift_report_file_path=self.data_validation_config.drift_report_file_path
            )

            logging.info(f"Data validation artifact: {data_validation_artifact}")
            return data_validation_artifact

        except Exception as e:
            raise USvisaException(e, sys) from e
