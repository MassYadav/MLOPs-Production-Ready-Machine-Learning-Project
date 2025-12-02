import os
import sys
import pandas as pd

from evidently.report import Report
from evidently.metric_preset import DataDriftPreset

from pandas import DataFrame

from us_visa.exception import USvisaException
from us_visa.logger import logging
from us_visa.entity.config_entity import DataValidationConfig
from us_visa.entity.artifact_entity import DataIngestionArtifact, DataValidationArtifact
from us_visa.utils.main_utils import read_yaml_file, write_yaml_file

# ✅ New Evidently imports
from evidently.report import Report
from evidently.metric_preset import DataDriftPreset


class DataValidation:
    def __init__(self, data_ingestion_artifact: DataIngestionArtifact,
                 data_validation_config: DataValidationConfig):
        """
        Data Validation step:
        - Loads train/test data
        - Validates schema
        - Detects data drift using Evidently
        """
        try:
            self.data_ingestion_artifact = data_ingestion_artifact
            self.data_validation_config = data_validation_config
            self.schema_config = read_yaml_file(data_validation_config.schema_file_path)

        except Exception as e:
            raise USvisaException(e, sys) from e

    def validate_schema(self, df: pd.DataFrame) -> bool:
        """
        Validate dataset schema (column names & count).
        """
        try:
            expected_columns = list(self.schema_config["columns"].keys())
            actual_columns = list(df.columns)

            if expected_columns != actual_columns:
                logging.error(f"Schema mismatch! Expected: {expected_columns}, Got: {actual_columns}")
                return False

            logging.info("Schema validation successful ✅")
            return True

        except Exception as e:
            raise USvisaException(e, sys) from e

    def detect_dataset_drift(self, reference_df: pd.DataFrame, current_df: pd.DataFrame) -> bool:
        """
        Detect dataset drift between reference (train) and current (test) datasets.
        Uses Evidently Report with DataDriftPreset.
        """
        try:
            # ✅ Create Evidently Report
            drift_report = Report(metrics=[DataDriftPreset()])
            drift_report.run(reference_data=reference_df, current_data=current_df)

            # ✅ Convert to dictionary
            report_dict = drift_report.as_dict()

            # ✅ Save drift report in YAML
            write_yaml_file(
                file_path=self.data_validation_config.drift_report_file_path,
                content=report_dict
            )

            # ✅ Extract drift results
            drift_result = report_dict["metrics"][0]["result"]
            drift_status = drift_result["dataset_drift"]  # True/False
            drift_by_columns = drift_result["drift_by_columns"]

            n_features = len(drift_by_columns)
            n_drifted = sum(drift_by_columns.values())

            logging.info(f"Drift detected in {n_drifted}/{n_features} features.")

            return drift_status

        except Exception as e:
            raise USvisaException(e, sys) from e

    def initiate_data_validation(self) -> DataValidationArtifact:
        """
        Main entry point:
        - Loads train/test
        - Validates schema
        - Runs data drift check
        - Returns DataValidationArtifact
        """
        try:
            # ✅ Load train/test data
            train_df = pd.read_csv(self.data_ingestion_artifact.train_file_path)
            test_df = pd.read_csv(self.data_ingestion_artifact.test_file_path)

            # ✅ Schema validation
            is_train_valid = self.validate_schema(train_df)
            is_test_valid = self.validate_schema(test_df)

            if not (is_train_valid and is_test_valid):
                raise USvisaException("Schema validation failed ❌", sys)

            # ✅ Data drift detection
            drift_status = self.detect_dataset_drift(train_df, test_df)

            # ✅ Create validation artifact
            data_validation_artifact = DataValidationArtifact(
                schema_file_path=self.data_validation_config.schema_file_path,
                drift_report_file_path=self.data_validation_config.drift_report_file_path,
                validation_status=True,
                message="Data validation completed successfully ✅"
            )

            logging.info(f"Data Validation Artifact: {data_validation_artifact}")
            return data_validation_artifact

        except Exception as e:
            raise USvisaException(e, sys) from e
