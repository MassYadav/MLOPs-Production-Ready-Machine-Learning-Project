import sys
from us_visa.exception import USvisaException
from us_visa.logger import logging

from us_visa.components.data_ingestion import DataIngestion
from us_visa.components.data_validation import DataValidation
# from us_visa.components.data_transformation import DataTransformation
# from us_visa.components.model_trainer import ModelTrainer
# from us_visa.components.model_evaluation import ModelEvaluation
# from us_visa.components.model_pusher import ModelPusher

from us_visa.entity.config_entity import (
    DataIngestionConfig,
    DataValidationConfig,
    # DataTransformationConfig,
    # ModelTrainerConfig,
    # ModelEvaluationConfig,
    # ModelPusherConfig
)

from us_visa.entity.artifact_entity import (
    DataIngestionArtifact,
    DataValidationArtifact,
    # DataTransformationArtifact,
    # ModelTrainerArtifact,
    # ModelEvaluationArtifact,
    # ModelPusherArtifact
)


class TrainPipeline:
    def __init__(self):
        # Active configs
        self.data_ingestion_config = DataIngestionConfig()
        self.data_validation_config = DataValidationConfig()

        # Inactive configs (commented for now)
        # self.data_transformation_config = DataTransformationConfig()
        # self.model_trainer_config = ModelTrainerConfig()
        # self.model_evaluation_config = ModelEvaluationConfig()
        # self.model_pusher_config = ModelPusherConfig()

    def start_data_ingestion(self) -> DataIngestionArtifact:
        """
        Start data ingestion component
        """
        try:
            logging.info("Entered start_data_ingestion method of TrainPipeline")
            logging.info("Fetching data from MongoDB")

            data_ingestion = DataIngestion(data_ingestion_config=self.data_ingestion_config)
            data_ingestion_artifact = data_ingestion.initiate_data_ingestion()

            logging.info("Data ingestion completed successfully")
            return data_ingestion_artifact
        except Exception as e:
            raise USvisaException(e, sys) from e

    def start_data_validation(self, data_ingestion_artifact: DataIngestionArtifact) -> DataValidationArtifact:
        """
        Start data validation component
        """
        try:
            data_validation = DataValidation(
                data_ingestion_artifact=data_ingestion_artifact,
                data_validation_config=self.data_validation_config
            )
            data_validation_artifact = data_validation.initiate_data_validation()
            return data_validation_artifact
        except Exception as e:
            raise USvisaException(e, sys) from e

    # def start_data_transformation(self, data_ingestion_artifact: DataIngestionArtifact,
    #                               data_validation_artifact: DataValidationArtifact) -> DataTransformationArtifact:
    #     try:
    #         data_transformation = DataTransformation(
    #             data_ingestion_artifact=data_ingestion_artifact,
    #             data_validation_artifact=data_validation_artifact,
    #             data_transformation_config=self.data_transformation_config
    #         )
    #         return data_transformation.initiate_data_transformation()
    #     except Exception as e:
    #         raise USvisaException(e, sys)

    # def start_model_trainer(self, data_transformation_artifact: DataTransformationArtifact) -> ModelTrainerArtifact:
    #     try:
    #         model_trainer = ModelTrainer(
    #             data_transformation_artifact=data_transformation_artifact,
    #             model_trainer_config=self.model_trainer_config
    #         )
    #         return model_trainer.initiate_model_trainer()
    #     except Exception as e:
    #         raise USvisaException(e, sys)

    # def start_model_evaluation(self, data_ingestion_artifact: DataIngestionArtifact,
    #                            model_trainer_artifact: ModelTrainerArtifact) -> ModelEvaluationArtifact:
    #     try:
    #         model_evaluation = ModelEvaluation(
    #             data_ingestion_artifact=data_ingestion_artifact,
    #             model_trainer_artifact=model_trainer_artifact,
    #             model_eval_config=self.model_evaluation_config
    #         )
    #         return model_evaluation.initiate_model_evaluation()
    #     except Exception as e:
    #         raise USvisaException(e, sys)

    # def start_model_pusher(self, model_evaluation_artifact: ModelEvaluationArtifact) -> ModelPusherArtifact:
    #     try:
    #         model_pusher = ModelPusher(
    #             model_evaluation_artifact=model_evaluation_artifact,
    #             model_pusher_config=self.model_pusher_config
    #         )
    #         return model_pusher.initiate_model_pusher()
    #     except Exception as e:
    #         raise USvisaException(e, sys)

    def run_pipeline(self) -> None:
        """
        Run complete pipeline
        Currently → only data ingestion + validation are active
        """
        try:
            # Data Ingestion
            data_ingestion_artifact = self.start_data_ingestion()
            logging.info(f"Data Ingestion Artifact: {data_ingestion_artifact}")

            # Data Validation
            data_validation_artifact = self.start_data_validation(data_ingestion_artifact)
            logging.info(f"Data Validation Artifact: {data_validation_artifact}")

            # Future steps (commented)
            # data_transformation_artifact = self.start_data_transformation(
            #     data_ingestion_artifact, data_validation_artifact
            # )
            # model_trainer_artifact = self.start_model_trainer(data_transformation_artifact)
            # model_evaluation_artifact = self.start_model_evaluation(
            #     data_ingestion_artifact, model_trainer_artifact
            # )
            # model_pusher_artifact = self.start_model_pusher(model_evaluation_artifact)

        except Exception as e:
            raise USvisaException(e, sys)
