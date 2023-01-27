import pandas as pd

from m4.common.SingletonInstance import SingletonInstance
from m4.ApplicationConfiguration import ApplicationConfiguration
from m4.process.Dataset import Dataset


class PostProcessor(SingletonInstance):
    _input_period = None
    _columns = None
    _dimension = None
    _execute_date = None
    _date_column = None

    def __init__(self):
        """
        생성자
        """
        config = ApplicationConfiguration.instance()
        self._dimension = config.parameter("FORECAST_DIMENSION")
        self._execute_date = config.parameter("EXECUTE_DATE")
        self._date_column = config.parameter("DATE_COL")

    def process(self, dataset: Dataset) -> Dataset:
        """pre-processing
        :param : dataset: Dataset
        :return: Dataset
        """
        dataset.forecast = self._share_forecast_in_cluster(dataset)

        return dataset

    def _share_forecast_in_cluster(self, dataset: Dataset) -> dict:
        """share forecast value in same cluster
        :param : dataset: Dataset
        :return: dict(pd.DataFrame):
        """
        forecast_df = dataset.forecast["result"]
        accu_df = dataset.forecast["accuracy"]

        forecast_df = pd.merge(forecast_df, dataset.clustering, how='left', on=self._dimension)
        accu_df = pd.merge(accu_df, dataset.clustering, how='left', on=self._dimension)

        return {"result": forecast_df, "accuracy": accu_df}
