from pandas import DataFrame
from statsmodels.api import OLS

from m4.common.SingletonInstance import SingletonInstance
from m4.ApplicationConfiguration import ApplicationConfiguration


class StockingCalculation(SingletonInstance):

    _execute_date = None

    def __init__(self):
        """
        생성자
        """
        config = ApplicationConfiguration.instance()
        self._execute_date = config.parameter("EXECUTE_DATE")

    def calculation(self, input_data: DataFrame):
        """
        forecast
        :param : input_data: DataFrame
        :return: DataFrame
        """

        return None
