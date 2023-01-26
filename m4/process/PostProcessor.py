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
        self._execute_date = config.parameter("EXECUTE_DATE")
        # self._date_column = config.parameter("FORECAST_DATE_COL")
        self._date_column = config.parameter("DATE_COL")

    def process(self, dataset: Dataset):
        """
        pre-processing
        :param :
        :return: Dataset
        """

        return dataset
