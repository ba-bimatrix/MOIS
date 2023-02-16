import pandas as pd

from m4.common.SingletonInstance import SingletonInstance
from m4.ApplicationConfiguration import ApplicationConfiguration
from m4.process.algorithm.SimpleRegression import SimpleRegression
from m4.process.algorithm.AbstractAlgorithm import AbstractAlgorithm

class StockingCalculation(SingletonInstance):

    config: ApplicationConfiguration = None
    _execute_date: str = None
    _organ_col: str = None
    _mater_col: str = None
    _value_col: str = None
    _ware_col: str = None
    _stock_col: list = None

    def __init__(self):
        """
        생성자
        """
        config = ApplicationConfiguration.instance()
        self._execute_date = config.parameter("EXECUTE_DATE")
        self._organ_col = config.parameter("ORGAN_COL")
        self._mater_col = config.parameter("MATER_COL")
        self._value_col = config.parameter("VALUE_COL")
        self._ware_col = config.parameter("WAREHOUSE_COL")
        self._stock_col = [self._organ_col, self._mater_col, self._ware_col]

    def calculation(self, resource_data: pd.DataFrame):
        """
        forecast
        :param : input_data: DataFrame
        :return: DataFrame
        """
        fcst_dataset = pd.DataFrame(columns=self._stock_col + ['FCST_CNT'])
        for dim_values, splitted_data in resource_data.groupby(self._stock_col):
            y_hat, valid = SimpleRegression.instance().forecast(splitted_data.reset_index()[self._value_col], 1, 1, {})

            if len(y_hat) == 0:
                continue

            dim_data = list(dim_values) if type(dim_values) is tuple else [dim_values]
            pred_data = list(map(lambda x: dim_data + [x], y_hat))
            one_fcst = pd.DataFrame(columns=self._stock_col + ['FCST_CNT'], data=pred_data)
            fcst_dataset = pd.concat((fcst_dataset, one_fcst))

        fcst_dataset.reset_index(inplace=True)

        return fcst_dataset
