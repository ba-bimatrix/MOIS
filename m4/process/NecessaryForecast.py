import warnings
import pandas as pd

from m4.common.SingletonInstance import SingletonInstance
from m4.ApplicationConfiguration import ApplicationConfiguration
from m4.process.algorithm.AbstractAlgorithm import AbstractAlgorithm
from m4.process.algorithm.ExponentialSmoothing import ExponentialSmoothing
from m4.process.algorithm.HoltWinters import HoltWinters
from m4.process.algorithm.SimpleRegression import SimpleRegression
from m4.process.algorithm.Arima import Arima
from m4.process.algorithm.SimpleLstm import SimpleLstm


class NecessaryForecast(SingletonInstance):
    _input_period = None
    _dimension = []
    _date_column = None
    _value_column = None
    _execute_date = None
    _forecast_dataset_columns = []
    _validateion_dataset_columns = []

    _algorithms = []
    _forecast_period = None
    _validation_period = None

    def __init__(self):
        """
        생성자
        """
        config = ApplicationConfiguration.instance()

        self._execute_date = config.parameter("EXECUTE_DATE")
        self._input_period = config.parameter("FORECAST_INPUT_PERIOD")
        self._dimension = config.parameter("FORECAST_DIMENSION")
        # self._date_column = config.parameter("FORECAST_DATE_COL")
        self._date_column = config.parameter("DATE_COL")
        # self._value_column = config.parameter("FORECAST_VALUE_COL")
        self._value_column = config.parameter("VALUE_COL")
        self._forecast_dataset_columns = self._dimension + ['STAT_CD', 'FCST_CNT']
        self._validateion_dataset_columns = self._dimension + ['STAT_CD', 'RMSE']

        self._algorithms = config.parameter("FORECAST_ALGORITHMS")
        self._forecast_period = config.parameter("FORECAST_PERIOD")
        self._validation_period = config.parameter("FORECAST_VALIDATION_PERIOD")

    def forecast(self, input_data: pd.DataFrame):
        """
        forecast
        :param : input_data: DataFrame
        :return: DataFrame
        """
        config = ApplicationConfiguration.instance()

        result, accuracy = self._forecast(ExponentialSmoothing.instance(), input_data, {"smoothing_level": config.parameter("SMOOTHING_LEVEL")})

        one_result, one_accu = self._forecast(HoltWinters.instance(), input_data, {"seasonal_periods": config.parameter("SEASONALITY_PERIOD")})
        result = pd.concat((result, one_result))
        accuracy = pd.concat((accuracy, one_accu))

        one_result, one_accu = self._forecast(SimpleRegression.instance(), input_data, {})
        result = pd.concat((result, one_result))
        accuracy = pd.concat((accuracy, one_accu))

        one_result, one_accu = self._forecast(Arima.instance(), input_data, {"arima_orders": config.parameter("ARIMA_ORDER")})
        result = pd.concat((result, one_result))
        accuracy = pd.concat((accuracy, one_accu))

        params = {"dimension": self._dimension,
                  "value_column": self._value_column,
                  "train_size": config.parameter("LSTM_TRAIN_SIZE"),
                  "window_input": config.parameter("LSTM_WINDOW_INPUT"),
                  "drop_rate": config.parameter("LSTM_DROP_RATE"),
                  "learning_rate": config.parameter("LSTM_LEARNING_RATE"),
                  "epochs": config.parameter("LSTM_EPOCHS")
                  }

        one_result, one_accu = self._forecast_lstm(SimpleLstm.instance(), input_data, params)
        result = pd.concat((result, one_result))
        accuracy = pd.concat((accuracy, one_accu))

        return {"result": result, "accuracy": accuracy}

    def _forecast(self, algorithm: AbstractAlgorithm, input_data: pd.DataFrame, params: dict):
        """
        1. 빈 데이터프레임 생성 (예측 결과/신뢰도 결과)
        2. 집계 기준별 For 루프
        3. 모델 예측 실행        (결과: 예측치, 신뢰도)
        4. 집계 기준 분리하여 예측 결과와 함께 빈 데이터프레임에 추가
        """
        fcst_dataset = pd.DataFrame(columns=self._forecast_dataset_columns)
        accu_dataset = pd.DataFrame(columns=self._validateion_dataset_columns)

        if algorithm.get_name() not in self._algorithms:
            return fcst_dataset, accu_dataset

        warnings.filterwarnings('ignore')

        for dim_values, splitted_data in input_data.groupby(self._dimension):
            y_hat, valid = algorithm.forecast(splitted_data[self._value_column][-self._input_period:], self._forecast_period, self._validation_period,
                                              params)

            if len(y_hat) == 0:
                continue
            dim_data = list(dim_values) if type(dim_values) is tuple else [dim_values]
            pred_data = list(map(lambda x: dim_data + [algorithm.get_name(), x], y_hat))
            one_fcst = pd.DataFrame(columns=self._forecast_dataset_columns, data=pred_data)
            one_accu = pd.DataFrame(columns=self._validateion_dataset_columns, data=[dim_data + [algorithm.get_name()] + valid])

            fcst_dataset = pd.concat((fcst_dataset, one_fcst))
            accu_dataset = pd.concat((accu_dataset, one_accu))

        fcst_dataset.reset_index(inplace=True)

        warnings.filterwarnings('default')

        return fcst_dataset, accu_dataset

    def _forecast_lstm(self, algorithm: AbstractAlgorithm, input_data: pd.DataFrame, params: dict):
        """
        1. 빈 데이터프레임 생성 (예측 결과/신뢰도 결과)
        2. 집계 기준별 For 루프
        3. 모델 예측 실행        (결과: 예측치, 신뢰도)
        4. 집계 기준 분리하여 예측 결과와 함께 빈 데이터프레임에 추가
        """

        fcst_dataset = pd.DataFrame(columns=self._forecast_dataset_columns)
        accu_dataset = pd.DataFrame(columns=self._validateion_dataset_columns)

        if algorithm.get_name() not in self._algorithms:
            return fcst_dataset, accu_dataset

        fcst_dataset, accu_dataset = algorithm.forecast(input_data, self._forecast_period, self._validation_period, params)

        return fcst_dataset, accu_dataset
