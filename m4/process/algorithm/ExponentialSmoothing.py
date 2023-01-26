import pandas as pd
from statsmodels.tsa.holtwinters import SimpleExpSmoothing

from m4.common.SingletonInstance import SingletonInstance
from m4.process.algorithm.AbstractAlgorithm import AbstractAlgorithm


class ExponentialSmoothing(AbstractAlgorithm, SingletonInstance):

    def get_name(self):
        """
        Algorithm 이름(코드) 반환
        :return: String
        """
        return "EMV"

    def forecast(self, data: pd.Series, forecast_period: int, validation_period: int, params: dict):
        """
        Data Source에 대한 CUD를 실행
        :param data: input_data
        :param forecast_period: forecast period
        :param validation_period: validation period
        :param params: Algorithm parameters
        :return: forecast, accuracy
        """
        model = SimpleExpSmoothing(data, initialization_method='estimated')
        model_fit = model.fit(smoothing_level=params["smoothing_level"])

        return model_fit.forecast(forecast_period), \
            self._validation(data[-validation_period:], model_fit.fittedvalues[-validation_period:])
