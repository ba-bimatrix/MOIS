import numpy as np
import pandas as pd
from statsmodels.tsa.arima_model import ARIMA

from m4.common.SingletonInstance import SingletonInstance
from m4.process.algorithm.AbstractAlgorithm import AbstractAlgorithm
from m4.util.LogHandler import LogHandler


class Arima(AbstractAlgorithm, SingletonInstance):
    _logger = None

    def __init__(self):
        """
        생성자
        """
        self._logger = LogHandler.instance().get_logger()

    def get_name(self):
        """
        Algorithm 이름(코드) 반환
        :return: String
        """
        return "ARM"

    def forecast(self, data: pd.Series, forecast_period: int, validation_period: int, params: dict):
        """
        Data Source에 대한 CUD를 실행
        :param data: input_data
        :param forecast_period: forecast period
        :param validation_period: validation period
        :param params: Algorithm parameters
        :return: forecast, accuracy
        """

        pred = []
        accu = []
        aic = float('inf')
        for order in params["arima_orders"]:
            try:
                model = ARIMA(data, order=order)
                model_fit = model.fit(disp=-1)
                y_pred = model_fit.forecast(forecast_period)

                if model_fit.aic < aic:
                    aic = model_fit.aic
                    pred = y_pred[0]
                    accu = self._validation(data[-validation_period:], model_fit.fittedvalues[-validation_period:])

            except (np.linalg.LinAlgError, ValueError, RuntimeError) as e:
                self._logger.debug(e)
                continue

        return pred, accu
