import pandas as pd
import statsmodels.api as sm

from m4.common.SingletonInstance import SingletonInstance
from m4.process.algorithm.AbstractAlgorithm import AbstractAlgorithm


class SimpleRegression(AbstractAlgorithm, SingletonInstance):

    def get_name(self):
        """
        Algorithm 이름(코드) 반환
        :return: String
        """
        return "REG"

    def forecast(self, data: pd.Series, forecast_period: int, validation_period: int, params: dict):
        """
        Data Source에 대한 CUD를 실행
        :param data: input_data
        :param forecast_period: forecast period
        :param validation_period: validation period
        :param params: Algorithm parameters
        :return: forecast, accuracy
        """
        df = pd.DataFrame({"val": data, "intercept": 1, "index": range(0, len(data))})

        model = sm.OLS(df["val"], df[["intercept", "index"]])
        model_fit = model.fit()
        fittedvalues = model_fit.predict(df[["intercept", "index"]])

        fdf = pd.DataFrame({"intercept": 1, "index": range(len(data), len(data)+forecast_period)})
        y_pred = model_fit.predict(fdf)

        return y_pred, self._validation(data[-validation_period:], fittedvalues[-validation_period:])
