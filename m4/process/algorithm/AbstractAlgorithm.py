from abc import *
import numpy as np


class AbstractAlgorithm(metaclass=ABCMeta):
    """
    Algorithm 추상 클래스
    """

    @classmethod
    def _mape(cls, y_test, y_pred):
        """MAPE 산출 함수
        실측이 0인 경우는 제외하고 평균
        모든 실측이 0인 경우 nan 결과
        """
        y_test, y_diff = np.array(y_test), np.abs(np.array(y_test - y_pred))
        mape = np.divide(y_diff, y_test, out=np.zeros(y_diff.shape, dtype=float), where=y_test != 0)
        return np.nanmean(mape)

    @classmethod
    def _rmse(cls, y_test, y_pred):
        """RMSE 산출 함수"""
        y_test, y_pred = np.array(y_test), np.array(y_pred)
        return np.sqrt(np.mean((y_test - y_pred) ** 2))

    @classmethod
    def _validation(cls, y_test, y_pred):
        """RMSE 및 MAPE 산출 함수 """
        return [cls._rmse(y_test, y_pred)]

    @abstractmethod
    def get_name(self):
        """
        Algorithm 이름(코드) 반환
        :return: String
        """

    @abstractmethod
    def forecast(self, data: object, forecast_period: int, validation_period: int, params: dict):
        """
        Data Source에 대한 CUD를 실행
        :param data: input_data
        :param forecast_period: forecast period
        :param validation_period: validation period
        :param params: Algorithm parameters
        :return: forecast, accuracy
        """
