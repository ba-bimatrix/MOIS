import pandas as pd

from m4.common.SingletonInstance import SingletonInstance
from m4.ApplicationConfiguration import ApplicationConfiguration

from surprise import Reader, Dataset
from surprise import SVD, KNNWithZScore, KNNWithMeans, KNNBaseline, NMF, SlopeOne, CoClustering
from surprise.model_selection import cross_validate

# TODO: 사용자 화면 추천 로직 개발 필요
class UserRecommender(SingletonInstance):

    _execute_date: object = None
    _reco_dimension: list = None
    _value_col: object = None
    _clust_col: object = None

    def __init__(self):
        """
        생성자
        """
        config = ApplicationConfiguration.instance()

        self._clust_col = config.parameter("CLUSTER_COL")
        self._organ_col = config.parameter("ORGAN_COL")
        self._mater_col = config.parameter("MATER_COL")
        self._value_col = config.parameter("VALUE_COL")
        self._reco_dimension = [self._organ_col, self._mater_col, self._value_col]
        self._clust_col = config.parameter("CLUSTER_COL")
        self._execute_date = config.parameter("EXECUTE_DATE")

    def cluster(self):
        return None

    def recommend(self, resource_data: pd.DataFrame) -> pd.DataFrame:
        return None
