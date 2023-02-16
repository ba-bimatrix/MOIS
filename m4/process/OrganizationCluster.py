import pandas as pd
from pandas import DataFrame
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score

from m4.ApplicationConfiguration import ApplicationConfiguration
from m4.common.SingletonInstance import SingletonInstance
from m4.util.LogHandler import LogHandler


class OrganizationCluster(SingletonInstance):

    _region_column: list = None
    _organ_col: str = None
    _clust_nm: str = None
    _min_n_clusters: int = None
    _max_n_clusters: int = None
    _execute_date: str = None
    _logger: LogHandler = None
    _clust_index: list = None
    _clust_col: list = None

    def __init__(self):
        """
        생성자
        """
        config = ApplicationConfiguration.instance()

        self._region_column = config.parameter("REGION_COL")
        self._organ_col = config.parameter("ORGAN_COL")
        self._clust_nm = config.parameter("CLUSTER_COL")
        self._min_n_clusters = config.parameter("MIN_N_CLUSTERS")
        self._max_n_clusters = config.parameter("MAX_N_CLUSTERS")
        self._execute_date = config.parameter("EXECUTE_DATE")
        self._logger = LogHandler.instance().get_logger()
        self._clust_index = [self._organ_col] + self._region_column
        self._clust_col = [self._organ_col, self._clust_nm]

    def cluster(self, organization_data: DataFrame) -> DataFrame:
        """Clustering Organization feature data & add region info
        :param : organization_data: DataFrame
        :param : region_info: dict
        :return: DataFrame
        """
        result = organization_data.copy()
        result.set_index(keys=self._clust_index, inplace=True)
        result[self._clust_nm] = self._clustering(result, {"MIN_N_CLUSTERS": self._min_n_clusters,
                                                           "MAX_N_CLUSTERS": self._max_n_clusters})
        result[self._clust_nm] = result[self._clust_nm].apply(str)
        result.reset_index(inplace=True)

        result[self._clust_nm] = result[self._region_column + [self._clust_nm]].apply(lambda x: '_'.join(x), axis=1)

        return result[self._clust_col]

    def _clustering(self, data: pd.DataFrame, params: dict) -> list:
        """Clustering organization by feature data
        :param : data: input_data(feature data, pk: object, feature: only int/float)
        :param : params: Algorithm parameters(min/max cluster number)
        :return: result: Clustering result
        """
        try:
            min_n_clusters = params.get('MIN_N_CLUSTERS')
            max_n_clusters = params.get('MAX_N_CLUSTERS')
        except Warning:
            min_n_clusters = 2
            max_n_clusters = 10

        self._logger.info(f'defined min/max cluster number is {min_n_clusters} to {max_n_clusters}')

        if min_n_clusters > max_n_clusters:
            min_n_clusters = max_n_clusters
            self._logger.info('min cluster number was changed, cause it is over the max number')

        optimal_score = -2
        optimal_n = min_n_clusters
        self._logger.info('Process 1: Searching the optimal number of clusters')
        for n_cluster in range(min_n_clusters, max_n_clusters, 1):
            model = KMeans(n_clusters=n_cluster)
            _result_cluster = model.fit_predict(data)
            _silhouette_score = silhouette_score(data, _result_cluster)

            if _silhouette_score > optimal_score:
                optimal_score = _silhouette_score
                optimal_n = n_cluster

        if optimal_score == -2:
            raise Exception(
                "The silhouette score is -2. The range of score must be -1 to 1.\
                 It is a result that cannot be calculated")

        model = KMeans(n_clusters=optimal_n)
        self._logger.info(f'The optimal number of clusters is {optimal_n}')

        return model.fit_predict(data)
