import pandas as pd
from pandas import DataFrame
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score

from m4.ApplicationConfiguration import ApplicationConfiguration
from m4.common.SingletonInstance import SingletonInstance
from m4.util.LogHandler import LogHandler


class OrganizationCluster(SingletonInstance):

    _region_column: str = None
    _clust_pk: str = None
    _clust_nm: str = None
    _min_n_clusters: int = None
    _max_n_clusters: int = None
    _execute_date: str = None
    _logger: LogHandler.get_logger = None

    def __init__(self):
        """
        생성자
        """
        config = ApplicationConfiguration.instance()

        self._region_column = config.parameter("REGION_COL")
        self._clust_pk = config.parameter("CLUSTER_PK")
        self._clust_nm = config.parameter("CLUSTER_COL")
        self._min_n_clusters = config.parameter("MIN_N_CLUSTERS")
        self._max_n_clusters = config.parameter("MAX_N_CLUSTERS")
        self._execute_date = config.parameter("EXECUTE_DATE")
        self._logger = LogHandler.instance().get_logger()

    def cluster(self, organization_data: DataFrame) -> DataFrame:
        """Clustering Organization feature data & add region info
        :param : organization_data: DataFrame
        :param : region_info: dict
        :return: DataFrame
        """
        result = organization_data.copy()
        result.set_index(keys=self._clust_pk + self._region_column, inplace=True)
        result[self._clust_nm] = self._clustering(result, {"MIN_N_CLUSTERS": self._min_n_clusters,
                                                           "MAX_N_CLUSTERS": self._max_n_clusters})
        result[self._clust_nm] = result[self._clust_nm].apply(str)
        result.reset_index(inplace=True)

        result[self._clust_nm] = result[self._region_column + [self._clust_nm]].apply(lambda x: '_'.join(x), axis=1)

        return result[self._clust_pk + [self._clust_nm]]

    def _clustering(self, data: pd.DataFrame, params: dict) -> list:
        """Clustering organization by feature data
        :param : data: input_data(feature data, pk: object, feature: only int/float)
        :param : params: Algorithm parameters(min/max cluster number)
        :return: result: Clustering result
        """
        try:
            min_n_clusters = params.get('MIN_N_CLUSTERS')
            max_n_clusters = params.get('MAX_N_CLUSTERS')
        except:
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


if __name__ == '__main__':
    print("Cluster test start")
    import pandas as pd

    test_params = {
        "REGION_COL": "REGION",
        "CLUSTER_PK": ["ORGAN_CODE", "TOP_CODE", "TOP_ORGAN", "FULL_ORGAN", "LWST_ORGAN"],
        "CLUSTER_COL": "CLUSTER",

        "MIN_N_CLUSTERS": 2,
        "MAX_N_CLUSTERS": 10,

        "EXECUTE_DATE": "202205"
    }

    config: ApplicationConfiguration = ApplicationConfiguration.instance()
    config.init('m4.properties', test_params)

    test_data = pd.read_csv('..\..\data\data_source\clust_data.csv', encoding='cp949')

    test_cluster = OrganizationCluster()
    result_cluster = test_cluster.cluster(test_data)
    print("test cluster dataframe output is")
    print(result_cluster)
    print()
    print(f"The length of unique cluster number is {len(result_cluster['CLUSTER'].unique())}")
    print("Cluster test success")
