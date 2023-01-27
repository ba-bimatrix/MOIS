import pandas as pd
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score

from m4.common.SingletonInstance import SingletonInstance
from m4.util.LogHandler import LogHandler

import warnings
warnings.filterwarnings('ignore')


class ClusterAlgorithm(SingletonInstance):
    _logger: LogHandler.get_logger = None
    min_n_clusters: int = None
    max_n_clusters: int = None

    def __init__(self):
        self._logger = LogHandler.instance().get_logger()

    def clustering(self, data: pd.DataFrame, params: dict) -> list:
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

    test_data = pd.read_csv("..\..\..\data\data_source\clust_data.csv", encoding='cp949')
    test_data.set_index(keys=['ORGAN_CODE', 'TOP_CODE', 'TOP_ORGAN', 'FULL_ORGAN', 'LWST_ORGAN', 'REGION'],
                        inplace=True)
    test_params = {
        'MIN_N_CLUSTERS': 2,
        'MAX_N_CLUSTERS': 10
    }
    cluster = ClusterAlgorithm()
    print(cluster.clustering(test_data, test_params))
    print("Cluster test success")
