import pandas as pd
# from sklearn.cluster import DBSCAN
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score

from m4.common.SingletonInstance import SingletonInstance

import warnings
warnings.filterwarnings('ignore')


class ClusterAlgorithm(SingletonInstance):

    def clustering(self, data: pd.DataFrame, params: dict):
        """
        Data Source에 대한 CUD를 실행
        :param data: input_data
        :param params: Algorithm parameters
        :return: cluster
        """
        try:
            min_n_clusters = params.get('MIN_N_CLUSTERS')
            max_n_clusters = params.get('MAX_N_CLUSTERS')
        except:
            min_n_clusters = 2
            max_n_clusters = 10

        if min_n_clusters > max_n_clusters:
            min_n_clusters = max_n_clusters

        optimal_score = -2
        optimal_n = min_n_clusters
        for n_cluster in range(min_n_clusters, max_n_clusters, 1):
            model = KMeans(n_clusters=n_cluster)
            _result_cluster = model.fit_predict(data)
            _silhouette_score = silhouette_score(data, _result_cluster)

            if _silhouette_score > optimal_score:
                optimal_score = _silhouette_score
                optimal_n = n_cluster

        if optimal_score == -2:
            raise Exception("The silhouette score is -2. The range of score must be -1 to 1. It is a result that cannot be calculated")

        model = KMeans(n_clusters=optimal_n)
        print(f"optimal number of cluster is {optimal_n}")

        return model.fit_predict(data)


if __name__ == '__main__':
    print("Cluster test start")

    data = pd.read_csv('..\..\..\data\data_source\input_cluster_data.csv', encoding='cp949')
    data.set_index(keys=['ORGAN_CODE', 'TOP_ORGAN', 'FULL_ORGAN', 'LWST_ORGAN'], inplace=True)
    params = {
        'MIN_N_CLUSTERS': 2,
        'MAX_N_CLUSTERS': 10
    }
    cluster = ClusterAlgorithm()
    print(cluster.clustering(data, params))
    print("Cluster test success")