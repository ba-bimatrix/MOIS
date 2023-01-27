from pandas import DataFrame

from m4.common.SingletonInstance import SingletonInstance
from m4.ApplicationConfiguration import ApplicationConfiguration
from m4.process.algorithm.ClusterAlgorithm import ClusterAlgorithm


class OrganizationCluster(SingletonInstance):

    _region_column: str = None
    _clust_pk: str = None
    _clust_nm: str = None
    _min_n_clusters: int = None
    _max_n_clusters: int = None
    _execute_date: str = None

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

    def cluster(self, organization_data: DataFrame) -> DataFrame:
        """Clustering Organization feature data & add region info
        :param : organization_data: DataFrame
        :param : region_info: dict
        :return: DataFrame
        """
        result = organization_data.copy()
        result.set_index(keys=self._clust_pk + [self._region_column], inplace=True)
        cluster = ClusterAlgorithm()
        result[self._clust_nm] = cluster.clustering(result, {"MIN_N_CLUSTERS": self._min_n_clusters,
                                                             "MAX_N_CLUSTERS": self._max_n_clusters})
        result.reset_index(inplace=True)

        # temporary option
        if self._region_column not in organization_data.columns:
            # default setting
            region_info = {
                'Sejong': 'CCA',  # '충청권',
                'Seoul': 'MPA',  # '수도권',
                'Busan': 'YNA',  # '영남권',
                'Daegu': 'YNA',  # '영남권',
                'Incheon': 'MPA',  # '수도권',
                'Gwangju': 'HNA',  # '호남권',
                'Daejeon': 'CCA',  # '충청권',
                'Ulsan': 'YNA',  # '영남권',
                'Jeju-do': 'JJA',  # '제주권',
                'Gyeonggi-do': 'MPA',  # '수도권',
                'Gangwon-do': 'GWA',  # '강원권',
                'Chungcheongbuk-do': 'CCA',  # '충청권',
                'Chungcheongnam-do': 'CCA',  # '충청권',
                'Jeollabuk-do': 'HNA',  # '호남권',
                'Jeollanam-do': 'HNA',  # '호남권',
                'Gyeongsangbuk-do': 'YNA',  # '영남권',
                'Gyeongsangnam-do': 'YNA',  # '영남권'
            }
            for region in region_info:
                result.loc[result["TOP_ORGAN"] == region, self._region_column] = region_info[region]

        result[self._clust_nm] = result[[self._region_column, self._clust_nm]].apply(lambda x: x[0] + '_' + str(x[1]),
                                                                                     axis=1)

        return result[self._clust_pk + [self._clust_nm]]


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
