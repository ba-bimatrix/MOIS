import pandas as pd

from m4.common.SingletonInstance import SingletonInstance
from m4.ApplicationConfiguration import ApplicationConfiguration
from m4.util.LogHandler import LogHandler

from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score

from surprise import Reader, Dataset
from surprise import SVD, KNNWithZScore, KNNWithMeans, KNNBaseline, NMF, SlopeOne, CoClustering
from surprise.model_selection import cross_validate


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

        self._user_col = config.parameter("USER_COL")
        self._item_col = config.parameter("ITEM_COL")
        self._val_col = config.parameter("USER_VAL")
        self._clust_col = 'ORG_GROUP_ID'
        self._recommend_col = [self._user_col, self._item_col, self._val_col]

        self._min_n_clusters = config.parameter("MIN_N_CLUSTERS")

        self._execute_date = config.parameter("EXECUTE_DATE")
        self._logger = LogHandler.instance().get_logger()

    def recommend(self, user_data: dict) -> pd.DataFrame:
        dataframe = user_data['for_recommend'].copy()
        cluster = user_data['for_clustering'].copy()

        cluster[self._clust_col] = self._clustering(cluster)
        cluster[self._clust_col] = cluster[self._clust_col].apply(lambda x: str(int(x)))
        dataframe = pd.merge(dataframe, cluster, how='left', on=self._user_col)

        return self._recommender(dataframe)

    def _clustering(self, user_data: pd.DataFrame) -> pd.DataFrame:
        optimal_score = -2
        optimal_n = self._min_n_clusters
        _max_n_cluster = len(user_data.index)
        self._logger.info('Process 1: Searching the optimal number of clusters')
        for n_cluster in range(self._min_n_clusters, _max_n_cluster, 1):
            model = KMeans(n_clusters=n_cluster)
            _result_cluster = model.fit_predict(user_data)
            _silhouette_score = silhouette_score(user_data, _result_cluster)

            if _silhouette_score > optimal_score:
                optimal_score = _silhouette_score
                optimal_n = n_cluster

        if optimal_score == -2:
            raise Exception(
                "The silhouette score is -2. The range of score must be -1 to 1.\
                 It is a result that cannot be calculated")

        model = KMeans(n_clusters=optimal_n)
        self._logger.info(f'The optimal number of clusters is {optimal_n}')

        return model.fit_predict(user_data)

    def _recommender(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        reader = Reader()
        rating_data = Dataset.load_from_df(dataframe[self._recommend_col], reader)

        models = {"SVD": SVD(), "KNNWithZScore": KNNWithZScore(), "KNNWithMeans": KNNWithMeans(),
                  "KNNBaseline": KNNBaseline(), "NMF": NMF(), "SlopeOne": SlopeOne(), "CoClustering": CoClustering()}

        model_accu = {}
        for name, model in models.items():
            accu = cross_validate(model, rating_data, measures=["RMSE", "MAE"], cv=5, verbose=False)
            model_accu[name] = accu

        best_model = min(model_accu, key=lambda x: model_accu[x]["test_rmse"].mean())
        model = models[best_model]

        result_df = pd.DataFrame(columns=self._recommend_col)
        for cluster, resources in dataframe.groupby(self._clust_col):
            resource = Dataset.load_from_df(resources[self._recommend_col], reader).build_full_trainset()
            model.fit(resource)

            result = []
            for index, row in resources[self._recommend_col].iterrows():
                result.append((row[0], row[1], row[2]))
            result = model.test(result)

            for row in result:
                row_df = pd.DataFrame(data=[[row[0], row[1], row[2]]], columns=self._recommend_col)
                row_df[self._clust_col] = cluster
                result_df = pd.concat([result_df, row_df])

        return result_df
