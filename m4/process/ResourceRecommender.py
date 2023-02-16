import pandas as pd

from m4.common.SingletonInstance import SingletonInstance
from m4.ApplicationConfiguration import ApplicationConfiguration

from surprise import Reader, Dataset
from surprise import SVD, KNNWithZScore, KNNWithMeans, KNNBaseline, NMF, SlopeOne, CoClustering
from surprise.model_selection import cross_validate


class ResourceRecommender(SingletonInstance):

    config: ApplicationConfiguration = None
    _clust_col: str = None
    _organ_col: str = None
    _mater_col: str = None
    _value_col: str = None
    _recommend_col: list = None
    _execute_date: str = None

    def __init__(self):
        """
        생성자
        """
        config = ApplicationConfiguration.instance()

        self._clust_col = config.parameter("CLUSTER_COL")
        self._organ_col = config.parameter("ORGAN_COL")
        self._mater_col = config.parameter("MATER_COL")
        self._value_col = config.parameter("VALUE_COL")
        self._recommend_col = [self._organ_col, self._mater_col, self._value_col]
        self._execute_date = config.parameter("EXECUTE_DATE")

    def recommend(self, resource_data: pd.DataFrame) -> pd.DataFrame:
        """Recommend item use surprise package(rating concept)
        :param : resource_data: DataFrame (3 Kind of columns needed - user, item, ratings)
        :return: DataFrame
        """
        reader = Reader()
        rating_data = Dataset.load_from_df(resource_data[self._recommend_col], reader)

        models = {"SVD": SVD(), "KNNWithZScore": KNNWithZScore(), "KNNWithMeans": KNNWithMeans(),
                  "KNNBaseline": KNNBaseline(), "NMF": NMF(), "SlopeOne": SlopeOne(), "CoClustering": CoClustering()}

        model_accu = {}
        for name, model in models.items():
            accu = cross_validate(model, rating_data, measures=["RMSE", "MAE"], cv=5, verbose=False)
            model_accu[name] = accu

        best_model = min(model_accu, key=lambda x: model_accu[x]["test_rmse"].mean())
        model = models[best_model]

        result_df = pd.DataFrame(columns=[self._clust_col] + self._recommend_col)
        for cluster, resources in resource_data.groupby(self._clust_col):
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
