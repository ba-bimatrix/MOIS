import pandas as pd

from m4.common.SingletonInstance import SingletonInstance
from m4.ApplicationConfiguration import ApplicationConfiguration

from surprise import Reader, Dataset
from surprise import SVD, KNNWithZScore, KNNWithMeans, KNNBaseline, NMF, SlopeOne, CoClustering
from surprise.model_selection import cross_validate


class ResourceRecommender(SingletonInstance):

    _execute_date: object = None
    _reco_dimension: list = None
    _value_col: object = None
    _clust_nm: object = None

    def __init__(self):
        """
        생성자
        """
        config = ApplicationConfiguration.instance()

        self._reco_dimension = config.parameter("RECO_DIMENSION")
        self._value_col = config.parameter("VALUE_COL")
        self._clust_nm = config.parameter("CLUSTER_COL")
        self._execute_date = config.parameter("EXECUTE_DATE")

    def recommend(self, resource_data: pd.DataFrame) -> pd.DataFrame:
        """Recommend item use surprise package(rating concept)
        :param : resource_data: DataFrame (3 Kind of columns needed - user, item, ratings)
        :return: DataFrame
        """
        reader = Reader()
        rating_data = Dataset.load_from_df(resource_data[self._reco_dimension], reader)

        models = {"SVD": SVD(), "KNNWithZScore": KNNWithZScore(), "KNNWithMeans": KNNWithMeans(),
                  "KNNBaseline": KNNBaseline(), "NMF": NMF(), "SlopeOne": SlopeOne(), "CoClustering": CoClustering()}

        model_accu = {}
        for name, model in models.items():
            accu = cross_validate(model, rating_data, measures=["RMSE", "MAE"], cv=5, verbose=False)
            model_accu[name] = accu

        best_model = min(model_accu, key=lambda x: model_accu[x]["test_rmse"].mean())
        model = models[best_model]

        result_df = pd.DataFrame(columns=[self._clust_nm] + self._reco_dimension)
        for cluster, resources in resource_data.groupby(self._clust_nm):
            resource = Dataset.load_from_df(resources[self._reco_dimension], reader)
            cross_validate(model, resource, measures=["RMSE", "MAE"], cv=5, verbose=False)

            result = []
            for index, row in resources[self._reco_dimension].iterrows():
                result.append((row[0], row[1], row[2]))
            result = model.test(result)

            for row in result:
                row_df = pd.DataFrame(data=[[row[0], row[1], row[2]]], columns=self._reco_dimension)
                row_df[self._clust_nm] = cluster
                result_df = pd.concat([result_df, row_df])

        return result_df


if __name__ == '__main__':
    print("Recommend test start")
    import pandas as pd

    test_params = {
        "RECO_DIMENSION": ["ORGAN_CODE", "SUPPLIES_CODE", "VAL"],
        "CLUSTER_COL": "CLUSTER",
        "VALUE_COL": "VAL",
        "EXECUTE_DATE": "202205"
    }

    config: ApplicationConfiguration = ApplicationConfiguration.instance()
    config.init('m4.properties', test_params)

    test_data = pd.read_csv('..\..\data\data_source\input_data.csv', encoding='cp949')
    test_data[test_params["CLUSTER_COL"]] = "TEST"

    recommender = ResourceRecommender()
    print()
    print("The result of recommend is")
    print(recommender.recommend(test_data))
    print("Recommend test success")
