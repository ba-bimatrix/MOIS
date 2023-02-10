import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta

from m4.common.SingletonInstance import SingletonInstance
from m4.ApplicationConfiguration import ApplicationConfiguration
from m4.process.Dataset import Dataset


class PostProcessor(SingletonInstance):
    _input_period = None
    _columns = None
    _dimension = None
    _execute_date = None
    _date_column = None

    # TODO: 생성/수정 정보 DB에서 받아오게 변경 필요
    def __init__(self):
        """
        생성자
        """
        config = ApplicationConfiguration.instance()
        self._clust_col = config.parameter("CLUSTER_COL")
        self._execute_date = config.parameter("EXECUTE_DATE")
        self._date_column = config.parameter("DATE_COL")
        self._user_id = 'skgo'
        self._stdr_yy = datetime.now().year
        self._execute_date = config.parameter("EXECUTE_DATE")
        self._run_dt = datetime.now().strftime('%Y%m%d')
        self._rank_limit = config.parameter("RANKING_LIMIT")

    # TODO: 후에 리팩토링 필요
    def process(self, dataset: Dataset) -> Dataset:
        """Post-processing All
        :param : dataset: Dataset
        :return: Dataset
        """
        dataset.clustering = self._match_cluster_format(dataset)

        dataset = self._calcu_forecast_bestfit(dataset)
        dataset.forecast = self._calcu_forecast_date(dataset)
        dataset.forecast = self._calcu_forecast_ratio(dataset)
        dataset.forecast = self._share_forecast_cluster(dataset)
        dataset.forecast = self._match_forecast_format(dataset)

        dataset.recommend = self._calcu_recommend_rating(dataset)
        dataset.recommend = self._calcu_recommend_ranking(dataset, self._rank_limit)
        dataset.recommend = self._match_recommend_format(dataset)

        dataset.stocking_calculation = self._calcu_stocking_ratio(dataset)
        dataset.stocking_calculation = self._match_stocking_format(dataset)

        return dataset

    def _match_cluster_format(self, dataset: Dataset) -> pd.DataFrame:
        clustering = dataset.clustering.copy()

        clustering['STDR_YY'] = self._stdr_yy
        clustering['FORST_AT'] = 'Y'
        clustering['CRTR_ID'] = self._user_id
        clustering['LAST_MODUSR_ID'] = self._user_id
        clustering['CREAT_DT'] = self._run_dt
        clustering['LAST_MODF_DT'] = self._run_dt

        return clustering[['STDR_YY', 'ORG_CD', 'ORG_GROUP_ID', 'FORST_AT', 'CRTR_ID', 'LAST_MODUSR_ID', 'CREAT_DT',
                                'LAST_MODF_DT']]

    @staticmethod
    def _calcu_forecast_bestfit(dataset: Dataset) -> Dataset:
        forecast = dataset.forecast["result"]
        accuracy = dataset.forecast["accuracy"]

        accuracy['RANK'] = accuracy.groupby(['ORG_GROUP_ID', 'CMYN_RSCD'])['RMSE'].rank(method='dense', ascending=False)
        dataset.forecast = pd.merge(accuracy[accuracy['RANK'] == 1], forecast, how='left',
                                    on=['ORG_GROUP_ID', 'CMYN_RSCD', 'STAT_CD'])

        return dataset

    def _calcu_forecast_date(self, dataset: Dataset) -> pd.DataFrame:
        forecast = dataset.forecast.copy()

        end_dt = datetime.strptime(self._execute_date + '01', '%Y%m%d')
        _fdate = end_dt + relativedelta(months=1)
        forecast['STDR_MT'] = forecast['index'].apply(lambda x: (_fdate+relativedelta(months=x)).strftime('%m'))

        return forecast

    @staticmethod
    def _calcu_forecast_ratio(dataset: Dataset) -> pd.DataFrame:
        forecast = dataset.forecast.copy()
        forecast['FCST_CNT'] = forecast['FCST_CNT'].apply(lambda x: 0 if x < 0 else x)
        forecast = pd.merge(forecast, forecast.groupby(['ORG_GROUP_ID', 'CMYN_RSCD'])['FCST_CNT'].sum()\
                            .reset_index(name='SUM_FCST'), how='left', on=['ORG_GROUP_ID', 'CMYN_RSCD'])
        forecast['FORST_RATIO'] = forecast[['FCST_CNT', 'SUM_FCST']].apply(lambda x: 0 if x[1] == 0 else x[0]/x[1],
                                                                           axis=1)
        return forecast

    def _share_forecast_cluster(self, dataset: Dataset) -> pd.DataFrame:
        """share forecast value in same cluster
        :param : dataset: Dataset
        :return: dict(pd.DataFrame):
        """
        return pd.merge(dataset.forecast, dataset.clustering, how='left', on=self._clust_col)

    def _match_forecast_format(self, dataset: Dataset) -> pd.DataFrame:
        forecast = dataset.forecast.copy()

        forecast['STDR_YY'] = self._stdr_yy
        forecast['GROUP_FORST_NE_QTY'] = forecast['FCST_CNT']
        forecast['FORST_NE_QTY'] = forecast['FCST_CNT']
        forecast['CALT_RATIO'] = forecast['FORST_RATIO']

        forecast['FORST_AT'] = 'Y'
        forecast['CRTR_ID'] = self._user_id
        forecast['LAST_MODUSER_ID'] = self._user_id
        forecast['CREAT_DT'] = self._run_dt
        forecast['LAST_MODF_DT'] = self._run_dt

        forecast[['GROUP_FORST_NE_QTY', 'FORST_NE_QTY', 'FORST_RATIO', 'CALT_RATIO']] = \
            forecast[['GROUP_FORST_NE_QTY', 'FORST_NE_QTY', 'FORST_RATIO', 'CALT_RATIO']].round(5)

        return forecast[['STDR_YY', 'CMYN_RSCD', 'ORG_CD', 'STDR_MT', 'ORG_GROUP_ID', 'GROUP_FORST_NE_QTY',
                              'FORST_NE_QTY', 'FORST_RATIO', 'FORST_AT', 'CALT_RATIO', 'CRTR_ID', 'LAST_MODUSER_ID',
                              'CREAT_DT', 'LAST_MODF_DT']]

    def _calcu_recommend_rating(self, dataset: Dataset) -> pd.DataFrame:
        return dataset.recommend.groupby(['ORG_GROUP_ID', 'CMYN_RSCD'], as_index=False)['VAL'].sum()

    def _calcu_recommend_ranking(self, dataset: Dataset, param) -> pd.DataFrame:
        recommend = dataset.recommend.copy()
        recommend['FORST_RKING'] = recommend.groupby(['ORG_GROUP_ID', 'CMYN_RSCD'])['VAL'].rank(method='dense')
        recommend = recommend[recommend['FORST_RKING'] <= param]

        return recommend

    def _match_recommend_format(self, dataset: Dataset) -> pd.DataFrame:
        recommend = dataset.recommend.copy()

        recommend['STDR_YY'] = self._stdr_yy
        recommend['CALT_RKING'] = recommend['FORST_RKING']

        recommend['FORST_AT'] = 'Y'
        recommend['CRTR_ID'] = self._user_id
        recommend['LAST_MODUSR_ID'] = self._user_id
        recommend['CREAT_DT'] = self._run_dt
        recommend['LAST_MODF_DT'] = self._run_dt

        return recommend[['STDR_YY', 'ORG_GROUP_ID', 'CMYN_RSCD', 'FORST_RKING', 'FORST_AT', 'CALT_RKING', 'CRTR_ID',
                          'LAST_MODUSR_ID', 'CREAT_DT', 'LAST_MODF_DT']]

    def _calcu_stocking_ratio(self, dataset: Dataset) -> pd.DataFrame:
        return dataset.stocking_calculation

    def _match_stocking_format(self, dataset: Dataset) -> pd.DataFrame:
        return dataset.stocking_calculation