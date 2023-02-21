import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from numpy import log1p
from sklearn.preprocessing import StandardScaler

from m4.ApplicationConfiguration import ApplicationConfiguration
from m4.common.SingletonInstance import SingletonInstance
from m4.process.Dataset import Dataset


class PreProcessor(SingletonInstance):

    _organ_col: str = None
    _mater_col: str = None
    _val_col: str = None
    _clust_col: str = None
    _region_col: list = None
    _warehouse_col: str = None
    _input_period: int = None
    _forecast_date: str = None
    _resource_date: str = None
    _execute_date: str = None
    _forecast_columns: list = None
    _forecast_groupby_key: list = None
    _forecast_unique_key: list = None
    _forecast_merge_key: list = None
    _recommend_groupby_key: list = None
    _stock_groupby_key: list = None

    def __init__(self):
        """
        생성자
        """
        config = ApplicationConfiguration.instance()

        self._organ_col = config.parameter("ORGAN_COL")
        self._mater_col = config.parameter("MATER_COL")
        self._val_col = config.parameter("VALUE_COL")
        self._clust_col = config.parameter("CLUSTER_COL")
        self._region_col = config.parameter("REGION_COL")
        self._warehouse_col = config.parameter("WAREHOUSE_COL")

        self._user_col = config.parameter("USER_COL")
        self._item_col = config.parameter("ITEM_COL")
        self._user_dimension = [self._user_col, self._item_col]
        self._user_val_col = config.parameter("USER_VAL")

        self._input_period = config.parameter("FORECAST_INPUT_PERIOD")
        self._forecast_date = config.parameter("FORECAST_DATE")
        self._resource_date = config.parameter("RESOURCE_DATE")
        self._execute_date = config.parameter("EXECUTE_DATE")

        self._forecast_columns = [self._clust_col, self._mater_col, config.parameter("FORECAST_DATE"), self._val_col]
        self._forecast_groupby_key = [self._clust_col, self._mater_col, self._forecast_date]
        self._forecast_unique_key = [self._clust_col, self._mater_col]
        self._forecast_merge_key = [self._clust_col, self._mater_col, self._forecast_date]
        self._recommend_groupby_key = [self._organ_col, self._clust_col, self._mater_col]
        self._stock_groupby_key = [self._organ_col, self._mater_col, self._warehouse_col, self._resource_date]

    # TODO: 범주형 자료 원핫 인코딩 및 리팩토링 필요
    def process_cluster(self, dataset: Dataset) -> Dataset:
        """pre-processing for cluster
        :param dataset:
        :return:
        """
        dataframe = self._aggregate_cluster(dataset.organization_data)
        scaled_col = ['POPUL_CNT', 'HOHOLD_CNT', 'RESI_CNT', 'BUGE_AMT', 'FULL_SQUARE',
                       'ROAD_SQUARE', 'CULT_SQUARE', 'FORE_SQUARE', 'RIVER_SQUARE', 'PBORD_AMT',
                       'TRSPT_AMT', 'MNCP_AMT', 'LCPB_AMT', 'GVN_MNG_SQUARE', 'GVN_SPRT_SQUARE',
                       'ETC_SQUARE', 'DMG_STORM_FLOOD_AMT', 'COAST_LEN']
        non_scaled_dataframe = dataframe[dataframe.columns.difference(scaled_col)]
        dataframe[scaled_col] = log1p(dataframe[scaled_col])
        dataframe = pd.concat([non_scaled_dataframe,
                               pd.DataFrame(StandardScaler().fit_transform(dataframe[scaled_col]), columns=scaled_col)],
                              axis=1)

        dataset.pre_processing_organization_data = dataframe

        return dataset

    # TODO: 집계 방법에 대한 개선 및 리팩토링 필요
    def _aggregate_cluster(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        columns_map = {
            'average': ['POPUL_CNT', 'HOHOLD_CNT', 'RESI_CNT', 'BUGE_AMT', 'FULL_SQUARE',
                        'ROAD_SQUARE', 'CULT_SQUARE', 'FORE_SQUARE', 'RIVER_SQUARE', 'PBORD_AMT',
                        'TRSPT_AMT', 'MNCP_AMT', 'LCPB_AMT', 'GVN_MNG_SQUARE', 'GVN_SPRT_SQUARE',
                        'ETC_SQUARE', 'DMG_STORM_FLOOD_AMT'],
            'get_last': ['REGION_CD', 'ADMDSTRC_CD', 'COAST_ADJ_AT', 'INLAND_CTY_AT', 'COAST_LEN']
        }

        return pd.concat(
            [dataframe.groupby(self._organ_col)[columns_map['average']].mean(),
             dataframe[dataframe['STDR_YY'] == max(dataframe['STDR_YY'])][columns_map['get_last'] + [self._organ_col]
                                                                          ].set_index(self._organ_col)],
            axis=1).reset_index()

    # TODO: 비군집 대상 추천 대안 필요
    def process_recommend(self, dataset: Dataset) -> Dataset:
        """pre-processing for recommend
        :param : dataset: Dataset
        :return: dataset: Dataset
        """
        dataframe = pd.merge(dataset.resource_data, dataset.clustering, how='left', on=self._organ_col)
        dataframe.loc[dataframe[self._clust_col].isnull(), self._clust_col] = 'NON'
        dataset.pre_processing_resource_data = dataframe.groupby(self._recommend_groupby_key,
                                                                 as_index=False)[self._val_col].mean()

        return dataset

    def process_forecast(self, dataset: Dataset) -> Dataset:
        """pre-processing for forecast
        :param : dataset: Dataset
        :return: dataset: Dataset
        """
        dataset = self._avg_by_cluster(dataset)
        dataset = self._fill_zero_and_date(dataset)

        return dataset

    def _avg_by_cluster(self, dataset: Dataset) -> Dataset:
        """Preprocessing before timeseries forecasting
        sum data by same cluster
        :param : dataset: Dataset
        :param : args: tuple
        :return: dataset: Dataset
        """
        dataframe = pd.merge(dataset.input_data, dataset.clustering, how='left', on=self._organ_col)
        dataframe.loc[dataframe[self._clust_col].isnull(), self._clust_col] = \
            dataframe.loc[dataframe[self._clust_col].isnull(), self._organ_col]
        dataframe = dataframe.groupby(self._forecast_groupby_key, as_index=False)[self._val_col].mean()

        dataset.pre_processing_input_data = dataframe

        return dataset

    def _fill_zero_and_date(self, dataset: Dataset) -> Dataset:
        """월별 실적과 함께 집계 컬럼, 시작/종료월을 넣을 시 결측월들을 0으로 채워줌.
        :param : dataset: Dataset 월별 실적
        :return: dataset: Dataset
        """

        if dataset.pre_processing_input_data is None:
            raise Exception("There is no data for filling to null months. plz check previous processing")

        dataframe: pd.DataFrame = dataset.pre_processing_input_data[self._forecast_columns]
        end_dt = datetime.strptime(self._execute_date + '01', '%Y%m%d').strftime('%Y.%m')
        start_dt = (pd.to_datetime(end_dt + '.01') + relativedelta(months=-(self._input_period - 1))).strftime('%Y.%m')

        date_range = pd.DataFrame({self._forecast_date: pd.date_range(start_dt, end_dt, freq='MS').strftime('%Y%m'),
                                   "INDEX": pd.date_range(start_dt, end_dt, freq='MS')})
        date_range['key'] = 'key'

        unique_dim = dataframe[self._forecast_unique_key].drop_duplicates()
        unique_dim['key'] = 'key'

        cp_result = pd.merge(date_range, unique_dim, on='key').drop('key', axis=1)
        result_df = pd.merge(cp_result, dataframe, how='left', on=self._forecast_merge_key).fillna(0)

        result_df = result_df.set_index("INDEX")
        dataset.pre_processing_input_data = result_df

        return dataset

    def process_stocking(self, dataset: Dataset) -> Dataset:
        dataset.pre_processing_resource_data = dataset.resource_data.groupby(self._stock_groupby_key,
                                                                             as_index=False)[self._val_col].sum()
        return dataset

    def process_user_recommend(self, dataset: Dataset) -> Dataset:
        dataframe = dataset.user_data.groupby(self._user_dimension, as_index=False)[self._user_val_col].count()
        pivot_user_log = dataframe.pivot(index=self._user_col, columns=self._item_col,
                                         values=self._user_val_col).fillna(0)

        dataset.pre_processing_user_data = {'for_recommend': dataframe, 'for_clustering': pivot_user_log}

        return dataset
