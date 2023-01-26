import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta

from m4.ApplicationConfiguration import ApplicationConfiguration
from m4.common.SingletonInstance import SingletonInstance
from m4.process.Dataset import Dataset


class PreProcessor(SingletonInstance):

    _input_period = None
    _columns = None
    _dimension = None
    _execute_date = None
    _date_column = None
    _date_object_column = None

    def __init__(self):
        """
        생성자
        """
        config = ApplicationConfiguration.instance()
        self._organ_pk = config.parameter("ORGAN_PK")
        self._input_period = config.parameter("FORECAST_INPUT_PERIOD")
        self._columns = config.parameter("FORECAST_COLUMNS")
        self._dimension = config.parameter("FORECAST_DIMENSION")
        self._execute_date = config.parameter("EXECUTE_DATE")
        # self._date_column = config.parameter("FORECAST_DATE_COL") DATE_COL
        self._date_column = config.parameter("DATE_COL")
        # self._val_coulmn = config.parameter("FORECAST_VALUE_COL")
        self._val_coulmn = config.parameter("VALUE_COL")
        self._region_column = config.parameter("REGION_COL")
        self._clust_nm = config.parameter("CLUSTER_COL")

    def process_clust(self, dataset: Dataset):
        """pre-processing

        :param :
        :return: Dataset
        """
        dataset.pre_processing_organization_data = self._agg_organ_data(dataset)

        return dataset

    def _agg_organ_data(self, dataset) -> pd.DataFrame:
        """Aggregation organization feature data for clustering

        :param dataset:
        :return:
        """
        if dataset.organization_data is None:
            return None

        result_df = dataset.organization_data

        return result_df

    def process_reco(self, dataset: Dataset):
        """pre-processing

        :param :
        :return: Dataset
        """
        dataframe = pd.merge(dataset.input_data, dataset.clustering, how='left', on=self._organ_pk)

        dataset.pre_processing_resource_data = dataframe

        return dataset

    def process_fcst(self, dataset: Dataset):
        """pre-processing

        :param :
        :return: Dataset
        """
        dataset = self._sum_by_cluster(dataset, self._date_column)
        dataset = self._fill_zero_and_date(dataset)

        return dataset

    def _sum_by_cluster(self, dataset: Dataset, *args: tuple) -> Dataset:
        """Preprocessing before timeseries forecasting
        sum data by same cluster
        :param : dataset: Dataset
        :param : args: tuple
        :return: DataFrame
        """
        dataframe = pd.merge(dataset.input_data, dataset.clustering, how='left', on=self._organ_pk)
        result_df = dataframe.groupby([self._clust_nm] + list(args), as_index=False)[self._val_coulmn].sum()

        # dataset.input_data = result_df
        dataset.pre_processing_input_data = result_df

        return dataset

    def _fill_zero_and_date(self, dataset) -> Dataset:
        """월별 실적과 함께 집계 컬럼, 시작/종료월을 넣을 시 결측월들을 0으로 채워줌.

        :param dataframe: 월별 실적
        :param end_dt   : 종료 월(YYYY-MM)
        :return: None
        """

        if dataset.pre_processing_input_data is None:
            return None

        # dataframe: pd.DataFrame = dataset.input_data[self._columns]
        dataframe: pd.DataFrame = dataset.pre_processing_input_data[self._columns]
        end_dt = datetime.strptime(self._execute_date + '01', '%Y%m%d').strftime('%Y.%m')
        start_dt = (pd.to_datetime(end_dt + '.01') + relativedelta(months=-(self._input_period - 1))).strftime('%Y.%m')

        date_range = pd.DataFrame({self._date_column: pd.date_range(start_dt, end_dt, freq='MS').strftime('%Y%m'),
                                  "INDEX": pd.date_range(start_dt, end_dt, freq='MS')})
        date_range['key'] = 'key'

        unique_dim = dataframe[self._dimension].drop_duplicates()
        unique_dim['key'] = 'key'

        cp_result = pd.merge(date_range, unique_dim, on='key').drop('key', axis=1)
        result_df = pd.merge(cp_result, dataframe, how='left', on=self._dimension + [self._date_column]).fillna(0)

        result_df = result_df.set_index("INDEX")
        dataset.pre_processing_input_data = result_df

        return dataset
