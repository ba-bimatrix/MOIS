import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta

from m4.ApplicationConfiguration import ApplicationConfiguration
from m4.common.SingletonInstance import SingletonInstance
from m4.process.Dataset import Dataset

from sklearn.preprocessing import MinMaxScaler, StandardScaler, RobustScaler, OneHotEncoder


class PreProcessor(SingletonInstance):

    _organ_pk: list = None
    _input_period: int = None
    _columns: list = None
    _dimension: list = None
    _execute_date: str = None
    _date_column: str = None
    _val_column: str = None
    _region_column: str = None
    _clust_nm: str = None

    def __init__(self):
        """
        생성자
        """
        config = ApplicationConfiguration.instance()

        self._tmp_col = config.parameter("ORGANIZATION_STRUCTURE")  # temporary parameter
        self._organ_pk = config.parameter("ORGAN_PK")
        self._mater_pk = config.parameter("MATER_PK")
        self._input_period = config.parameter("FORECAST_INPUT_PERIOD")
        self._columns = config.parameter("FORECAST_COLUMNS")
        self._dimension = config.parameter("FORECAST_DIMENSION")
        self._execute_date = config.parameter("EXECUTE_DATE")
        self._date_column = config.parameter("DATE_COL")
        self._val_column = config.parameter("VALUE_COL")
        self._region_column = config.parameter("REGION_COL")
        self._clust_nm = config.parameter("CLUSTER_COL")

    # TODO: 범주형 자료 원핫 인코딩 및 다른 최적 스케일링 추가 필요 + 리팩토링 필요
    def process_cluster(self, dataset: Dataset) -> Dataset:
        """pre-processing for cluster
        :param dataset:
        :return:
        """
        preprocess_df = self._aggregate_cluster(dataset.organization_data)

        scaler = MinMaxScaler()
        preprocess_df[['POPUL_CNT', 'HOHOLD_CNT', 'RESI_CNT', 'BUGE_AMT', 'FULL_SQUARE',
                        'ROAD_SQUARE', 'CULT_SQUARE', 'FORE_SQUARE', 'RIVER_SQUARE', 'PBORD_AMT',
                        'TRSPT_AMT', 'MNCP_AMT', 'LCPB_AMT', 'GVN_MNG_SQUARE', 'GVN_SPRT_SQUARE',
                        'ETC_SQUARE', 'DMG_STORM_FLOOD_AMT']] = \
         scaler.fit_transform(preprocess_df[['POPUL_CNT', 'HOHOLD_CNT', 'RESI_CNT', 'BUGE_AMT', 'FULL_SQUARE',
                        'ROAD_SQUARE', 'CULT_SQUARE', 'FORE_SQUARE', 'RIVER_SQUARE', 'PBORD_AMT',
                        'TRSPT_AMT', 'MNCP_AMT', 'LCPB_AMT', 'GVN_MNG_SQUARE', 'GVN_SPRT_SQUARE',
                        'ETC_SQUARE', 'DMG_STORM_FLOOD_AMT']])
        dataset.pre_processing_organization_data = preprocess_df

        return dataset

    def _aggregate_cluster(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        columns_map = {
            'average': ['POPUL_CNT', 'HOHOLD_CNT', 'RESI_CNT', 'BUGE_AMT', 'FULL_SQUARE',
                        'ROAD_SQUARE', 'CULT_SQUARE', 'FORE_SQUARE', 'RIVER_SQUARE', 'PBORD_AMT',
                        'TRSPT_AMT', 'MNCP_AMT', 'LCPB_AMT', 'GVN_MNG_SQUARE', 'GVN_SPRT_SQUARE',
                        'ETC_SQUARE', 'DMG_STORM_FLOOD_AMT'],
            'get_last': ['REGION_CD', 'ADMDSTRC_CD', 'COAST_ADJ_AT', 'INLAND_CTY_AT', 'COAST_LEN']
        }

        return pd.concat(
            [dataframe.groupby(self._organ_pk)[columns_map['average']].mean(),
             dataframe[dataframe['STDR_YY'] == max(dataframe['STDR_YY'])][columns_map['get_last']+['ORG_CD']
            ].set_index('ORG_CD')], axis=1).reset_index()

    def process_recommend(self, dataset: Dataset) -> Dataset:
        """pre-processing for recommend
        :param : dataset: Dataset
        :return: dataset: Dataset
        """
        dataset.pre_processing_resource_data = pd.merge(dataset.input_data, dataset.clustering, how='left',
                                                        on=self._organ_pk)

        return dataset

    def process_forecast(self, dataset: Dataset) -> Dataset:
        """pre-processing for forecast
        :param : dataset: Dataset
        :return: dataset: Dataset
        """
        dataset = self._avg_by_cluster(dataset, self._date_column)
        dataset = self._fill_zero_and_date(dataset)

        return dataset

    def _avg_by_cluster(self, dataset: Dataset, *args: str) -> Dataset:
        """Preprocessing before timeseries forecasting
        sum data by same cluster
        :param : dataset: Dataset
        :param : args: tuple
        :return: dataset: Dataset
        """
        dataframe = pd.merge(dataset.input_data, dataset.clustering, how='left', on=self._organ_pk)
        result_df = dataframe.groupby([self._clust_nm] + self._mater_pk + list(args), as_index=False)[self._val_column].mean()

        dataset.pre_processing_input_data = result_df

        return dataset

    def _fill_zero_and_date(self, dataset: Dataset) -> Dataset:
        """월별 실적과 함께 집계 컬럼, 시작/종료월을 넣을 시 결측월들을 0으로 채워줌.
        :param : dataset: Dataset 월별 실적
        :return: dataset: Dataset
        """

        if dataset.pre_processing_input_data is None:
            raise Exception("There is no data for filling to null months. plz check previous processing")

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


if __name__ == '__main__':
    print("preprocessing test start")
    from m4.process.Dataset import Dataset

    test_params = {
        "EXECUTE_DATE": "202205",
        "ORGANIZATION_STRUCTURE": ["ORGAN_CODE", "TOP_CODE", "TOP_ORGAN", "FULL_ORGAN", "LWST_ORGAN"],  # useless
        "MATERIAL_STRUCTURE": ["SUPPLIES_CLASS_L", "SUPPLIES_CLASS_M", "SUPPLIES_CLASS_S", "SUPPLIES_CODE"],  # useless
        "DATE_COL": "YYMM",
        "ORGAN_PK": ["ORGAN_CODE"],
        "MATER_PK": ["SUPPLIES_CODE"],
        "VALUE_COL": "VAL",

        "FORECAST_INPUT_PERIOD": 36,
        "FORECAST_COLUMNS": ["CLUSTER", "YYMM", "VAL"],
        "FORECAST_DIMENSION": ["CLUSTER"],

        "REGION_COL": "REGION",
        "CLUSTER_COL": "CLUSTER"
    }
    config: ApplicationConfiguration = ApplicationConfiguration.instance()
    config.init('m4.properties', test_params)

    test_dataset = Dataset()

    test_dataset.organization_data = pd.read_csv('..\..\data\data_source\clust_data.csv', dtype=object)
    test_dataset.resource_data = pd.read_csv('..\..\data\data_source\input_data.csv', dtype=object)
    test_dataset.input_data = pd.read_csv('..\..\data\data_source\input_data.csv', dtype=object)

    test_dataset.clustering = test_dataset.organization_data[test_params["ORGANIZATION_STRUCTURE"]]
    test_dataset.clustering.loc[:, test_params["CLUSTER_COL"]] = "TEST"

    test_dataset = PreProcessor.instance().process_cluster(test_dataset)
    test_dataset = PreProcessor.instance().process_recommend(test_dataset)
    test_dataset = PreProcessor.instance().process_forecast(test_dataset)

    print(f"cluster preprocessing result is \n{test_dataset.pre_processing_organization_data}")
    print(f"recommend preprocessing result is \n{test_dataset.pre_processing_resource_data}")
    print(f"forecast preprocessing result is \n{test_dataset.pre_processing_input_data}")
    print("preprocessing test success")
