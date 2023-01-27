from m4.ApplicationConfiguration import ApplicationConfiguration
from m4.util.LogHandler import LogHandler
from m4.dao.DataSourceError import DataSourceError
from m4.process.ProcessException import ProcessException
from m4.dao.AbstractDataSource import AbstractDataSource
from m4.dao.FileDataSource import FileDataSource
from m4.dao.OracleDataSource import OracleDataSource
from m4.process.Dataset import Dataset
from m4.process.DataAccess import DataAccess
from m4.process.PreProcessor import PreProcessor
from m4.process.OrganizationCluster import OrganizationCluster
from m4.process.ResourceRecommender import ResourceRecommender
from m4.process.NecessaryForecast import NecessaryForecast
from m4.process.StockingCalculation import StockingCalculation
from m4.process.PostProcessor import PostProcessor

import os
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '1'

"""
최상위 어플리케이션 실행 파일
    분석 파이프라인 실행
"""


def pipe_line(data_source: AbstractDataSource) -> None:
    dataset = Dataset()

    data_access = DataAccess.instance()
    data_access.init(data_source)

    dataset.organization_data = data_access.fetch_organization_data()
    dataset.resource_data = data_access.fetch_resource_data()
    dataset.input_data = data_access.fetch_input_data()

    dataset = PreProcessor.instance().process_cluster(dataset)
    dataset.clustering = OrganizationCluster.instance().cluster(dataset.pre_processing_organization_data)

    dataset = PreProcessor.instance().process_recommend(dataset)
    dataset.recommend = ResourceRecommender.instance().recommend(dataset.pre_processing_resource_data)

    dataset = PreProcessor.instance().process_forecast(dataset)
    dataset.forecast = NecessaryForecast.instance().forecast(dataset.pre_processing_input_data)

    dataset.stocking_calculation = StockingCalculation.instance().calculation(dataset.pre_processing_input_data)

    dataset = PostProcessor.instance().process(dataset)

    data_access.save_clustering(dataset.clustering)
    data_access.save_recommend(dataset.recommend)
    data_access.save_forecast(dataset.forecast)
    data_access.save_stocking_calculation(dataset.stocking_calculation)

    print(dataset.forecast["result"])
    print(dataset.forecast["accuracy"])


def main():
    # params for test
    ## 고정 변수: 실행일, 지자체 분류체계, 재난비품 분류체계, 연월, PK, 실적
    params = {
        "EXECUTE_DATE": "202205",
        "ORGANIZATION_STRUCTURE": ["ORGAN_CODE", "TOP_CODE", "TOP_ORGAN", "FULL_ORGAN", "LWST_ORGAN"],  # useless
        "MATERIAL_STRUCTURE": ["SUPPLIES_CLASS_L", "SUPPLIES_CLASS_M", "SUPPLIES_CLASS_S", "SUPPLIES_CODE"],  # useless
        "DATE_COL": "YYMM",
        "ORGAN_PK": ["ORGAN_CODE"],
        "MATER_PK": ["SUPPLIES_CODE"],
        "VALUE_COL": "VAL"
    }
    ## 지자체 군집
    clust_params = {
        # 컬럼명 설정
        "REGION_COL": "REGION",
        "CLUSTER_PK": params["ORGANIZATION_STRUCTURE"],
        "CLUSTER_COL": "CLUSTER",

        # 알고리즘 설정
        "MIN_N_CLUSTERS": 2,
        "MAX_N_CLUSTERS": 10
    }
    ## 소비 예측
    fcst_params = {
        # 변수 설정
        "FORECAST_COLUMNS": [clust_params["CLUSTER_COL"], params["DATE_COL"], params["VALUE_COL"]],
        "FORECAST_DIMENSION": [clust_params["CLUSTER_COL"]],

        # 알고리즘 세팅
        "FORECAST_ALGORITHMS": ["EMV", "HW", "REG", "ARM", "LSTM"],
        "FORECAST_INPUT_PERIOD": 36,
        "FORECAST_PERIOD": 12,
        "FORECAST_VALIDATION_PERIOD": 6,

        "SMOOTHING_LEVEL": 0.5,
        "SEASONALITY_PERIOD": 12,
        "ARIMA_ORDER": [(1, 0, 1), (2, 0, 2), (2, 1, 2), (2, 0, 1), (1, 0, 2)],
        "LSTM_TRAIN_SIZE": 0.9,
        "LSTM_WINDOW_INPUT": 12,
        "LSTM_DROP_RATE": 0.1,
        "LSTM_LEARNING_RATE": 0.001,
        "LSTM_EPOCHS": 10
    }
    ## 비품 추천
    reco_params = {
        "RECO_DIMENSION": params["ORGAN_PK"] + params["MATER_PK"] + [params["VALUE_COL"]],
        "CLUSTER_COL": clust_params["CLUSTER_COL"]
    }
    params.update(clust_params)
    params.update(fcst_params)
    params.update(reco_params)

    config: ApplicationConfiguration = ApplicationConfiguration.instance()
    config.init('m4.properties', params)

    logHandler: LogHandler = LogHandler.instance()
    logHandler.init(config)

    logger = logHandler.get_logger()

    try:

        #        oracle_data_source: OracleDataSource = OracleDataSource.instance()
        #        oracle_data_source.init(config)
        csv_data_source: FileDataSource = FileDataSource.instance()
        csv_data_source.init(config)

        logger.info("pipeline started")
        pipe_line(csv_data_source)
        logger.info("pipeline ended")

    except DataSourceError as e:
        logger.error(e)
    except ProcessException as e:
        logger.error(e)
    finally:
        pass

if __name__ == '__main__':
    main()
