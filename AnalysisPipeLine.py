from m4.ApplicationConfiguration import ApplicationConfiguration
from m4.dao.TiberoDataSource import TiberoDataSource
from m4.util.LogHandler import LogHandler
from m4.dao.DataSourceError import DataSourceError
from m4.process.ProcessException import ProcessException
from m4.dao.AbstractDataSource import AbstractDataSource

from m4.process.Dataset import Dataset
from m4.process.DataAccess import DataAccess
from m4.process.PreProcessor import PreProcessor
from m4.process.OrganizationCluster import OrganizationCluster
from m4.process.ResourceRecommender import ResourceRecommender
from m4.process.NecessaryForecast import NecessaryForecast
from m4.process.StockingCalculation import StockingCalculation
from m4.process.UserRecommender import UserRecommender
from m4.process.PostProcessor import PostProcessor

import os
import sys
from datetime import datetime
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

os.environ['TF_CPP_MIN_LOG_LEVEL'] = '1'

"""
최상위 어플리케이션 실행 파일
    분석 파이프라인 실행
"""


def pipe_line(data_source: AbstractDataSource) -> None:
    dataset = Dataset()

    data_access = DataAccess.instance()
    data_access.init(data_source)

    # sys.argv == True, check whether run or not, User define batch
    # sys.argv == None, run regular batch
    if len(sys.argv) > 1 and sys.argv[1]:
        data_access.check_hist()
    else:
        config = ApplicationConfiguration.instance()
        config.add_params('STDR_YY', datetime.now().year)
        config.add_params('OPERTOR_ID', 'SYSTEM')

    dataset.organization_data = data_access.fetch_organization_data()
    dataset.resource_data = data_access.fetch_resource_data()
    dataset.input_data = data_access.fetch_input_data()
    dataset.user_data = data_access.fetch_user_data()

    dataset = PreProcessor.instance().process_cluster(dataset)
    dataset.clustering = OrganizationCluster.instance().cluster(dataset.pre_processing_organization_data)

    dataset = PreProcessor.instance().process_recommend(dataset)
    dataset.recommend = ResourceRecommender.instance().recommend(dataset.pre_processing_resource_data)

    dataset = PreProcessor.instance().process_forecast(dataset)
    dataset.forecast = NecessaryForecast.instance().forecast(dataset.pre_processing_input_data)

    dataset = PreProcessor.instance().process_stocking(dataset)
    dataset.stocking_calculation = StockingCalculation.instance().calculation(dataset.pre_processing_resource_data)

    dataset = PreProcessor.instance().process_user_recommend(dataset)
    dataset.user_recommend = UserRecommender.instance().recommend(dataset.pre_processing_user_data)

    dataset = PostProcessor.instance().process(dataset)

    data_access.save_clustering(dataset.clustering)
    data_access.save_recommend(dataset.recommend)
    data_access.save_forecast(dataset.forecast)
    data_access.save_stocking_calculation(dataset.stocking_calculation)
    data_access.save_user_recommend(dataset.user_recommend)

    data_access.close_process()


def main():
    config: ApplicationConfiguration = ApplicationConfiguration.instance()
    config.init('m4.properties')
    config.parsing_properties()

    loghandler: LogHandler = LogHandler.instance()
    loghandler.init(config)

    logger = loghandler.get_logger()

    try:
        data_source = TiberoDataSource.instance()
        data_source.init(config)
        data_source.get_session()

        logger.info("pipeline started")
        pipe_line(data_source)
        logger.info("pipeline ended")
    except DataSourceError as e:
        logger.error(e)
    except ProcessException as e:
        logger.error(e)
    finally:
        pass


if __name__ == '__main__':
    main()
