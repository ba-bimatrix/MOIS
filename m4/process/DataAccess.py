from pandas import DataFrame
from datetime import datetime
import jpype.dbapi2

from m4.common.SingletonInstance import SingletonInstance
from m4.dao.AbstractDataSource import AbstractDataSource
from m4.dao.AbstractSession import AbstractSession
from m4.dao.InputDAO import InputDAO as InputDAO
from m4.dao.OrganizationDAO import OrganizationDAO as OrganizationDAO
from m4.dao.ResourceDAO import ResourceDAO as ResourceDAO
from m4.ApplicationConfiguration import ApplicationConfiguration
from m4.util.LogHandler import LogHandler


class DataAccess(SingletonInstance):

    _data_source: AbstractDataSource = None
    _db_data_source: AbstractDataSource = None
    _session: AbstractSession = None

    _input_dao: InputDAO = InputDAO.instance()
    _organization_dao: OrganizationDAO = OrganizationDAO.instance()
    _resource_dao: ResourceDAO = ResourceDAO.instance()
    config: ApplicationConfiguration = None

    def init(self, data_source: AbstractDataSource):

        self.config = ApplicationConfiguration.instance()

        self._load_period = self.config.parameter("LOAD_PERIOD")
        self._data_source = data_source
        self._session = data_source.get_session()
        self._start_dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self._logger = LogHandler.instance().get_logger()

    def get_session(self):
        return self._session

    def check_hist(self) -> bool:

        hist_list = self._session.select(
        """
        SELECT 
            STDR_YY,
            CRTR_ID
        FROM TIBERO.TSC_FORST_HIST
         WHERE STATUS_CD = 'R'
        """
        )

        excute_yn = bool(len(hist_list['data']))
        if bool(excute_yn):
            self.config.add_params('STDR_YY', hist_list['data'][0][0])
            self.config.add_params('CRTR_ID', hist_list['data'][0][1])
            # comments for dev
            # self._session.execute(
            # f"""
            # UPDATE TIBERO.TSC_FORST_HIST
            #     SET STATUS_CD = 'P',
            #         START_DT = ?
            #  WHERE STDR_YY = ?
            #    AND STATUS_CD = 'R'
            # """, [[self._start_dt, self.config.parameter("STDR_YY")]])
        else:
            self._logger.info("There is no process to run. Close the forecast program.")
            self._session.close()
            exit()
        return excute_yn

    def close_process(self):

        self._session.execute(
        """
        UPDATE TIBERO.TSC_FORST_HIST
                SET END_DT = ?,
                    STATUS_CD = 'E'
             WHERE STDR_YY = ?
               AND STATUS_CD = 'P'
        """, [[datetime.now().strftime('%Y-%m-%d %H:%M:%S'), self.config.parameter("STDR_YY")]])
        self._session.close()

    def fetch_organization_data(self):
        """
        organization data 조회
        :param :
        :return: DataFrame
        """

        return self._organization_dao.read(self._session, self._load_period)
    
    # TODO: 추천용 데이터 DB에서 불러오기 기능 필요
    def fetch_resource_data(self):
        """
        resource data 조회
        :param :
        :return: DataFrame
        """

        return self._resource_dao.read(self._session)
    
    # TODO: 예측용 데이터 DB에서 불러오기 기능 필요
    def fetch_input_data(self):
        """
        input data 조회
        :param :
        :return: DataFrame
        """

        return self._input_dao.read(self._session)

    def save_forecast(self, forecast: DataFrame):
        """
        forecast 저장
        :param : forecast : DataFrame
        :return:
        """

        result_msg = self._input_dao.execute(self._session, forecast)
        if result_msg:
            result_msg = 'Success'
        else:
            result_msg = 'Fail'
        self._logger.info(f'Saving forecast result is {result_msg}')

    def save_clustering(self, clustering: DataFrame):
        """
        clustering 저장
        :param : clustering : DataFrame
        :return:
        """
        result_msg = self._organization_dao.execute(self._session, clustering)
        if result_msg:
            result_msg = 'Success'
        else:
            result_msg = 'Fail'
        self._logger.info(f'Saving clustering result is {result_msg}')

    def save_recommend(self, recommend: DataFrame):
        """
        recommend 저장
        :param : recommend : DataFrame
        :return:
        """
        result_msg = self._resource_dao.execute(self._session, recommend)
        if result_msg:
            result_msg = 'Success'
        else:
            result_msg = 'Fail'
        self._logger.info(f'Saving recommend result is {result_msg}')

    def save_stocking_calculation(self, stocking_calculation: DataFrame):
        """
        stock calculation 저장
        :param : stocking_calculation : DataFrame
        :return:
        """
        pass
