from pandas import DataFrame
from datetime import datetime
from dateutil.relativedelta import relativedelta

from m4.common.SingletonInstance import SingletonInstance
from m4.dao.AbstractDataSource import AbstractDataSource
from m4.dao.AbstractSession import AbstractSession
from m4.dao.InputDAO import InputDAO as InputDAO
from m4.dao.OrganizationDAO import OrganizationDAO as OrganizationDAO
from m4.dao.ResourceDAO import ResourceDAO as ResourceDAO
from m4.dao.UserDAO import UserDAO as UserDAO
from m4.ApplicationConfiguration import ApplicationConfiguration
from m4.util.LogHandler import LogHandler


class DataAccess(SingletonInstance):

    config: ApplicationConfiguration = None
    _data_source: AbstractDataSource = None
    _session: AbstractSession = None
    _logger: LogHandler = None
    _begin_time: str = None
    _load_period: int = None
    _value_col: str = None
    _forecast_dt: str = None
    _resource_dt: str = None
    _input_period: int = None
    _execute_date: str = None
    _start_date: str = None

    _input_dao: InputDAO = InputDAO.instance()
    _organization_dao: OrganizationDAO = OrganizationDAO.instance()
    _resource_dao: ResourceDAO = ResourceDAO.instance()
    _user_dao: UserDAO = UserDAO.instance()

    def init(self, data_source: AbstractDataSource):
        """ init, DB access & set parameters
        :param data_source:
        :return:
        """
        self.config = ApplicationConfiguration.instance()
        self._data_source = data_source
        self._session = data_source.get_session()
        self._logger = LogHandler.instance().get_logger()
        self._begin_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self._load_period = self.config.parameter("LOAD_PERIOD")
        self._value_col = self.config.parameter("VALUE_COL")
        self._forecast_dt = self.config.parameter("FORECAST_DATE")
        self._resource_dt = self.config.parameter("RESOURCE_DATE")
        self._input_period = self.config.parameter("FORECAST_INPUT_PERIOD")
        self._execute_date = self.config.parameter("EXECUTE_DATE")
        self._start_date = (datetime.strptime(self._execute_date + '01', '%Y%m%d') +
                            relativedelta(months=-(self._input_period - 1))).strftime('%Y%m')

    def get_session(self):
        return self._session

    def check_hist(self) -> bool:

        hist_list = self._session.select("""
        SELECT 
            STDR_YY,
            OPERTOR_ID
        FROM TIBERO.TSC_FORST_HIST
         WHERE STTUS_CD = 'R'
        """, {})

        excute_yn = bool(len(hist_list['data']))
        if bool(excute_yn):
            self.config.add_params('STDR_YY', hist_list['data'][0][0])
            self.config.add_params('OPERTOR_ID', hist_list['data'][0][1])
            # comments for dev
            # self._session.execute(
            # f"""
            # UPDATE TIBERO.TSC_FORST_HIST
            #     SET STTUS_CD = 'P',
            #         BEGIN_DT = ?
            #  WHERE STDR_YY = ?
            #    AND STTUS_CD = 'R'
            # """, [[self._start_dt, self.config.parameter("STDR_YY")]])
        else:
            self._logger.info("There is no process to run. Close the forecast program.")
            self._session.close()
            exit()
        return excute_yn

    def close_process(self):

        self._session.execute("""
        UPDATE TIBERO.TSC_FORST_HIST
                SET END_DT = ?,
                    STTUS_CD = 'E'
             WHERE STDR_YY = ?
               AND STTUS_CD = 'P'
        """, [[datetime.now().strftime('%Y-%m-%d %H:%M:%S'), self.config.parameter("STDR_YY")]])
        self._session.close()

    def fetch_organization_data(self):
        """
        organization data 조회
        :param :
        :return: DataFrame
        """

        return self._organization_dao.read(self._session, self._load_period)
    
    def fetch_resource_data(self):
        """
        resource data 조회
        :param :
        :return: DataFrame
        """

        return self._resource_dao.read(self._session, {"DATE": self._resource_dt, "VAL": self._value_col})
    
    def fetch_input_data(self):
        """
        input data 조회
        :param :
        :return: DataFrame
        """

        return self._input_dao.read(self._session, {"DATE": self._forecast_dt, "VAL": self._value_col})

    def fetch_user_data(self):
        """
        input data 조회
        :param :
        :return: DataFrame
        """

        return self._user_dao.read(self._session, {"DATE": self._forecast_dt,
                                                   "START_DT": self._start_date, "END_DT": self._execute_date})

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
        result_msg = self._resource_dao.execute(self._session, recommend, True)
        if result_msg:
            result_msg = 'Success'
        else:
            result_msg = 'Fail'
        self._logger.info(f'Saving recommend result is {result_msg}')

    def save_stocking_calculation(self, stocking: DataFrame):
        """
        stock calculation 저장
        :param : stocking_calculation : DataFrame
        :return:
        """
        result_msg = self._resource_dao.execute(self._session, stocking, False)
        if result_msg:
            result_msg = 'Success'
        else:
            result_msg = 'Fail'
        self._logger.info(f'Saving recommend result is {result_msg}')

    def save_user_recommend(self, recommend: DataFrame):
        return None
