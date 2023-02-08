from pandas import DataFrame

from m4.common.SingletonInstance import SingletonInstance
from m4.dao.AbstractDataSource import AbstractDataSource
from m4.dao.AbstractSession import AbstractSession
from m4.dao.FileInputDAO import FileInputDAO as InputDAO
# from m4.dao.FileOrganizationDAO import FileOrganizationDAO as OrganizationDAO
from m4.dao.TiberoSqlSession import TiberoSqlSession
from m4.dao.OrganizationDAO import OrganizationDAO as OrganizationDAO
from m4.dao.FileResourceDAO import FileResourceDAO as ResourceDAO
from m4.ApplicationConfiguration import ApplicationConfiguration

class DataAccess(SingletonInstance):

    _data_source: AbstractDataSource = None
    _db_data_source: AbstractDataSource = None
    _session: AbstractSession = None

    _input_dao: InputDAO = InputDAO.instance()
    _organization_dao: OrganizationDAO = OrganizationDAO.instance()
    _resource_dao: ResourceDAO = ResourceDAO.instance()

    def init(self, data_source: AbstractDataSource):

        config = ApplicationConfiguration.instance()

        self._load_period = config.parameter("LOAD_PERIOD")
        self._data_source = data_source
        # self._session = data_source.get_session()
        self._session = TiberoSqlSession()
        self._session.get_connection()

    def get_session(self):
        return self._session

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

        return self._resource_dao.read(self._session)

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
        pass

    def save_clustering(self, clustering: DataFrame):
        """
        clustering 저장
        :param : clustering : DataFrame
        :return:
        """
        pass

    def save_recommend(self, recommend: DataFrame):
        """
        recommend 저장
        :param : recommend : DataFrame
        :return:
        """
        pass

    def save_stocking_calculation(self, stocking_calculation: DataFrame):
        """
        stock calculation 저장
        :param : stocking_calculation : DataFrame
        :return:
        """
        pass
