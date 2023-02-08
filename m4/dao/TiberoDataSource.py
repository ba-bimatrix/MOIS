import jaydebeapi as jp

from m4.common.SingletonInstance import SingletonInstance
from m4.dao.AbstractDataSource import AbstractDataSource
from m4.ApplicationConfiguration import ApplicationConfiguration
from m4.dao.TiberoSqlSession import TiberoSqlSession


class TiberoDataSource(AbstractDataSource, SingletonInstance):
    _connection = None

    def init(self, config: ApplicationConfiguration):
        self._jclassname = config.parameter("DS.CONNECTION.JCLASSNM")
        self._url = config.parameter("DS.CONNECTION.URL")
        self._driver_args = {'user': config.parameter("DS.CONNECTION.USER"),
                             'password': config.parameter("DS.CONNECTION.PW")}
        self._jars = config.parameter("DS.CONNECTION.JDBCPATH")

    def get_session(self):
        session = TiberoSqlSession()
        session.init(self, [self._connection])
        return session

    def release_session(self, session: TiberoSqlSession):
        session.close()

    def close(self):
        pass
