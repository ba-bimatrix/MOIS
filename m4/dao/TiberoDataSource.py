
from m4.common.SingletonInstance import SingletonInstance
from m4.dao.AbstractDataSource import AbstractDataSource
from m4.ApplicationConfiguration import ApplicationConfiguration
from m4.dao.TiberoSqlSession import TiberoSqlSession


class TiberoDataSource(AbstractDataSource, SingletonInstance):
    _connection = None
    _file_directory = None  # tmp
    _file_input_data = None  # tmp

    def __init__(self):
        """
        생성자 : FileDataSource 클래스 멤버 변수들
        """
        super(__class__, self).__init__()

    # TODO: 현재 기관특성데이터만 DB에서 불러옴, 다른 csv도 DB에서 불러와야함
    def init(self, config: ApplicationConfiguration):
        self._jclassname = config.parameter("DS.CONNECTION.JCLASSNM")
        self._url = config.parameter("DS.CONNECTION.URL")
        self._driver_args = {'user': config.parameter("DS.CONNECTION.USER"),
                             'password': config.parameter("DS.CONNECTION.PW")}
        self._jars = config.parameter("DS.CONNECTION.JDBCPATH")

        # tmp
        uri_map = dict(config.find_section("FileSource"))
        self._file_directory = uri_map["file.directory"]

    def get_session(self):
        session = TiberoSqlSession()
        session.init(self, [self._connection])
        return session

    def release_session(self, session: TiberoSqlSession):
        session.close()

    def close(self, session: TiberoSqlSession):
        session.close()
