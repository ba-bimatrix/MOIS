
import os

from m4.common.SingletonInstance import SingletonInstance
from m4.dao.AbstractDataSource import AbstractDataSource
from m4.ApplicationConfiguration import ApplicationConfiguration


class FileDataSource(AbstractDataSource, SingletonInstance):
    """
    File Data Source 클래스
    """

    _file_directory = ""
    _file_input_data = ""

    def __init__(self):
        """
        생성자 : FileDataSource 클래스 멤버 변수들
        """
        super(__class__, self).__init__()

    # Public 메서드
    def init(self, config: ApplicationConfiguration):
        """
        DataSource 초기화
        :param: config - Application Configuration
        """
        uri_map = dict(config.find_section("FileSource"))
        self._file_directory = uri_map["file.directory"]

    def get_session(self):
        return None

    def release_session(self, session: object):
        pass

    def read_csv(self, file_path):
        pass

    def close(self):
        pass
