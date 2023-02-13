
import pandas as pd

from m4.common.SingletonInstance import SingletonInstance
from m4.dao.AbstractDAO import AbstractDAO
from m4.dao.AbstractSession import AbstractSession
from m4.ApplicationConfiguration import ApplicationConfiguration


class FileResourceDAO(AbstractDAO, SingletonInstance):
    """
    File Input Data Access Object
    """

    @classmethod
    def read(cls):
        """
        Data Source로부터 리스트 데이터를 조회
        :return: DataFrame
        """
        config = ApplicationConfiguration.instance()
        ret = pd.read_csv(config.find("FileSource", "file.directory") + "/" +
                          config.find("FileSource", "file.resource_data"), dtype=object)

        return ret

    def execute(self, session: AbstractSession, data: pd.DataFrame):
        """
        Data Source에 대한 CUD를 실행
        :param session: AbstractSession 인스턴스
        :param data: CUD 대상 데이터
        :return: True/False
        """
        pass
