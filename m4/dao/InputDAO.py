from m4.common.SingletonInstance import SingletonInstance
from m4.dao.AbstractDAO import AbstractDAO
from m4.dao.AbstractSession import AbstractSession


class InputDAO(AbstractDAO, SingletonInstance):
    """
    Input Data Access Object
    """

    def read(self, session: AbstractSession, **params):
        """
        Data Source로부터 리스트 데이터를 조회
        :param session: AbstractSession 인스턴스
        :param params: 파라미터 데이터
        :return: DataFrame
        """
        pass

    def execute(self, session: AbstractSession, sql_template: str, data_list: list):
        """
        Data Source에 대한 CUD를 실행
        :param session: AbstractSession 인스턴스
        :param sql_template: sql template string
        :param data_list: CUD 대상 데이터
        :return: True/False
        """
        pass
