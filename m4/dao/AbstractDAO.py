
from abc import *

from ..dao.AbstractSession import AbstractSession


class AbstractDAO(metaclass=ABCMeta):
    """
    Data Access Object 추상 클래스
    """

    @classmethod
    def map(cls, inp: dict) -> list:
        """
         Data Source의 조회 결과를 dict 형식으로 변환(필요 시)
         {"columns" : columns, "data" : list} =>
         [{"column_name" : value, "column_name" : value, ...}, {"column_name" : value, "column_name" : value, ...}, {"column_name" : value, "column_name" : value, ...}, ...]
         :rtype: list
         :param inp : 조회 결과({"columns" : columns, "data" : list})
         :return dict array
        """
        arr = []
        for data in inp["data"]:
            arr.append(dict(zip(inp["columns"], data)))
        return arr

    @classmethod
    def key_map(cls, inp: dict, key_column: str) -> dict:
        """
         Data Source의 조회 결과를 dict 형식으로 변환(필요 시)
         {"columns" : columns, "data" : list} =>
         {"key1": [{"column_name" : key1, "column_name" : value, ...}],
          "key2": [{"column_name" : key2, "column_name" : value, ...}],
          "key3": [{"column_name" : key3, "column_name" : value, ...}], ...}
         :rtype: dict
         :param inp : 조회 결과({"columns" : columns, "data" : list})
         :param key_column : key column
         :return dict
        """
        arr: list = cls.map(inp)
        res: dict = {}
        for row in arr:
            values: list = res.get(row[key_column])
            if values is None:
                values = []
                res[row[key_column]] = values
            values.append(row)
        return res

    @abstractmethod
    def execute(self, session: AbstractSession, sql_template: str, data_list: list):
        """
        Data Source에 대한 CUD를 실행
        :param session: AbstractSession 인스턴스
        :param sql_template: sql template string
        :param data_list: CUD 대상 데이터
        :return: True/False
        """
