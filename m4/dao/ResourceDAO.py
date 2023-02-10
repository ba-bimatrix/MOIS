from m4.common.SingletonInstance import SingletonInstance
from m4.dao.AbstractDAO import AbstractDAO
from m4.dao.AbstractSession import AbstractSession

import pandas as pd
from m4.ApplicationConfiguration import ApplicationConfiguration


class ResourceDAO(AbstractDAO, SingletonInstance):
    """
    Resource Data Access Object
    """

    # TODO: csv load -> DB load 변경 필요
    @classmethod
    def read(self, session: AbstractSession, **params):
        """
        Data Source로부터 리스트 데이터를 조회
        :param session: AbstractSession 인스턴스
        :param params: 파라미터 데이터
        :return: DataFrame
        """
        config = ApplicationConfiguration.instance()
        ret = pd.read_csv(
            config.find("FileSource", "file.directory") + "/" + config.find("FileSource", "file.resource_data"),
            dtype=object)

        return ret


    def execute(self, session: AbstractSession, data: pd.DataFrame) -> bool:
        """
        Data Source에 대한 CUD를 실행
        :param session: AbstractSession 인스턴스
        :param data_list: CUD 대상 데이터
        :return: True/False
        """
        delete_query = \
        """
        DELETE FROM TIBERO.TSC_FORST_ORG_GROUP_RESRCE_RECND
        WHERE STDR_YY = ?
          AND ORG_GROUP_ID = ?
          AND CMYN_RSCD = ?
        """
        insert_query = \
        """
        INSERT INTO TIBERO.TSC_FORST_ORG_GROUP_RESRCE_RECND
        (STDR_YY, ORG_GROUP_ID, CMYN_RSCD, FORST_RKING, FORST_AT, CALT_RKING, CRTR_ID, LAST_MODUSR_ID, CREAT_DT, LAST_MODF_DT)
        VALUES
        (?,?,?,?,?,?,?,?,?,?)
        """
        delete_data = data[['STDR_YY', 'ORG_GROUP_ID', 'CMYN_RSCD']].drop_duplicates().values.tolist()
        insert_data = data.values.tolist()
        try:
            session.execute(delete_query, delete_data)
            session.execute(insert_query, insert_data)
            return True
        except:
            return False