from m4.common.SingletonInstance import SingletonInstance
from m4.dao.AbstractDAO import AbstractDAO
from m4.dao.AbstractSession import AbstractSession

import pandas as pd  # delete after
from m4.ApplicationConfiguration import ApplicationConfiguration  # delete after


class InputDAO(AbstractDAO, SingletonInstance):
    """
    Input Data Access Object
    """

    # TODO: csv load -> DB load 변경 필요
    @classmethod
    def read(cls, session: AbstractSession):
        """
        Data Source로부터 리스트 데이터를 조회
        :return: DataFrame
        """

        config = ApplicationConfiguration.instance()
        ret = pd.read_csv(
            config.find("FileSource", "file.directory") + "/" + config.find("FileSource", "file.input_data"),
            dtype=object)

        # value_column = ApplicationConfiguration.instance().parameter("FORECAST_VALUE_COL")
        value_column = ApplicationConfiguration.instance().parameter("VALUE_COL")
        return ret.astype({value_column: "int32"})

    def execute(self, session: AbstractSession, data: pd.DataFrame):
        """
        Data Source에 대한 CUD를 실행
        :param session: AbstractSession 인스턴스
        :param data: CUD 대상 데이터
        :return: True/False
        """
        delete_query = """
        DELETE FROM TIBERO.TSC_FORST_ORG_RESRCE_NE
        WHERE STDR_YY   = ?
          AND CMYN_RSCD = ?
          AND ORG_CD    = ?
          AND STDR_MT   = ?
        """
        insert_query = """
        INSERT INTO TIBERO.TSC_FORST_ORG_RESRCE_NE
        (STDR_YY, CMYN_RSCD, ORG_CD, STDR_MT, ORG_GROUP_ID, GROUP_FORST_NE_QTY, FORST_NE_QTY, FORST_RATIO, 
        FORST_AT, CALT_RATIO, CRTR_ID, LAST_MODUSR_ID, CREAT_DT, LAST_MODF_DT)
        VALUES
        (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """
        delete_data = data[['STDR_YY', 'CMYN_RSCD', 'ORG_CD', 'STDR_MT']].drop_duplicates().values.tolist()
        insert_data = data.values.tolist()
        try:
            session.execute(delete_query, delete_data)
            session.execute(insert_query, insert_data)
            return True
        except Warning:
            return False
