from m4.common.SingletonInstance import SingletonInstance
from m4.dao.AbstractDAO import AbstractDAO
from m4.dao.AbstractSession import AbstractSession

import pandas as pd


class InputDAO(AbstractDAO, SingletonInstance):
    """
    Input Data Access Object
    """

    @classmethod
    def read(cls, session: AbstractSession, params: dict):
        """
        Data Source로부터 리스트 데이터를 조회
        :return: DataFrame
        """
        select_query = f"""
        WITH TRNSC_ACTION AS (
            SELECT 2 TRNSC_ACTION_CD FROM DUAL UNION
            SELECT 4 TRNSC_ACTION_CD FROM DUAL
        )
        SELECT 
            TO_CHAR(TRNSC_DT,'YYYYMM') {params["DATE"]}, OWNER_ORG_CD ,ANNAME_CD,
            SUM(TRNSC_QTY) {params["VAL"]}
        FROM TIBERO.TSC_MTRIL_TRNSC TRNSC
            LEFT JOIN TIBERO.TMD_REPRSNT_CD_ANNAME ANNAME
            ON TRNSC.CMYN_RSCD = ANNAME.CMYN_RSCD
        WHERE TRNSC_ACTION_CD IN (SELECT TRNSC_ACTION_CD FROM TRNSC_ACTION)
        GROUP BY TO_CHAR(TRNSC_DT,'YYYYMM'), OWNER_ORG_CD ,ANNAME_CD
        UNION
        SELECT 
            TO_CHAR(TRNSC_DT,'YYYYMM') {params["DATE"]}, OWNER_ORG_CD ,ANNAME_CD,
            SUM(TRNSC_QTY) {params["VAL"]}
        FROM TIBERO.TSC_MTRIL_TRNSC_ARCHV ARCHV
            LEFT JOIN TIBERO.TMD_REPRSNT_CD_ANNAME ANNAME
            ON ARCHV.CMYN_RSCD = ANNAME.CMYN_RSCD
        WHERE TRNSC_ACTION_CD IN (SELECT TRNSC_ACTION_CD FROM TRNSC_ACTION)
        GROUP BY TO_CHAR(TRNSC_DT,'YYYYMM'), OWNER_ORG_CD ,ANNAME_CD
        """

        result = session.select(select_query)

        return pd.DataFrame(data=result['data'], columns=result['columns'])

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
          AND ANNAME_CD = ?
          AND OWNER_ORG_CD = ?
          AND STDR_MT   = ?
        """
        insert_query = """
        INSERT INTO TIBERO.TSC_FORST_ORG_RESRCE_NE
        (STDR_YY, ANNAME_CD, OWNER_ORG_CD, STDR_MT, ORG_GROUP_ID, GROUP_FORST_NE_QTY, FORST_NE_QTY, FORST_RATIO, 
        FORST_AT, CALT_RATIO, CRTR_ID, LAST_MODUSR_ID, CREAT_DT, LAST_MODF_DT)
        VALUES
        (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """
        delete_data = data[['STDR_YY', 'ANNAME_CD', 'OWNER_ORG_CD', 'STDR_MT']].drop_duplicates().values.tolist()
        insert_data = data.values.tolist()
        try:
            session.execute(delete_query, delete_data)
            session.execute(insert_query, insert_data)
            return True
        except Warning:
            return False
