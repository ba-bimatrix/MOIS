from m4.common.SingletonInstance import SingletonInstance
from m4.dao.AbstractDAO import AbstractDAO
from m4.dao.AbstractSession import AbstractSession

import pandas as pd
from m4.ApplicationConfiguration import ApplicationConfiguration


class ResourceDAO(AbstractDAO, SingletonInstance):
    """
    Resource Data Access Object
    """

    @staticmethod
    def read(session: AbstractSession, params: dict):
        """
        Data Source로부터 리스트 데이터를 조회
        :param session: AbstractSession 인스턴스
        :param params: 파라미터 데이터
        :return: DataFrame
        """
        select_query = f"""
        WITH TRNSC_ACTION AS (
            SELECT 1 TRNSC_ACTION_CD FROM DUAL UNION
            SELECT 3 TRNSC_ACTION_CD FROM DUAL
        )
        SELECT 
            TO_CHAR(TRNSC_DT,'YYYY') {params['DATE']}, OWNER_ORG_CD ,ANNAME_CD, BSPLC_CD,
            SUM(TRNSC_QTY) {params['VAL']}
        FROM TIBERO.TSC_MTRIL_TRNSC TRNSC
            LEFT JOIN TIBERO.TMD_REPRSNT_CD_ANNAME ANNAME
            ON TRNSC.CMYN_RSCD = ANNAME.CMYN_RSCD
        WHERE TRNSC_ACTION_CD IN (SELECT TRNSC_ACTION_CD FROM TRNSC_ACTION)
        GROUP BY TO_CHAR(TRNSC_DT,'YYYY'), OWNER_ORG_CD ,ANNAME_CD, BSPLC_CD
        UNION
        SELECT 
            TO_CHAR(TRNSC_DT,'YYYY') {params['DATE']}, OWNER_ORG_CD ,ANNAME_CD, BSPLC_CD,
            SUM(TRNSC_QTY) {params['VAL']}
        FROM TIBERO.TSC_MTRIL_TRNSC_ARCHV ARCHV
            LEFT JOIN TIBERO.TMD_REPRSNT_CD_ANNAME ANNAME
            ON ARCHV.CMYN_RSCD = ANNAME.CMYN_RSCD
        WHERE TRNSC_ACTION_CD IN (SELECT TRNSC_ACTION_CD FROM TRNSC_ACTION)
        GROUP BY TO_CHAR(TRNSC_DT,'YYYY'), OWNER_ORG_CD ,ANNAME_CD ,BSPLC_CD
        """
        result = session.select(select_query)

        return pd.DataFrame(data=result['data'], columns=result['columns'])

    def execute(self, session: AbstractSession, data: pd.DataFrame, mode: bool) -> bool:
        """
        Data Source에 대한 CUD를 실행
        :param session: AbstractSession 인스턴스
        :param data: CUD 대상 데이터
        :return: True/False
        """
        if mode:
            delete_query = """
            DELETE FROM TIBERO.TSC_FORST_ORG_GROUP_RESRCE_RECND
            WHERE STDR_YY = ?
              AND ORG_GROUP_ID = ?
              AND ANNAME_CD = ?
            """
            insert_query = """
            INSERT INTO TIBERO.TSC_FORST_ORG_GROUP_RESRCE_RECND
            (STDR_YY, ORG_GROUP_ID, ANNAME_CD, FORST_RKING, FORST_AT, 
            CALT_RKING, CRTR_ID, LAST_MODUSR_ID, CREAT_DT, LAST_MODF_DT)
            VALUES (?,?,?,?,?,?,?,?,?,?)
            """
            delete_data = data[['STDR_YY', 'ORG_GROUP_ID', 'ANNAME_CD']].drop_duplicates().values.tolist()
            insert_data = data.values.tolist()
        else:
            delete_query = """
            DELETE FROM TIBERO.TSC_FORST_ORG_GROUP_RESRCE_SHA
            WHERE STDR_YY = ?
              AND OWNER_ORG_CD = ?
              AND ANNAME_CD = ?
              AND WRHOUS_CD = ?
            """
            insert_query = """
            INSERT INTO TIBERO.TSC_FORST_ORG_GROUP_RESRCE_SHA
            (STDR_YY, OWNER_ORG_CD, ANNAME_CD, WRHOUS_CD, FORST_RATIO, 
            FORST_AT, CALT_RATIO, CRTR_ID, LAST_MODUSR_ID, CREAT_DT, LAST_MODF_DT)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """
            delete_data = data[['STDR_YY', 'OWNER_ORG_CD', 'ANNAME_CD', 'WRHOUS_CD']].drop_duplicates().values.tolist()
            insert_data = data.values.tolist()

        try:
            session.execute(delete_query, delete_data)
            session.execute(insert_query, insert_data)
            return True
        except Warning:
            return False
