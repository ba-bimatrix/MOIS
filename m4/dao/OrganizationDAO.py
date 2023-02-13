import pandas as pd

from m4.common.SingletonInstance import SingletonInstance
from m4.dao.AbstractDAO import AbstractDAO
from m4.dao.AbstractSession import AbstractSession


class OrganizationDAO(AbstractDAO, SingletonInstance):
    """
    Organization Data Access Object
    """

    @staticmethod
    def read(session: AbstractSession, param):
        """
        Data Source로부터 리스트 데이터를 조회
        :param session: AbstractSession 인스턴스
        :param param: 파라미터 데이터
        :return: DataFrame
        """
        select_query = """
        WITH ANALY_PERIOD AS (
                    SELECT TO_CHAR(ADD_MONTHS(SYSDATE, -LEVEL*12),'YYYY') AS STDR_YY 
                        FROM DUAL CONNECT BY LEVEL<=?
            ), CONSIS_CHECK AS (
                    SELECT STND.STDR_YY,
                           CASE WHEN STND.STND_CNT=CPRN.CPRN_CNT THEN 'Y' ELSE 'N' END AS CHECK_RSLT
                        FROM (SELECT STDR_YY , COUNT(*) AS STND_CNT 
                                FROM TIBERO.TSC_FORST_ORG_CRTCT_INFO
                              GROUP BY STDR_YY) AS STND LEFT JOIN (
                              SELECT STDR_YY, COUNT(*) AS CPRN_CNT
                                FROM TIBERO.TSC_FORST_ORG_CRTCT_INFO
                              WHERE POPUL_CNT IS NOT NULL AND HOHOLD_CNT IS NOT NULL
                                AND RESI_CNT IS NOT NULL AND BUGE_AMT IS NOT NULL
                                AND REGION_CD IS NOT NULL AND ADMDSTRC_CD IS NOT NULL
                                AND FULL_SQUARE IS NOT NULL AND ROAD_SQUARE IS NOT NULL
                                AND CULT_SQUARE IS NOT NULL AND FORE_SQUARE IS NOT NULL
                                AND RIVER_SQUARE IS NOT NULL AND COAST_ADJ_AT IS NOT NULL
                                AND INLAND_CTY_AT IS NOT NULL AND PBORD_AMT IS NOT NULL
                                AND TRSPT_AMT IS NOT NULL AND MNCP_AMT IS NOT NULL
                                AND LCPB_AMT IS NOT NULL AND COAST_LEN IS NOT NULL
                                AND GVN_MNG_SQUARE IS NOT NULL AND GVN_SPRT_SQUARE IS NOT NULL
                                AND ETC_SQUARE IS NOT NULL AND DMG_STORM_FLOOD_AMT IS NOT NULL
                              GROUP BY STDR_YY) AS CPRN
                     ON STND.STDR_YY=CPRN.STDR_YY
        )
        SELECT 
                STDR_YY,
                ORG_CD,
                POPUL_CNT,
                HOHOLD_CNT,
                RESI_CNT,
                BUGE_AMT,
                REGION_CD,
                ADMDSTRC_CD,
                FULL_SQUARE,
                ROAD_SQUARE,
                CULT_SQUARE,
                FORE_SQUARE,
                RIVER_SQUARE,
                COAST_ADJ_AT,
                INLAND_CTY_AT,
                PBORD_AMT,
                TRSPT_AMT,
                MNCP_AMT,
                LCPB_AMT,
                COAST_LEN,
                GVN_MNG_SQUARE,
                GVN_SPRT_SQUARE,
                ETC_SQUARE,
                DMG_STORM_FLOOD_AMT
            FROM TIBERO.TSC_FORST_ORG_CRTCT_INFO
         WHERE STDR_YY IN (SELECT ANALY_PERIOD.STDR_YY
                                FROM ANALY_PERIOD JOIN CONSIS_CHECK
                            ON ANALY_PERIOD.STDR_YY=CONSIS_CHECK.STDR_YY
                            WHERE CHECK_RSLT='Y'
                          )
        """

        result = session.select(select_query, param)

        return pd.DataFrame(data=result['data'], columns=result['columns'])

    def execute(self, session: AbstractSession, data: pd.DataFrame) -> bool:
        """
        Data Source에 대한 CUD를 실행
        :param session: AbstractSession 인스턴스
        :param data: CUD 대상 데이터
        :return: True/False
        """
        delete_query = """
        DELETE FROM TIBERO.TSC_FORST_ORG_GROUP
        WHERE STDR_YY = ?
          AND ORG_CD  = ?
          AND ORG_GROUP_ID = ?
        """
        insert_query = """
        INSERT INTO TIBERO.TSC_FORST_ORG_GROUP 
        (STDR_YY, ORG_CD, ORG_GROUP_ID, FORST_AT, CRTR_ID, LAST_MODUSR_ID, CREAT_DT, LAST_MODF_DT) 
        VALUES 
        (?,?,?,?,?,?,?,?)
        """
        delete_data = data[['STDR_YY', 'ORG_CD', 'ORG_GROUP_ID']].drop_duplicates().values.tolist()
        insert_data = data.values.tolist()
        try:
            session.execute(delete_query, delete_data)
            session.execute(insert_query, insert_data)
            return True
        except Warning:
            return False
