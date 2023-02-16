from m4.common.SingletonInstance import SingletonInstance
from m4.dao.AbstractDAO import AbstractDAO
from m4.dao.AbstractSession import AbstractSession

import pandas as pd
from m4.ApplicationConfiguration import ApplicationConfiguration


class UserDAO(AbstractDAO, SingletonInstance):
    """
    User Data Access Object
    """

    # TODO: 후에 어떤식으로 SELECT 해야하는지 협의 필요
    @staticmethod
    def read(session: AbstractSession, params: dict):
        """
        Data Source로부터 리스트 데이터를 조회
        :param session: AbstractSession 인스턴스
        :param params: 파라미터 데이터
        :return: DataFrame
        """
        if params['DATE'] == 'YYMM':
            date_format = 'YYYYMM'
        elif params['DATE'] == 'YYYY':
            date_format = 'YYYY'
        else:
            date_format = 'YYYYMM'

        select_query = f"""
        SELECT 
            TO_CHAR(CREAT_DT, '{date_format}') {params['DATE']}, USR_ID, PRGM_URL,
            SUM(LOG_PARAMTR) LOG_CNT
        FROM TIBERO.TIP_USER_LOG
        WHERE TO_CHAR(CREAT_DT, '{date_format}') BETWEEN {params['START_DT']} AND {params['END_DT']}
        GROUP BY TO_CHAR(CREAT_DT, '{date_format}'), USR_ID, PRGM_URL
        """
        result = session.select(select_query)

        return pd.DataFrame(data=result['data'], columns=result['columns'])
    
    # TODO: 후에 어떤식으로 데이터 넣을지 협의 필요
    def execute(self, session: AbstractSession, data: pd.DataFrame) -> bool:
        """
        Data Source에 대한 CUD를 실행
        :param session: AbstractSession 인스턴스
        :param data: CUD 대상 데이터
        :return: True/False
        """
        delete_query = """
        DELETE FROM TIBERO.TIM_USER_RECOMM
        WHERE STDR_DE = ?
          AND OWNER_ORG_CD = ?
          AND OWNER_DEPT_CD = ?
          AND OWNER_ID = ?
        """
        insert_query = """
        INSERT INTO TIBERO.TIM_USER_RECOMM
        (STDR_DE, OWNER_ORG_CD, OWNER_FULL_ORG_NM, OWNER_DEPT_CD, OWNER_ID, 
        ORG_GROUP_ID, RECOMM1_DASH, RECOMM2_DASH, RECOMM1_STUR, RECOMM2_STUR, RECOMM1_UNST, RECOMM2_UNST)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """
        delete_data = data[['STDR_DE', 'OWNER_ORG_CD', 'OWNER_DEPT_CD', 'OWNER_ID']].drop_duplicates().values.tolist()
        insert_data = data.values.tolist()

        try:
            session.execute(delete_query, delete_data)
            session.execute(insert_query, insert_data)
            return True
        except Warning:
            return False
