import jaydebeapi as jp
import pandas as pd

from m4.dao.AbstractDataSource import AbstractDataSource
from m4.dao.AbstractSession import AbstractSession
from m4.dao.DataSourceError import DataSourceError
from m4.ApplicationConfiguration import ApplicationConfiguration


class TiberoSqlSession(AbstractSession):
    """
    Tibero Sql Session 클래스
    """
    # 세션을 생성한 Data Source
    _data_source: AbstractDataSource = None

    # Tibero connection information
    _connection: jp.Connection = None
    _cursor: jp.Cursor = None

    def __init__(self):
        """
        생성자 : SqlSession
        """

    def init(self, data_source: AbstractDataSource, conn_ref: list):
        connection: jp.Connection = conn_ref[0]

        self._data_source = data_source
        if connection:
            self._connection = connection
        else:
            connection = jp.connect(data_source._jclassname, data_source._url, data_source._driver_args, data_source._jars)
            self._connection = connection
            self._cursor = self._connection.cursor()

    def get_connection(self):
        """
        Data Source Connection 객체를 반환
        """
        return self._connection

    def commit(self):
        """
        commit
        """
        self._connection.commit()

    def rollback(self):
        """
        rollback
        """
        self._connection.rollback()

    def close(self):
        """
        생성된 세션을 반환, Data Source 접속을 해제하는 처리
        :return: void
        """
        if self._cursor:
            self._cursor.close()
        if self._connection:
            self._connection.close()

    def select(self, sql: str, params) -> None:
        """
        Data Source로부터 Query문 결과 Array를 가져오는 처리
        :param sql: sql string
        :param params: sql 파라미터
        :return: {"columns" : columns, "data" : list}
        """
        if self._connection is None:
            raise DataSourceError('Data Source session is not initialized')

        if params is None:
            params = {}

        # try:
        cursor = self._connection.cursor()
        cursor.execute(sql, params)
        columns = [d[0] for d in cursor.description]
        result = cursor.fetchall()

        return {"columns": columns, "data": result}
        # except jp.DatabaseError as e:
        #     error, = e.args
        #     error_code = error.code
        #     raise DataSourceError("Tibero database select Error", e, error_code)

    def execute(self, sql_template: str, data_list: list) -> None:
        """
        CRUD 쿼리문을 실행하는 처리
        :param sql_template: sql template
        :param data_list:  CUD 대상 데이터
        :return: True/False : 성공 여부
        """
        if self._connection is None:
            raise DataSourceError('Data Source session is not initialized')

        try:
            cursor = self._connection.cursor()
            cursor.executemany(sql_template, data_list)
            self._connection.commit()
            return True
        except jp.DatabaseError as e:
            error, = e.args
            error_code = error.code
            print("Row", cursor.rowcount, "has error", error.message)
            raise DataSourceError("Tibero database execute Error", error_code)

    def execute_procedure(self, procedure_name: str, *variables) -> None:
        """
        DB 에 저장된 프로시져를 호출하는 처리
        :param procedure_name: procedure name
        :param params: procedure 파라미터
        :return: True/False : 성공 여부
       """
        if len(variables) != 0:
            _variables = ','.join(['?' for i in range(len(variables))])
            procedure = procedure_name + _variables
        else:
            procedure = procedure_name

        if self._connection is None:
            raise DataSourceError('Data Source session is not initialized')

        try:
            cursor = self._connection.cursor()
            cursor.excute(f'call {procedure}', variables)
            self._connection.commit()
            return True
        except jp.DatabaseError as e:
            error, = e.args
            error_code = error.code
            print(f"{procedure_name} has error {error.message}")
            raise DataSourceError("Tibero database call procedure Error", error_code)


if __name__ == '__main__':
    print("Tibero connecting test start")

    # db connect info
    config: ApplicationConfiguration = ApplicationConfiguration.instance()
    config.init('m4.properties')
    config.parsing_properties()

    # DB Connecting
    test_session = TiberoSqlSession()
    test_session.init()

    # Select test
    result = test_session.select("SELECT ID, NAME FROM TIBERO.TMP")
    print(result)
    print(pd.DataFrame(result['data'], columns=result['columns']))

    # Insert test
    # test_session.execute("INSERT INTO TIBERO.TMP (ID, NAME) VALUES (?, ?)", [(3, 'insert test')])

    # close
    test_session.close()
