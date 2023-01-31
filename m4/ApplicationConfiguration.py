import os
import configparser
from json import loads as json_loads, dumps as json_dumps

from m4.common.SingletonInstance import SingletonInstance
from m4.dao.AbstractDataSource import AbstractDataSource
from m4.dao.AbstractSession import AbstractSession
from m4.dao.CommonCodeDAO import CommonCodeDAO


class ApplicationConfiguration(SingletonInstance):
    """
    어플리케이션 설정 정보
        1. 어플리케이션 설치 정보 : file(m4.properties)
        2. 데이터베이스 실행 설정 정보
        3. 데이터베이스 코드 정보
    """
    # properties file path : working directory path
    PROPERTIES_FILE_PATH: str = \
        os.path.dirname(
                os.path.dirname(__file__)
        )

    # 어플리케이션 설치 정보, 어플리케이션 initialize 시 db로부터 추가 설정를 add
    _config: configparser.RawConfigParser = None
    _params = None

    def __init__(self):
        self._config = configparser.RawConfigParser()
        self._config.optionxform = lambda option: option

    def init(self, properties_file, **params):
        """
        ApplicationConfiguration 초기화
        :param properties_file : 어플리케이션 설치 정보 파일명
        :param params: 분석 hyper-parameters
        """
        self._config.read(os.path.join(self.PROPERTIES_FILE_PATH, properties_file))
        self._params = params

    def parsing_properties(self) -> dict:
        sections = self._config.sections()

        params = {}
        for section in sections:
            for item in self._config[section]:
                params[item.upper()] = self._config[section][item]

        self._params = self._trans_format(params)

        return None

    def _trans_format(self, params: dict) -> dict:
        params['LIST'] = json_loads(params['LIST'])
        params['TUPLE'] = json_loads(params['TUPLE'])
        params['INT'] = json_loads(params['INT'])
        params['FLOAT'] = json_loads(params['FLOAT'])

        for element in params['LIST']:
            params[element] = json_loads(params[element])

        for element in params['TUPLE']:
            json_format = json_loads(json_dumps(json_loads(params[element])))
            _ret = []

            for n in range(len(json_format)):
                _ret.append(tuple(json_format[n]))

            params[element] = _ret

        for element in params['INT']:
            params[element] = int(params[element])

        for element in params["FLOAT"]:
            params[element] = float(params[element])

        return params

    def parameter(self, name):
        """
        섹션명, 설정명으로 설정 검색
        :param name: 설정명(코드명)
        :return: 설정값(코드값)
        """
        return self._params[name]

    def find(self, section, name):
        """
        섹션명, 설정명으로 설정 검색
        :param section: 섹션명(그룹)
        :param name: 설정명(코드명)
        :return: 설정값(코드값)
        """
        return self._config[section][name]

    def find_section(self, section):
        """
         섹션명으로 설정 검색
         :param section : 섹션명(그룹)
         :return: 섹션 내의 설정명(코드명), 설정값(코드값) 리스트
        """
        return self._config.items(section)

    def _add(self, section, items):
        """
         설정(코드) 정보 추가
         :param section : 섹션명(그룹)
         :param items : 섹션 내의 설정명(코드명), 설정값(코드값) 리스트
        """
        self._config.add_section(section)
        for item in items:
            self._config.set(section, item[0], item[1])

    def fetch_code(self, data_source: AbstractDataSource):
        """
         공통 코드 정보 설정
         self._config 객체에 section, item 생성
         :param data_source : Data Source
        """
        session: AbstractSession = data_source.get_session()

        dao: CommonCodeDAO = CommonCodeDAO.instance()
        group_codes = dao.map(dao.list_group_code(session, use_yn="Y"))
        codes = dao.map(dao.list(session, use_yn="Y"))

        for group_code in group_codes:
            section = group_code["COMN_GRP_CD"]
            items = []
            for code in codes:
                if section == code["COMN_GRP_CD"]:
                    items.append((code["COMN_CD"], code["COMN_CD_NM"]))
            self._add(section, items)

        session.close()


if __name__ == '__main__':
    test_config = ApplicationConfiguration.instance()
    test_config.init('m4.properties')
    test_config.parsing_properties()
    print(test_config._params)