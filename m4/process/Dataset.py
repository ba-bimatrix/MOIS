from pandas import DataFrame


class Dataset(object):
    """
    input_data: 예측용 자원 소모 실적 데이터
    organization_data: 군집용 기관 특성 데이터
    resource_data: 비축계산 및 물품 추천용 자원 비축 실적 데이터
    user_data: 화면 추천용 사용자 사용 이력 데이터
    """
    # 원본 데이터, 쿼리 결과
    input_data: DataFrame = None
    organization_data: DataFrame = None
    resource_data: DataFrame = None
    user_data: DataFrame = None

    # 전처리 데이터
    pre_processing_input_data: DataFrame = None
    pre_processing_organization_data: DataFrame = None
    pre_processing_resource_data: DataFrame = None
    pre_processing_user_data: DataFrame = None

    # 알고리즘 결과 데이터
    clustering: DataFrame = None
    recommend: DataFrame = None
    forecast: DataFrame = None
    stocking_calculation: DataFrame = None
    user_recommend: DataFrame = None
