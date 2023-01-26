from pandas import DataFrame


class Dataset(object):

    input_data: DataFrame = None
    organization_data: DataFrame = None
    resource_data: DataFrame = None

    pre_processing_input_data: DataFrame = None
    pre_processing_organization_data: DataFrame = None
    pre_processing_resource_data: DataFrame = None

    clustering: DataFrame = None
    recommend: DataFrame = None
    forecast: DataFrame = None
    stocking_calculation: DataFrame = None
