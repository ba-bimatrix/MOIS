import numpy as np
import pandas as pd

from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense
from tensorflow.keras.layers import TimeDistributed
from tensorflow.keras.optimizers import Adam
from sklearn.preprocessing import MinMaxScaler

from m4.common.SingletonInstance import SingletonInstance
from m4.process.algorithm.AbstractAlgorithm import AbstractAlgorithm
from m4.util.LogHandler import LogHandler


class SimpleLstm(AbstractAlgorithm, SingletonInstance):
    _logger = None

    def __init__(self):
        """
        생성자
        """
        self._logger = LogHandler.instance().get_logger()

    def get_name(self):
        """
        Algorithm 이름(코드) 반환
        :return: String
        """
        return "LSTM"

    def forecast(self, data: pd.DataFrame, forecast_period: int, validation_period: int, params: dict):
        """
        Data Source에 대한 CUD를 실행
        :param data: input_data
        :param forecast_period: forecast period
        :param validation_period: validation period
        :param params: Algorithm parameters
        :return: forecast, accuracy
        """
        dimension = params["dimension"]
        target_column = params["value_column"]
        forecast_dataset_columns = dimension + ['STAT_CD', 'FCST_CNT']
        validateion_dataset_columns = dimension + ['STAT_CD', 'RMSE']

        train_size = params["train_size"]
        window_input = params["window_input"]
        drop_rate = params["drop_rate"]
        learning_rate = params["learning_rate"]
        epochs = params["epochs"]

        # Process 1 : 입력 데이터 로딩
        self._logger.info('Process 1 : Sales Data Loading')
        input_data = data.reset_index(drop=True)
        min_max = MinMaxScaler()
        input_min_max = input_data[target_column].values.reshape(-1, 1)
        min_max.fit(input_min_max)
        input_data[target_column] = min_max.transform(input_min_max)

        # Process 2 : one-hot encoding for categorical data
        self._logger.info('Process 2 : one-hot encoding for categorical data')
        for idx in dimension:
            category = list(set(input_data[idx]))
            val = self._get_one_hot_encoding(category)
            cate_map = dict(zip(category, val))
            one_hot = input_data[idx].apply(lambda x: cate_map.get(x))

            ndf = pd.DataFrame(np.vstack(one_hot), columns=[idx + str(i) for i in range(len(category))])
            input_data = pd.concat([input_data, ndf], axis=1)

        self._logger.debug(input_data.head(5))

        # Process 3 : Data Cleansing - No cleansing
        self._logger.info('Process 3 : Data Cleansing')

        # Process 4 : LSTM/RNN hyper parameter setting
        self._logger.info('Process 4 : LSTM/RNN hyper parameter setting')
        self._logger.info('epoch number : %d' % epochs)

        # Process 5 : slicing input data into series data
        self._logger.info('Process 5 : slicing input data into series data')
        datax = []  # 입력으로 사용될 Sequence Data
        datay = []  # 출력(타켓)으로 사용

        # 독립변수 데이터 array slicing
        for name, group in input_data.groupby(dimension):
            group = group.dropna()
            x = np.array(group.loc[:, target_column:])
            self._logger.debug("dimension : " + ", ".join(name))

            y = x[:, [0]]  # 종속변수

            for i in range(0, len(y) - window_input):
                _x = x[i: i + window_input]
                _y = y[i + window_input]  # 다음 나타날 예측치(실제치 )
                datax.append(_x)  # dataX 리스트에 추가
                datay.append(_y)  # dataY 리스트에 추가

        # 학습용/테스트용 데이터 생성
        # 학습용 데이터로 사용
        mask = np.ones(len(datay), dtype=bool)
        test_index = np.random.choice(len(datay), int(len(datay) * (1 - train_size)))
        mask[test_index] = False

        # 데이터를 잘라 학습용 데이터 생성
        trainx = np.array(datax)[mask]
        trainy = np.array(datay)[mask]

        # 데이터를 잘라 테스트용 데이터 생성
        testx = np.array(datax)[~mask]
        testy = np.array(datay)[~mask]

        # Define Stacked LSTM model
        self._logger.info('Process 6 : Create LSTM model')
        n_features = trainx.shape[-1]

        model = Sequential()
        model.add(LSTM(32, activation='relu', return_sequences=True, input_shape=(window_input, n_features)))
        model.add(LSTM(16, activation='relu', return_sequences=True))
        model.add(TimeDistributed(Dense(1)))
        optimizer = Adam(learning_rate=learning_rate)
        model.compile(optimizer=optimizer, loss='mean_squared_error', metrics=['mse'])
        model.summary()

        # # Train
        history = model.fit(trainx, trainy, epochs=epochs, verbose=0)
        eval_test = model.evaluate(testx, testy)

        # # Predict
        self._logger.info('Process 7 : predict and calculate accuracy')
        fcst_dataset = pd.DataFrame(columns=forecast_dataset_columns)
        accu_dataset = pd.DataFrame(columns=validateion_dataset_columns)

        for name, splict in input_data.groupby(dimension):
            predicted = []
            group = splict.dropna()
            x = np.array(group.loc[:, target_column:])
            y = x[:, [0]]

            recent_data = x[-window_input:]
            for i in range(0, forecast_period):
                predict_data = recent_data.reshape((1, recent_data.shape[0], recent_data.shape[1]))
                prediction = model.predict(predict_data)
                prediction = prediction[:, 0, :]

                predict_val = min_max.inverse_transform(prediction)  # Inverce transformation
                predicted.append(predict_val.flatten()[0])

                recent_data = np.roll(recent_data, shift=-1, axis=0)
                recent_data[-1, 0] = predict_val.flatten()[0]

            testx = []
            testy = []
            end = len(y) - window_input
            start = end - validation_period
            for i in range(start, end):
                _x = x[i: i + window_input]
                _y = y[i + window_input]  # 다음 나타날 예측치(실제치 )
                testx.append(_x)
                testy.append(_y)

            testx = np.array(testx)
            testy = np.array(testy)
            test_predict = model.predict(testx)
            test_predict = test_predict[:, 0, :]

            test_error = np.sqrt(np.mean(np.square(testy - test_predict)))
            test_predict_val = min_max.inverse_transform([[test_error]])  # Inverce transformation

            dim_data = list(name) if type(name) is tuple else [name]
            pred_data = list(map(lambda x: dim_data + [self.get_name(), x], predicted))
            one_fcst = pd.DataFrame(columns=forecast_dataset_columns, data=pred_data)
            one_accu = pd.DataFrame(columns=validateion_dataset_columns, data=[dim_data + [self.get_name(), test_predict_val.flatten()[0]]])

            fcst_dataset = pd.concat((fcst_dataset, one_fcst))
            accu_dataset = pd.concat((accu_dataset, one_accu))

        self._logger.debug(fcst_dataset.head(5))
        self._logger.debug(accu_dataset.head(5))

        return fcst_dataset, accu_dataset

    @classmethod
    def _get_one_hot_encoding(cls, li):
        """
        one hot encoding 반환
        :param li: 대상 list
        :return: array
        """

        values = []
        for x in range(len(li)):
            values.append(x)
        values_len = len(values)
        encoding = np.eye(values_len)[values]

        return encoding
