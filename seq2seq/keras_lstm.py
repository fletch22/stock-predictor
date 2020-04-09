import keras.layers as L
import keras.models as M

import numpy
import matplotlib.pyplot as plt
from sklearn.preprocessing import MinMaxScaler

from services.StockService import StockService
from utils import time_series_util


def get_data():
    symbol = "GOOG"
    training_set = StockService.get_symbol_df(symbol, translate_file_path_to_hdfs=False)

    # print(training_set.head())

    training_set = training_set.iloc[:, 3:5]

    training_num_columns = training_set.shape[1]

    print(f"{training_set.head()}: {training_num_columns}")

    training_set_np = training_set.values

    plt.plot(training_set_np, label='LSTM')
    plt.show()

    sc = MinMaxScaler()
    training_data = sc.fit_transform(training_set_np)

    # print(f"Transformed td: {training_data.shape[1]}")

    seq_length = 4
    x, y = time_series_util.sliding_windows(training_data, seq_length)

    return x, y

def run_lstm():
    # The inputs to the model.
    # We will create two data points, just for the example.
    # data_x = numpy.array([
    #     # Datapoint 1
    #     [
    #         # Input features at timestep 1
    #         [1, 2, 3],
    #         # Input features at timestep 2
    #         [4, 5, 6]
    #     ],
    #     # Datapoint 2
    #     [
    #         # Features at timestep 1
    #         [7, 8, 9],
    #         # Features at timestep 2
    #         [10, 11, 12]
    #     ]
    # ])

    data_x, data_y = get_data()
    print(f"data_x: {data_x.shape}")


    # The desired model outputs.
    # We will create two data points, just for the example.
    # data_y = numpy.array([
    #     # Datapoint 1
    #     # Target features at timestep 2
    #     [105, 106, 107, 108],
    #     # Datapoint 2
    #     # Target features at timestep 2
    #     [205, 206, 207, 208]
    # ])

    # Each input data point has 2 timesteps, each with 3 features.
    # So the input shape (excluding batch_size) is (2, 3), which
    # matches the shape of each data point in data_x above.
    model_input = L.Input(shape=(4, 2))

    # This RNN will return timesteps with 4 features each.
    # Because return_sequences=False, it will output 2 timesteps, each
    # with 4 features. So the output shape (excluding batch size) is
    # (2, 4), which matches the shape of each data point in data_y above.
    model_output = L.LSTM(2, return_sequences=False)(model_input)

    # Create the model.
    model = M.Model(input=model_input, output=model_output)

    # You need to pick appropriate loss/optimizers for your problem.
    # I'm just using these to make the example compile.
    model.compile('sgd', 'mean_squared_error')

    # Train
    model.fit(data_x, data_y)

if __name__ == '__main__':
    run_lstm()
