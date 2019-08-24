import numpy as np

class Model:
  DROPOUT = 0.9

  def __init__(self, input_size, layer_size, output_size):
    self.weights = [
      np.random.normal(scale=0.05, size=(input_size, layer_size)),
      np.random.normal(scale=0.05, size=(layer_size, layer_size)),
      np.random.normal(scale=0.05, size=(layer_size, output_size)),
      np.zeros((1, layer_size)),
      np.zeros((1, layer_size)),
    ]

  def predict(self, inputs):
    feed = np.dot(inputs, self.weights[0]) + self.weights[-2]
    feed = np.dot(feed, self.weights[1]) + self.weights[-1]
    decision = np.dot(feed, self.weights[2])
    return decision

  def get_weights(self):
    return self.weights

  def set_weights(self, weights):
    self.weights = weights
