import os

import tensorflow as tf
from tensorflow import keras


class Predictor():

    def __init__(self):
        self.model = tf.keras.models.load_model('models/gru_model.h5')

    def predict(self, dataframe):
        return self.model.predict(dataframe.to_numpy())
