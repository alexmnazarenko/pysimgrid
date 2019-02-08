from abc import ABCMeta, abstractmethod
import numpy as np


class Estimator(metaclass=ABCMeta):

    @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    def generate(self, value):
        pass


class AccurateEstimator(Estimator):

    def __init__(self):
        super().__init__()

    def generate(self, value):
        return value


class SimpleDispersionEstimator(Estimator):

    def __init__(self, percentage, seed=1234):
        self.percentage = percentage
        np.random.seed(seed)
        super().__init__()

    def generate(self, value):
        low = max(float(value * (1. - self.percentage)), 1.0)
        high = max(float(value * (1. + self.percentage)), 1.0)
        return np.random.uniform(low, high)
