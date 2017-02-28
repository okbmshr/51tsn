from abc import ABCMeta, abstractmethod

class Executor(metaclass = ABCMeta):
    @abstractmethod
    def convert(self, logger_body):
        pass
