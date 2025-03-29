from abc import ABC, abstractmethod



class BaseStep (ABC):
    def __init__ (self):
        pass



    @abstractmethod
    def run (self, data):
        pass