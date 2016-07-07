import abc


class DataType(abc.ABC):

    @abc.abstractmethod
    def validate(self):
        raise NotImplementedError

    @abc.abstractmethod
    def to_db(self, val):
        return val

    @abc.abstractmethod
    def to_ogm(self, val):
        return val


class BaseProperty:

    @property
    def data_type(self):
        raise NotImplementedError
