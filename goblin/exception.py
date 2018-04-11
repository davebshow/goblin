class ConfigError(Exception):
    pass


class ClientError(Exception):
    pass


class MappingError(AttributeError):
    pass


class ValidationError(Exception):
    pass


class ElementError(Exception):
    pass


class ConfigurationError(Exception):
    pass


class GremlinServerError(Exception):
    pass


class ResponseTimeoutError(Exception):
    pass
