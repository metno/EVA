class EvaException(Exception):
    pass


class ShutdownException(EvaException):
    """
    @brief Thrown when EVA is sent a SIGINT or SIGTERM signal.
    """
    pass


class MissingConfigurationException(EvaException):
    """
    @brief Thrown when a configuration variable is missing.
    """
    pass


class InvalidConfigurationException(EvaException):
    """
    @brief Thrown when configuration does not make sense.
    """
    pass


class RetryException(EvaException):
    """
    @brief Thrown when a step cannot be completed due to a transient error on
    an underlying resource, typically a network or service outage.
    """
    pass
