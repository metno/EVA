class EvaException(Exception):
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
