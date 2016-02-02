class EvaException(Exception):
    pass


class MissingConfigurationException(EvaException):
    """
    @brief Thrown when a configuration variable is missing.
    """
    pass
