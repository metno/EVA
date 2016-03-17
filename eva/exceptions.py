class EvaException(Exception):
    pass


class ShutdownException(EvaException):
    """!
    @brief Thrown when EVA is sent a SIGINT or SIGTERM signal.
    """
    pass


class MissingConfigurationException(EvaException):
    """!
    @brief Thrown when a configuration variable is missing.
    """
    pass


class InvalidConfigurationException(EvaException):
    """!
    @brief Thrown when configuration does not make sense.
    """
    pass


class RetryException(EvaException):
    """!
    @brief Thrown when a step cannot be completed due to a transient error on
    an underlying resource, typically a network or service outage.
    """
    pass


class RPCException(EvaException):
    """!
    @brief Base class for RPC exceptions.
    """


class InvalidRPCException(RPCException):
    """!
    @brief Thrown when an RPC call contains invalid data.
    """
    pass


class RPCFailedException(RPCException):
    """!
    @brief Thrown when an RPC call fails.
    """
    pass


class RPCTimeoutException(RPCException):
    """!
    @brief Thrown when the RPC message queue has no events.
    """
    pass


class RPCWrongInstanceIDException(RPCException):
    """!
    @brief Thrown when the RPC message does not match our configured EVA instance ID.
    """
    pass


class RPCInvalidRegexException(RPCException):
    """!
    @brief Thrown then the instance_id in a RPC message is not a valid regular expression.
    """
    pass
