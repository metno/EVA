"""
Exceptions thrown by EVA under various circumstances.
"""


class EvaException(Exception):
    """
    Base exception for all exceptions thrown by EVA.
    """
    pass


class ShutdownException(EvaException):
    """
    Thrown when EVA is sent a `SIGINT` or `SIGTERM` signal.
    """
    pass


class ConfigurationException(EvaException):
    """
    Base class for configuration related exceptions.
    """
    pass


class MissingConfigurationException(ConfigurationException):
    """
    Thrown when a configuration variable is missing.
    """
    pass


class InvalidConfigurationException(ConfigurationException):
    """
    Thrown when configuration does not make sense.
    """
    pass


class MissingConfigurationSectionException(MissingConfigurationException):
    """
    Thrown when a configuration section is requested from a ConfigParser
    object, but does not exist.
    """
    pass


class RetryException(EvaException):
    """
    Thrown when event processing cannot be completed due to a transient error
    on an underlying resource, typically a network or service outage.
    """
    pass


class ResourceTooOldException(EvaException):
    """
    Thrown when an event is outdated because the Productstatus resource it
    refers to has been overwritten with more recent data.
    """
    pass


class JobNotGenerated(EvaException):
    """
    Thrown by an adapter when a resource event fits the processing
    criteria, but does not need processing after all.
    """
    pass


class JobNotCompleteException(EvaException):
    """
    Thrown when an operation is performed on a Job object that is not
    complete, but should be. Treat this exception as a BUG!
    """
    pass


class InvalidEventException(EvaException):
    """
    Thrown when a received event is not valid for processing.
    """
    pass


class EventTimeoutException(InvalidEventException):
    """
    Thrown when the next event did not arrive in the expected time period.
    """
    pass


class InvalidRPCException(InvalidEventException):
    """
    Thrown when an RPC call contains invalid data.
    """
    pass


class RPCWrongInstanceIDException(InvalidEventException):
    """
    Thrown when the RPC message does not match our configured EVA instance ID.
    """
    pass


class RPCInvalidRegexException(InvalidEventException):
    """
    Thrown then the instance_id in a RPC message is not a valid regular expression.
    """
    pass


class RPCException(EvaException):
    """
    Base class for RPC exceptions.
    """


class RPCFailedException(RPCException):
    """
    Thrown when an RPC call fails.
    """
    pass


class InvalidGroupIdException(EvaException):
    """
    Thrown when a group_id is incompatible with Zookeeper.
    """
    pass


class AlreadyRunningException(EvaException):
    """
    Thrown when EVA is configured to run as a single instance, but it is
    already running according to Zookeeper.
    """
    pass


class ZooKeeperDataTooLargeException(EvaException):
    """
    Thrown when trying to store a message in ZooKeeper that is too big.
    """
    pass


class DuplicateEventException(EvaException):
    """
    Thrown when trying to add an event to the event queue and it already exists.
    """
    pass
