import eva


class BaseListener(eva.ConfigurableObject):
    """!
    @brief Abstract base class for execution engines.
    """

    def __init__(self, environment_variables, logger, **kwargs):
        self.env = environment_variables
        self.logger = logger
        self.kwargs = kwargs
        self.validate_configuration()

    def setup_listener(self):
        """!
        @brief Create objects neccessary to listen for events.
        """
        raise NotImplementedError()

    def get_next_event(self):
        """!
        @brief Get the next event that should be processed.
        """
        raise NotImplementedError()
