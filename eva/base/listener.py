import eva.config
import eva.globe


class BaseListener(eva.config.ConfigurableObject, eva.globe.GlobalMixin):
    """!
    @brief Abstract base class for execution engines.
    """

    def set_kwargs(self, **kwargs):
        """!
        @brief Provide a set of keyword arguments as extra data for the listener.
        """
        self.kwargs = kwargs

    def setup_listener(self):
        """!
        @brief Create objects neccessary to listen for events.
        """
        raise NotImplementedError()

    def close_listener(self):
        """!
        @brief Close any connections to the event listener.
        """
        raise NotImplementedError()

    def get_next_event(self):
        """!
        @brief Get the next event that should be processed.
        """
        raise NotImplementedError()

    def acknowledge(self):
        """!
        @brief Acknowledge that the most recent event has been taken
        responsibility for.
        """
        raise NotImplementedError()
