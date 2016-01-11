class BaseAdapter(object):
    """
    Adapters contain all the information and configuration needed to translate
    a Productstatus event into job execution.
    """

    def __init__(self, api):
        """
        @param api Productstatus API object
        """
        pass

    def match(self, event, resource):
        """
        @brief Check if the event and resource fits this adapter.
        @param event The message sent by the Productstatus server.
        @param resource The Productstatus API resource for the message.
        @returns A Job object if the message fits this adapter, else None.
        """
        raise NotImplementedError()

    def finish(self, job):
        """
        @brief Finish a job that was previously created by 'match', e.g. by updating Productstatus.
        @param job A Job object.
        """
        pass
