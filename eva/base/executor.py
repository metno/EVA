import os
import tempfile

import eva


class BaseExecutor(eva.ConfigurableObject):
    """!
    @brief Abstract base class for execution engines.
    """

    def __init__(self, group_id, environment_variables, logger, zookeeper, statsd):
        self.group_id = group_id
        self.env = environment_variables
        self.logger = logger
        self.zookeeper = zookeeper
        self.statsd = statsd
        self.read_configuration()

    def execute_async(self, job):
        """!
        @brief Execute a job asynchronously. Should return immediately after
        starting the job.
        """
        raise NotImplementedError()

    def sync(self, job):
        """!
        @brief Check if a job has completed. Check the Job.status variable
        afterwards to get the Job state.
        """
        raise NotImplementedError()

    def create_temporary_script(self, content):
        """!
        @brief Generate a temporary file and fill it with the specified content.
        @param content Content of the temporary file.
        @return The full path of the temporary file.
        """
        fd, path = tempfile.mkstemp()
        os.close(fd)
        with open(path, 'w') as f:
            f.write(content)
        return path

    def delete_temporary_script(self, path):
        """!
        @brief Remove a temporary script.
        """
        return os.unlink(path)
