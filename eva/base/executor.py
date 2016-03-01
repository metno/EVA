import os

import eva


class BaseExecutor(eva.ConfigurableObject):
    """
    @brief Abstract base class for execution engines.
    """

    def __init__(self, environment_variables, logger):
        self.env = environment_variables
        self.logger = logger
        self.validate_configuration()

    def execute(self, job):
        """
        @brief Execute a job and populate members exit_code, stdout, stderr.
        """
        raise NotImplementedError()

    def create_temporary_script(self, content):
        """
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
        """
        @brief Remove a temporary script.
        """
        return os.unlink(path)
