import logging

import eva.job


class BaseExecutor(object):
    """
    Abstract base class for execution engines.
    """

    def __init__(self, environment_variables):
        self.env = environment_variables

    def execute_async(self, adapter, job):
        """
        Execute the job asynchronously, and return immediately.
        """
        raise NotImplementedError()

    def update_status(self, adapter, job):
        """
        Check job status, and update the Job object accordingly, populating
        Job.exit_code, Job.stdout, Job.stderr, and Job.status.
        """
        raise NotImplementedError()

    def set_state(self, state):
        """
        @brief Restore state of the executor from serialization
        @param state state from serialization
        """
        pass

    def get_state(self):
        """
        @brief Get current state of the executor for serialization
        @returns state for serialization
        """
        return None


class NullExecutor(BaseExecutor):
    """
    Pretend to execute tasks, but don't actually do it.
    """

    def execute_async(self, adapter, job):
        job.status = eva.job.STARTED

    def update_status(self, adapter, job):
        job.status = eva.job.COMPLETE


def log_stdout_stderr(job, stdout, stderr):
    """
    Print stdout and stderr to syslog
    """
    logging.debug('[%s] --- Standard output ---', (job.id))
    [logging.debug(line) for line in stdout]
    logging.debug('[%s] --- End of standard output ---', (job.id))
    logging.debug('[%s] --- Standard error ---', (job.id))
    [logging.debug(line) for line in stderr]
    logging.debug('[%s] --- End of standard error ---', (job.id))


def strip_stdout_newlines(lines):
    """
    Strip newlines from an array of strings.
    """
    return [line.strip() for line in lines]


def read_and_log_stdout_stderr(job, stdout_path, stderr_path):
    with open(stdout_path, 'r') as f:
        job.stdout = strip_stdout_newlines(f.readlines())
    with open(stderr_path, 'r') as f:
        job.stderr = strip_stdout_newlines(f.readlines())
    log_stdout_stderr(job, job.stdout, job.stderr)
