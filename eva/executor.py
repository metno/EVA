import logging
import tempfile
import os
import subprocess

import eva
import eva.job


class BaseExecutor(eva.ConfigurableObject):
    """
    @brief Abstract base class for execution engines.
    """

    def __init__(self, environment_variables):
        self.env = environment_variables
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


class NullExecutor(BaseExecutor):
    """
    @brief Pretend to execute tasks, but don't actually do it.
    """

    def execute(self, job):
        logging.info("[%s] Faking job execution and setting exit code to zero.", job.id)
        job.exit_code = 0
        job.stdout = []
        job.stderr = []


class ShellExecutor(BaseExecutor):
    """
    @brief Execute tasks in a thread.
    """

    def execute(self, job):
        # Generate a temporary script file
        script = self.create_temporary_script(job.command)

        # Run the script
        logging.info("[%s] Executing job via script '%s'", job.id, script)
        proc = subprocess.Popen(
            ['sh', script],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        logging.info("[%s] Script started with pid %d, waiting for process to finish...", job.id, proc.pid)
        job.set_status(eva.job.STARTED)
        stdout, stderr = proc.communicate()

        # Log script status, stdout and stderr
        job.exit_code = proc.returncode
        job.stdout = eva.executor.get_std_lines(stdout)
        job.stderr = eva.executor.get_std_lines(stderr)
        logging.info("[%s] Script finished, exit code: %d", job.id, job.exit_code)
        eva.executor.log_stdout_stderr(job, job.stdout, job.stderr)

        if job.exit_code == 0:
            job.set_status(eva.job.COMPLETE)
        else:
            job.set_status(eva.job.FAILED)

        # Delete the temporary file
        self.delete_temporary_script(script)


def get_std_lines(std):
    """
    Return a list of lines from stderr or stdout
    """
    return std.splitlines() if std is not None else []


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
