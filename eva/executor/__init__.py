"""
Collection of all executors, for easy access when configuring EVA. Also
includes some useful function for writing executors.

_DO NOT_ import anything from here when subclassing executors!
"""

from eva.executor.null import NullExecutor
from eva.executor.shell import ShellExecutor
from eva.executor.grid_engine import GridEngineExecutor


def get_std_lines(std):
    """
    Return a list of lines from stderr or stdout
    """
    return std.splitlines() if std is not None else []


def log_stdout_stderr(job, stdout, stderr):
    """
    Print stdout and stderr to syslog
    """
    self.logger.debug('[%s] --- Standard output ---', (job.id))
    [self.logger.debug(line) for line in stdout]
    self.logger.debug('[%s] --- End of standard output ---', (job.id))
    self.logger.debug('[%s] --- Standard error ---', (job.id))
    [self.logger.debug(line) for line in stderr]
    self.logger.debug('[%s] --- End of standard error ---', (job.id))


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
