"""!
Collection of all executors, for easy access when configuring EVA. Also
includes some useful function for writing executors.

_DO NOT_ import anything from here when subclassing executors!
"""

from eva.executor.null import NullExecutor
from eva.executor.shell import ShellExecutor
from eva.executor.grid_engine import GridEngineExecutor


def get_std_lines(std):
    """!
    Return a list of lines from stderr or stdout
    """
    return std.splitlines() if std is not None else []


def log_job_script(logger, job):
    """!
    Print log script to syslog
    """
    logger.info('[%s] --- Job script ---', job.id)
    [logger.info('[%s] %s', job.id, line.strip()) for line in job.command.splitlines()]
    logger.info('[%s] --- End of job script ---', job.id)


def log_stdout_stderr(logger, job, stdout, stderr):
    """!
    Print stdout and stderr to syslog
    """
    logger.info('[%s] --- Standard output ---', (job.id))
    [logger.info('[%s] %s', job.id, line) for line in stdout]
    logger.info('[%s] --- End of standard output ---', (job.id))
    logger.info('[%s] --- Standard error ---', (job.id))
    [logger.info('[%s] %s', job.id, line) for line in stderr]
    logger.info('[%s] --- End of standard error ---', (job.id))


def strip_stdout_newlines(lines):
    """!
    Strip newlines from an array of strings.
    """
    return [line.strip() for line in lines]
