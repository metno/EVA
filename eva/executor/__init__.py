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
    if type(std) is str:
        return [x for x in std.splitlines()] if std is not None else []
    else:
        return [x.decode('utf-8') for x in std.splitlines()] if std is not None else []


def log_job_script(job):
    """!
    Print log script to syslog
    """
    job.logger.info('--- Job script ---')
    [job.logger.info(line.strip()) for line in job.command.splitlines()]
    job.logger.info('--- End of job script ---')


def log_stdout_stderr(job, stdout, stderr):
    """!
    Print stdout and stderr to syslog
    """
    job.logger.info('--- Standard output ---')
    [job.logger.info(line) for line in stdout]
    job.logger.info('--- End of standard output ---')
    job.logger.info('--- Standard error ---')
    [job.logger.info(line) for line in stderr]
    job.logger.info('--- End of standard error ---')


def strip_stdout_newlines(lines):
    """!
    Strip newlines from an array of strings.
    """
    return [line.strip() for line in lines]
