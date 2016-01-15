import os
import re
import subprocess
import tempfile
import logging

import eva.executor
import eva.job


EXIT_OK = 0
QSUB_EXIT_NO_FREE_RESOURCES = 25


def get_std_lines(std):
    """
    Return a list of lines from stderr or stdout
    """
    return std.splitlines() if std is not None else []


def strip_stdout_newlines(lines):
    """
    Strip newlines from an array of strings.
    """
    return [line.strip() for line in lines]


def get_job_id_from_qsub_output(output):
    """
    Parse the JOB_ID from qsub output. Unfortunately, there is no command-line
    option to make it explicitly machine readable, so we use a regular
    expression to extract the first number instead.
    """
    matches = re.search('\d+', output)
    if not matches:
        raise Exception('Unparseable output from qsub: expected job id, but no digits in output: %s' % output)
    return int(matches.group(0))


def get_exit_code_from_qacct_output(output):
    """
    @brief Parse a finished job's exit code using the output from qacct.
    @returns Exit code if the job is finished, or None if it is still running.
    """
    regex = re.compile('^exit_status\s+(\d+)\s*$')
    for line in output:
        matches = regex.match(line)
        if matches:
            return int(matches.group(1))
    return None


def generate_job_script(job):
    return job.command


class GridEngineExecutor(eva.executor.BaseExecutor):
    """
    Execute programs on Grid Engine.
    """

    def get_log_output_path(self, *args):
        return os.path.join(self.env['EVA_SGE_LOG_PATH'], *args)

    def execute_async(self, job):

        # Create a temporary submit script
        fd, submit_script_path = tempfile.mkstemp()
        os.close(fd)
        with open(submit_script_path, 'w') as submit_script:
            script_content = generate_job_script(job)
            submit_script.write(script_content)

        job.stdout_path = self.get_log_output_path('$JOB_ID.stdout')
        job.stderr_path = self.get_log_output_path('$JOB_ID.stderr')

        # Submit the job using qsub
        command = ['qsub', '-b', 'n',
                   '-o', job.stdout_path,
                   '-e', job.stderr_path,
                   submit_script.name,
                   ]
        logging.info('[%s] Executing: %s' % (job.id, ' '.join(command)))
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
        exit_code = process.returncode
        logging.info('[%s] Exit status: %d' % (job.id, exit_code))

        # Job was submitted. Set paths, job id, status, and return
        if exit_code == EXIT_OK:
            job.set_status(eva.job.STARTED)
            job.pid = get_job_id_from_qsub_output(get_std_lines(stdout)[0])
            job.stdout_path = job.stdout_path.replace('$JOB_ID', unicode(job.pid))
            job.stderr_path = job.stderr_path.replace('$JOB_ID', unicode(job.pid))
            logging.info('[%s] Grid Engine JOB_ID: %d' % (job.id, job.pid))
            os.unlink(submit_script_path)
            return

        # Submitting the job failed. Do some verbose logging.
        job.set_status(eva.job.FAILED)
        if exit_code == QSUB_EXIT_NO_FREE_RESOURCES:
            logging.error('[%s] Job failed to start because there are no free Grid Engine resources at the moment. Please try again later.', (job.id))
        else:
            logging.error('[%s] Job failed to start because an unspecified error occured. Refer to Grid Engine documentation.', (job.id))
        logging.debug('[%s] --- Standard output ---', (job.id))
        [logging.debug(line) for line in get_std_lines(stdout)]
        logging.debug('[%s] --- End of standard output ---', (job.id))
        logging.debug('[%s] --- Standard error ---', (job.id))
        [logging.debug(line) for line in get_std_lines(stderr)]
        logging.debug('[%s] --- End of standard error ---', (job.id))
        logging.debug('[%s] --- Script file contents ---', (job.id))
        with open(submit_script_path, 'r') as submit_script:
            [logging.debug(line) for line in submit_script.readlines()]
        logging.debug('[%s] --- End of script file contents ---', (job.id))
        os.unlink(submit_script_path)

    def update_status(self, job):

        # Check job status using qacct
        command = ['qacct', '-j', unicode(job.pid)]
        logging.info('[%s] Executing: %s' % (job.id, ' '.join(command)))
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
        exit_code = process.returncode
        logging.info('[%s] Exit status: %d' % (job.id, exit_code))

        # Exit code non-zero means that the job is not found in the accounting database.
        # Should be checked with `qstat` whether or not it is still running.
        if exit_code != 0:
            return

        exit_code = get_exit_code_from_qacct_output(get_std_lines(stdout))
        if exit_code is None:
            return

        # Job is finished. Assign exit code, stdout and stderr to the Job object.
        logging.info('[%s] Grid Engine job %d finished with exit status %d' % (job.id, job.pid, exit_code))
        job.exit_code = exit_code
        if exit_code == EXIT_OK:
            job.set_status(eva.job.COMPLETE)
        else:
            job.set_status(eva.job.FAILED)
        with open(job.stdout_path, 'r') as f:
            job.stdout = strip_stdout_newlines(f.readlines())
        with open(job.stderr_path, 'r') as f:
            job.stderr = strip_stdout_newlines(f.readlines())

        os.unlink(job.stdout)
        os.unlink(job.stderr)
