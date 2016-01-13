import os
import re
import subprocess
import tempfile
import logging

import eva.executor
import eva.job


STATE_DIRECTORY = '/var/lib/eva'

QSUB_EXIT_OK = 0
QSUB_EXIT_NO_FREE_RESOURCES = 25


def get_std_lines(std):
    """
    Return a list of lines from stderr or stdout
    """
    return std.splitlines() if std is not None else []


def get_job_id_from_qsub_output(output):
    matches = re.search('\d+', output)
    if not matches:
        raise Exception('Unparseable output from qsub: expected job id, but no digits in output: %s' % output)
    return int(matches.group(0))


def generate_job_script(job):
    return job.command


def get_output_path(*args):
    return os.path.join(STATE_DIRECTORY, *args)


class GridEngineExecutor(eva.executor.BaseExecutor):
    """
    Execute programs on Grid Engine.
    """

    def execute_async(self, job):
        with tempfile.NamedTemporaryFile(mode='rw+b') as submit_script:
            script_content = generate_job_script(job)
            submit_script.write(script_content)
            stdout_path = get_output_path('$JOB_ID.stdout')
            stderr_path = get_output_path('$JOB_ID.stderr')
            command = ['qsub', '-b', 'n',
                       '-o', stdout_path,
                       '-e', stderr_path,
                       submit_script.name,
                       ]
            logging.info('[%s] Executing: %s' % (job.id, ' '.join(command)))
            process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, stderr = process.communicate()
            exit_code = process.returncode
            logging.info('[%s] Exit status: %d' % (job.id, exit_code))
            if exit_code == QSUB_EXIT_OK:
                job.set_status(eva.job.STARTED)
                job.pid = get_job_id_from_qsub_output(get_std_lines(stdout)[0])
                logging.info('[%s] Grid Engine JOB_ID: %d' % (job.id, job.pid))
                return
            else:
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
            submit_script.seek(0)
            [logging.debug(line) for line in submit_script.readlines()]
            logging.debug('[%s] --- End of script file contents ---', (job.id))

    def update_status(self, job):
        command = ['qacct', '-j', unicode(job.pid)]
        logging.info('[%s] Executing: %s' % (job.id, ' '.join(command)))
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
        exit_code = process.returncode
        logging.info('[%s] Exit status: %d' % (job.id, exit_code))
        # FIXME
        job.set_status(eva.job.COMPLETE)
