import logging
import os.path
import subprocess
import tempfile

import eva.executor
import eva.job


class LocalShellExecutor(eva.executor.BaseExecutor):
    def __init__(self, *args):
        super(LocalShellExecutor, self).__init__(*args)
        self.job_data = {}

    def generate_job_script(self, job):
        return job.command

    def execute_async(self, job):
        tempdir = tempfile.mkdtemp('LocalShellExecutor')
        scriptfile = os.path.join(tempdir, 'script')
        logging.debug("[%s] executing job via script '%s'", job.id, scriptfile)
        with open(scriptfile, 'w') as script:
            script.write(self.generate_job_script(job))
        f_stdout = open(os.path.join(tempdir, 'stdout'), 'w')
        f_stderr = open(os.path.join(tempdir, 'stderr'), 'w')
        popen = subprocess.Popen(
            args=['sh', scriptfile],
            stdout=f_stdout,
            stderr=f_stderr,
        )
        self.job_data[job.id] = (popen, tempdir, f_stdout, f_stderr)
        job.pid = popen.pid
        logging.debug("[%s] script started, process pid %d", job.id, job.pid)
        job.status = eva.job.STARTED

    def update_status(self, job):
        if job.exit_code is None:
            (popen, tempdir, f_stdout, f_stderr) = self.job_data[job.id]
            exit_code = popen.poll()
            if exit_code is None:
                logging.debug("[%s] script is running, pid %d", job.id, job.pid)
                return

            # process finished, update job object properties
            logging.debug("[%s] script with pid %d finished, exit status %d", job.id, job.pid, exit_code)
            job.exit_code = exit_code
            job.pid = None

            # read stdout and stderr files, ...
            f_stdout.close()
            f_stderr.close()

            stdout_path = os.path.join(tempdir, 'stdout')
            stderr_path = os.path.join(tempdir, 'stderr')
            eva.executor.read_and_log_stdout_stderr(job, stdout_path, stderr_path)

        if job.exit_code == 0:
            job.status = eva.job.COMPLETE
        else:
            job.status = eva.job.FAILED
