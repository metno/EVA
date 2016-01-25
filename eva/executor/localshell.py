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

    def generate_job_script(self, tempdir, job):
        script = job.command + ("""
EXITSTATUS=$?
echo "$EXITSTATUS" > "%s/exit_code"
""" % tempdir)
        return script

    def execute_async(self, job):
        tempdir = tempfile.mkdtemp('LocalShellExecutor')
        self.job_data[job.id] = tempdir

        scriptfile = os.path.join(tempdir, 'script')
        logging.debug("[%s] executing job via script '%s'", job.id, scriptfile)
        with open(scriptfile, 'w') as script:
            script.write(self.generate_job_script(tempdir, job))
        f_stdout = open(os.path.join(tempdir, 'stdout'), 'w')
        f_stderr = open(os.path.join(tempdir, 'stderr'), 'w')
        popen = subprocess.Popen(
            args=['sh', scriptfile],
            stdout=f_stdout,
            stderr=f_stderr,
        )
        logging.debug("[%s] script started, process pid %d", job.id, popen.pid)
        f_stdout.close()
        f_stderr.close()
        job.status = eva.job.STARTED

    def update_status(self, job):
        logging.debug("[%s] status check", job.id)
        if job.exit_code is None:
            if not job.id in self.job_data:
                logging.debug("[%s] id unknown", job.id)
                return
            tempdir = self.job_data[job.id]
            exit_file = os.path.join(tempdir, 'exit_code')
            if not os.path.isfile(exit_file):
                logging.debug("[%s] script is running", job.id)
                return

            # process finished, update job object properties
            with open(exit_file, 'r') as exit_file:
                job.exit_code = int(exit_file.readline().strip())

            logging.debug("[%s] script finished, exit status %d", job.id, job.exit_code)

            # read stdout and stderr files, ...
            stdout_path = os.path.join(tempdir, 'stdout')
            stderr_path = os.path.join(tempdir, 'stderr')
            eva.executor.read_and_log_stdout_stderr(job, stdout_path, stderr_path)
            del self.job_data[job.id]

        if job.exit_code == 0:
            job.status = eva.job.COMPLETE
        else:
            job.status = eva.job.FAILED

    def set_state(self, state):
        if state is None:
            self.job_data = {}
        else:
            self.job_data = state

    def get_state(self):
        return self.job_data
