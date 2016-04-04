import subprocess

import eva
import eva.job
import eva.executor


class ShellExecutor(eva.base.executor.BaseExecutor):
    """!
    @brief Execute tasks in a thread.
    """

    def execute(self, job):
        # Generate a temporary script file
        script = self.create_temporary_script(job.command)

        # Start logging
        self.logger.info("[%s] Executing job via script '%s'", job.id, script)

        # Print the job script to the log
        eva.executor.log_job_script(self.logger, job)

        # Run the script
        proc = subprocess.Popen(
            ['sh', script],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        self.logger.info("[%s] Script started with pid %d, waiting for process to finish...", job.id, proc.pid)
        job.set_status(eva.job.STARTED)
        stdout, stderr = proc.communicate()

        # Log script status, stdout and stderr
        job.exit_code = proc.returncode
        job.stdout = eva.executor.get_std_lines(stdout)
        job.stderr = eva.executor.get_std_lines(stderr)
        self.logger.info("[%s] Script finished, exit code: %d", job.id, job.exit_code)
        eva.executor.log_stdout_stderr(self.logger, job, job.stdout, job.stderr)

        if job.exit_code == 0:
            job.set_status(eva.job.COMPLETE)
        else:
            job.set_status(eva.job.FAILED)

        # Delete the temporary file
        self.delete_temporary_script(script)
