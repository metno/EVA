import subprocess

import eva
import eva.job
import eva.executor


class ShellExecutor(eva.base.executor.BaseExecutor):
    """!
    @brief Execute tasks locally using a shell.
    """

    def execute_async(self, job):
        """!
        @fixme This is not asynchronous! The job should ideally run in a thread or similar.
        """
        # Generate a temporary script file
        job.script = self.create_temporary_script(job.command)

        # Start logging
        job.logger.info("Executing job via script '%s'", job.script)

        # Print the job script to the log
        eva.executor.log_job_script(job)

        # Run the script
        job.proc = subprocess.Popen(
            ['sh', job.script],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        job.logger.info("Script started with pid %d, waiting for process to finish...", job.proc.pid)
        job.set_status(eva.job.STARTED)
        job.stdout, job.stderr = job.proc.communicate()

    def sync(self, job):
        # Log script status, stdout and stderr
        job.exit_code = job.proc.returncode
        job.stdout = eva.executor.get_std_lines(job.stdout)
        job.stderr = eva.executor.get_std_lines(job.stderr)
        job.logger.info("Script finished, exit code: %d", job.exit_code)
        eva.executor.log_stdout_stderr(job, job.stdout, job.stderr)

        if job.exit_code == 0:
            job.set_status(eva.job.COMPLETE)
        else:
            job.set_status(eva.job.FAILED)

        # Delete the temporary file
        self.delete_temporary_script(job.script)
