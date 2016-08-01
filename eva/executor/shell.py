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
        script = self.create_temporary_script(job.command)

        # Start logging
        job.logger.info("Executing job via script '%s'", script)

        # Print the job script to the log
        eva.executor.log_job_script(job)

        # Run the script
        proc = subprocess.Popen(
            ['sh', script],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        job.logger.info("Script started with pid %d, waiting for process to finish...", proc.pid)
        job.set_status(eva.job.STARTED)
        stdout, stderr = proc.communicate()

    def sync(self, job):
        # Log script status, stdout and stderr
        job.exit_code = proc.returncode
        job.stdout = eva.executor.get_std_lines(stdout)
        job.stderr = eva.executor.get_std_lines(stderr)
        job.logger.info("Script finished, exit code: %d", job.exit_code)
        eva.executor.log_stdout_stderr(job, job.stdout, job.stderr)

        if job.exit_code == 0:
            job.set_status(eva.job.COMPLETE)
        else:
            job.set_status(eva.job.FAILED)

        # Delete the temporary file
        self.delete_temporary_script(script)
