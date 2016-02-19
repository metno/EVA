import re
import time
import logging

import paramiko

import eva.executor
import eva.job


SSH_RECV_BUFFER = 4096
NO_PID = -1
EXIT_OK = 0


def get_job_id_from_qsub_output(output):
    """
    Parse the JOB_ID from qsub output. Unfortunately, there is no command-line
    option to make it explicitly machine readable, so we use a regular
    expression to extract the first number instead.
    """
    matches = re.search('\d+', output)
    if not matches:
        raise TypeError('Unparseable output from qsub: expected job id, but no digits in output: %s' % output)
    return int(matches.group(0))


class GridEngineExecutor(eva.executor.BaseExecutor):
    """
    Execute programs on Sun OpenGridEngine via an SSH connection to a submit host.
    """

    REQUIRED_CONFIG = {
        'EVA_GRIDENGINE_SSH_HOST': 'Hostname of the Grid Engine submit host',
        'EVA_GRIDENGINE_SSH_USER': 'Username on the Grid Engine submit host',
        'EVA_GRIDENGINE_SSH_KEY_FILE': 'Path to a SSH private key used for connecting to the Grid Engine submit host',
    }

    def create_job_filename(self, job, *args):
        """
        @brief Generate a unique job name that can be used as a filename.
        """
        params = ['eva', unicode(job.id)] + list(args)
        return '.'.join(params)

    def create_ssh_connection(self):
        """
        @brief Open an SSH connection to the submit host, and open an SFTP channel.
        """
        self.ssh_client = paramiko.SSHClient()
        self.ssh_client.set_missing_host_key_policy(paramiko.client.AutoAddPolicy())
        self.ssh_client.connect(self.env['EVA_GRIDENGINE_SSH_HOST'],
                                username=self.env['EVA_GRIDENGINE_SSH_USER'],
                                key_filename=self.env['EVA_GRIDENGINE_SSH_KEY_FILE'])
        self.sftp_client = self.ssh_client.open_sftp()

    def destroy_ssh_connection(self):
        self.ssh_client.close()
        del self.sftp_client
        del self.ssh_client

    def execute_ssh_command(self, command):
        """
        @brief Execute a command remotely using a new SSH channel.
        @returns A tuple of (exit_status, stdout, stderr).
        """
        stdout = ""
        stderr = ""
        channel = self.ssh_client.get_transport().open_channel('session')
        channel.get_pty()
        channel.exec_command(command)

        def recv_both(channel, stdout, stderr):
            if channel.recv_ready():
                stdout += channel.recv(SSH_RECV_BUFFER)
            if channel.recv_stderr_ready():
                stderr += channel.recv_stderr(SSH_RECV_BUFFER)
            return stdout, stderr

        while not channel.exit_status_ready():
            time.sleep(0.1)
            stdout, stderr = recv_both(channel, stdout, stderr)

        exit_status = channel.recv_exit_status()
        stdout, stderr = recv_both(channel, stdout, stderr)

        channel.close()

        return exit_status, stdout, stderr

    def execute(self, job):
        """
        @brief Execute a job on Grid Engine.
        """

        # Create SSH connection
        self.create_ssh_connection()

        # Create a submit script
        job.submit_script_path = self.create_job_filename(job, 'sh')
        with self.sftp_client.open(job.submit_script_path, 'w') as submit_script:
            script_content = job.command
            submit_script.write(script_content)

        # Submit the job using qsub
        job.stdout_path = self.create_job_filename(job, 'stdout')
        job.stderr_path = self.create_job_filename(job, 'stderr')
        command = ['qsub',
                   '-b', 'n',
                   '-sync', 'y',
                   '-o', job.stdout_path,
                   '-e', job.stderr_path,
                   job.submit_script_path,
                   ]
        command = ' '.join(command)
        logging.info('[%s] Executing: %s' % (job.id, command))

        # Execute synchronously
        job.exit_code, stdout, stderr = self.execute_ssh_command(command)

        # Parse job ID and get stdout and stderr
        try:
            job.pid = get_job_id_from_qsub_output(eva.executor.get_std_lines(stdout)[0])
        except IndexError:
            job.pid = NO_PID
        logging.info('[%s] Grid Engine job %d finished with exit status %d' % (job.id, job.pid, job.exit_code))

        # Print stdout and stderr
        if job.pid == NO_PID:
            job.stdout = eva.executor.strip_stdout_newlines(stdout)
            job.stderr = eva.executor.strip_stdout_newlines(stderr)
        else:
            with self.sftp_client.open(job.stdout_path, 'r') as f:
                job.stdout = eva.executor.strip_stdout_newlines(f.readlines())
            with self.sftp_client.open(job.stderr_path, 'r') as f:
                job.stderr = eva.executor.strip_stdout_newlines(f.readlines())
        eva.executor.log_stdout_stderr(job, job.stdout, job.stderr)

        # Set job exit status and remove submit script and log files
        if job.exit_code == EXIT_OK:
            job.set_status(eva.job.COMPLETE)
        else:
            job.set_status(eva.job.FAILED)

        self.sftp_client.unlink(job.submit_script_path)
        if job.pid != NO_PID:
            self.sftp_client.unlink(job.stdout_path)
            self.sftp_client.unlink(job.stderr_path)

        # Close the SSH connection
        self.destroy_ssh_connection()
