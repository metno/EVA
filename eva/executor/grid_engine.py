import os
import re
import time

import paramiko
import paramiko.ssh_exception

import eva
import eva.base.executor
import eva.job


QACCT_CHECK_INTERVAL_MSECS = 2000

SSH_RECV_BUFFER = 4096
SSH_TIMEOUT = 5
SSH_RETRY_EXCEPTIONS = (paramiko.ssh_exception.NoValidConnectionsError,
                        paramiko.ssh_exception.SSHException,
                        paramiko.ssh_exception.socket.timeout,
                        paramiko.ssh_exception.socket.error,
                        )
EXIT_OK = 0


def create_job_unique_id(group_id, job_id):
    """!
    @brief Given a EVA group_id and a job UUID, returns a valid job id for GridEngine.
    """
    return u'eva.' + re.sub(r'[^a-zA-Z0-9]', u'-', group_id).strip(u'-') + u'.' + str(job_id)


def get_job_id_from_qsub_output(output):
    """!
    Parse the JOB_ID from qsub output. Unfortunately, there is no command-line
    option to make it explicitly machine readable, so we use a regular
    expression to extract the first number instead.
    """
    matches = re.search('\d+', output)
    if not matches:
        raise TypeError('Unparseable output from qsub: expected job id, but no digits in output: %s' % output)
    return int(matches.group(0))


def get_job_id_from_qstat_output(output):
    """!
    @brief Parse the JOB_ID from qstat output using a regular expression.
    """
    regex = re.compile('^job_number:\s+(\d+)\s*$')
    for line in output.splitlines():
        matches = regex.match(line)
        if matches:
            return int(matches.group(1))
    raise RuntimeError('Could not parse job_number from qstat output, perhaps the format changed?')


def get_exit_code_from_qacct_output(output):
    """!
    @brief Parse the job exit code from qacct output using a regular expression.
    """
    regex = re.compile('^exit_status\s+(\d+)\s*$')
    for line in output.splitlines():
        matches = regex.match(line)
        if matches:
            return int(matches.group(1))
    raise RuntimeError('Could not parse exit_code from qacct output, perhaps the format changed?')


class GridEngineExecutor(eva.base.executor.BaseExecutor):
    """!
    Execute programs on Sun OpenGridEngine via an SSH connection to a submit host.
    """

    CONFIG = {
        'EVA_GRIDENGINE_QUEUE': {
            'type': 'string',
            'help': 'Which Grid Engine queue to run jobs in',
            'default': '',
        },
        'EVA_GRIDENGINE_SSH_HOST': {
            'type': 'string',
            'help': 'Hostname of the Grid Engine submit host',
            'default': '',
        },
        'EVA_GRIDENGINE_SSH_USER': {
            'type': 'string',
            'help': 'Username on the Grid Engine submit host',
            'default': '',
        },
        'EVA_GRIDENGINE_SSH_KEY_FILE': {
            'type': 'string',
            'help': 'Path to a SSH private key used for connecting to the Grid Engine submit host',
            'default': '',
        },
    }

    OPTIONAL_CONFIG = [
        'EVA_GRIDENGINE_QUEUE',
    ]

    REQUIRED_CONFIG = [
        'EVA_GRIDENGINE_SSH_HOST',
        'EVA_GRIDENGINE_SSH_USER',
        'EVA_GRIDENGINE_SSH_KEY_FILE',
    ]

    def validate_configuration(self, *args, **kwargs):
        """!
        @brief Make sure that the SSH key file exists.
        """
        super(GridEngineExecutor, self).validate_configuration(*args, **kwargs)
        if not os.access(self.env['EVA_GRIDENGINE_SSH_KEY_FILE'], os.R_OK):
            raise eva.exceptions.InvalidConfigurationException("The SSH key '%s' is not readable!" % self.env['EVA_GRIDENGINE_SSH_KEY_FILE'])

    def create_job_filename(self, *args):
        """!
        @brief Generate a unique job name that can be used as a filename.
        """
        return '.'.join(list(args))

    def ensure_ssh_connection(self, job):
        """!
        @brief Ensure that a working SSH connection exists. Throws any of
        SSH_RETRY_EXCEPTIONS if a working connection could not be established.
        """
        if hasattr(self, 'ssh_client'):
            try:
                job.logger.debug('Checking if SSH connection is usable.')
                self.ssh_client.exec_command('true')
                job.logger.debug('SSH connection seems usable, proceeding.')
                return
            except SSH_RETRY_EXCEPTIONS as e:
                job.logger.debug('SSH connection not working, trying to establish a working connection.')
        self.create_ssh_connection(job)

    def create_ssh_connection(self, job):
        """!
        @brief Open an SSH connection to the submit host, and open an SFTP channel.
        """
        job.logger.info('Creating SSH connection to %s@%s', self.env['EVA_GRIDENGINE_SSH_USER'], self.env['EVA_GRIDENGINE_SSH_HOST'])
        self.ssh_client = paramiko.SSHClient()
        self.ssh_client.set_missing_host_key_policy(paramiko.client.AutoAddPolicy())
        self.ssh_client.connect(self.env['EVA_GRIDENGINE_SSH_HOST'],
                                username=self.env['EVA_GRIDENGINE_SSH_USER'],
                                key_filename=self.env['EVA_GRIDENGINE_SSH_KEY_FILE'],
                                timeout=SSH_TIMEOUT)
        self.sftp_client = self.ssh_client.open_sftp()
        self.sftp_client.get_channel().settimeout(SSH_TIMEOUT)

    def destroy_ssh_connection(self):
        """!
        @brief Tear down the SSH connection.
        """
        self.ssh_client.close()
        del self.sftp_client
        del self.ssh_client

    def execute_ssh_command(self, command):
        """!
        @brief Execute a command remotely using a new SSH channel.
        @returns A tuple of (exit_status, stdout, stderr).
        """
        stdout = ""
        stderr = ""
        channel = self.ssh_client.get_transport().open_channel('session',
                                                               timeout=SSH_TIMEOUT)
        channel.get_pty()
        channel.exec_command(command)

        def recv_both(channel, stdout, stderr):
            if channel.recv_ready():
                stdout += channel.recv(SSH_RECV_BUFFER).decode('utf8')
            if channel.recv_stderr_ready():
                stderr += channel.recv_stderr(SSH_RECV_BUFFER).decode('utf8')
            return stdout, stderr

        while not channel.exit_status_ready():
            time.sleep(0.1)
            stdout, stderr = recv_both(channel, stdout, stderr)

        exit_status = channel.recv_exit_status()
        stdout, stderr = recv_both(channel, stdout, stderr)

        channel.close()

        return exit_status, stdout, stderr

    def execute_async(self, job):
        """!
        @brief Execute a job on Grid Engine.
        """

        skip_submit = False

        # Create SSH connection
        try:
            self.ensure_ssh_connection(job)
        except SSH_RETRY_EXCEPTIONS as e:
            raise eva.exceptions.RetryException(e)

        # Check whether a GridEngine task is already running for this job. If
        # it is, we skip submitting the job and jump right to the qacct polling.
        job.logger.info('Querying if job is already running.')
        job_id = create_job_unique_id(self.group_id, job.id)
        command = 'qstat -j %s' % job_id
        try:
            exit_code, stdout, stderr = self.execute_ssh_command(command)
            if exit_code == 0:
                job.pid = get_job_id_from_qstat_output(stdout)
                job.logger.warning('Job is already running with JOB_ID %d, will not submit a new job.', job.pid)
                skip_submit = True
            else:
                job.logger.info('Job is not running, continuing with submission.')
        except SSH_RETRY_EXCEPTIONS as e:
            raise eva.exceptions.RetryException(e)

        # Generate paths
        job.stdout_path = self.create_job_filename(job_id, 'stdout')
        job.stderr_path = self.create_job_filename(job_id, 'stderr')
        job.submit_script_path = self.create_job_filename(job_id, 'sh')

        # Skip submitting the job if it already exists
        if not skip_submit:

            # Create a submit script
            try:
                with self.sftp_client.open(job.submit_script_path, 'w') as submit_script:
                    script_content = job.command
                    submit_script.write(script_content)
            except SSH_RETRY_EXCEPTIONS as e:
                raise eva.exceptions.RetryException(e)

            # Print the job script to the log
            eva.executor.log_job_script(job)

            # Submit the job using qsub
            command = ['qsub',
                       '-N', job_id,
                       '-b', 'n',
                       '-sync', 'n',
                       '-o', job.stdout_path,
                       '-e', job.stderr_path,
                       ]

            # Run jobs in a specified queue
            if self.env['EVA_GRIDENGINE_QUEUE']:
                command += ['-q', self.env['EVA_GRIDENGINE_QUEUE']]

            command += [job.submit_script_path]

            command = ' '.join(command)
            job.logger.info('Submitting job to GridEngine: %s', command)

            # Execute command asynchronously
            try:
                exit_code, stdout, stderr = self.execute_ssh_command(command)
                if exit_code != EXIT_OK:
                    raise eva.exceptions.RetryException(
                        'Failed to submit the job to GridEngine, exit code %d' %
                        exit_code
                    )
                job.pid = get_job_id_from_qsub_output(eva.executor.get_std_lines(stdout)[0])
                job.logger.info('Job has been submitted, JOB_ID = %d', job.pid)
                job.set_status(eva.job.STARTED)
                job.set_next_poll_time(QACCT_CHECK_INTERVAL_MSECS)
            except SSH_RETRY_EXCEPTIONS as e:
                raise eva.exceptions.RetryException(e)

    def sync(self, job):
        """!
        @brief Poll Grid Engine for job completion.
        """

        # Create SSH connection
        try:
            self.ensure_ssh_connection(job)
        except SSH_RETRY_EXCEPTIONS as e:
            raise eva.exceptions.RetryException(e)

        # Poll for job completion
        check_command = 'qacct -j %d' % job.pid
        try:
            exit_code, stdout, stderr = self.execute_ssh_command(check_command)
        except SSH_RETRY_EXCEPTIONS as e:
            raise eva.exceptions.RetryException(e)
        if exit_code != EXIT_OK:
            job.logger.debug('Job has not completed yet.')
            job.set_next_poll_time(QACCT_CHECK_INTERVAL_MSECS)
            return False
        job.exit_code = get_exit_code_from_qacct_output(stdout)

        # Retrieve stdout and stderr
        try:
            with self.sftp_client.open(job.stdout_path, 'r') as f:
                job.stdout = eva.executor.strip_stdout_newlines(f.readlines())
            with self.sftp_client.open(job.stderr_path, 'r') as f:
                job.stderr = eva.executor.strip_stdout_newlines(f.readlines())
        except SSH_RETRY_EXCEPTIONS + (IOError,) as e:
            raise eva.exceptions.RetryException(
                'Unable to retrieve stdout and stderr from finished Grid Engine job.'
            )

        # Set job exit status
        if job.exit_code == EXIT_OK:
            job.set_status(eva.job.COMPLETE)
        else:
            job.set_status(eva.job.FAILED)

        # Print stdout and stderr
        eva.executor.log_stdout_stderr(job, job.stdout, job.stderr)

        # Remove job script, stdout, and stderr caches
        try:
            self.sftp_client.unlink(job.submit_script_path)
            self.sftp_client.unlink(job.stdout_path)
            self.sftp_client.unlink(job.stderr_path)
        except SSH_RETRY_EXCEPTIONS + (IOError,) as e:
            job.logger.warning('Could not remove script file, stdout and stderr')
