import os
import re
import time
import dateutil.parser

import paramiko
import paramiko.agent
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
        raise eva.exceptions.GridEngineParseException('Unparseable output from qsub: expected job id, but no digits in output: %s' % output)
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
    raise eva.exceptions.GridEngineParseException('Could not parse job_number from qstat output.')


def get_exit_code_from_qacct_output(output):
    """!
    @brief Parse the job exit code from qacct output using a regular expression.
    """
    regex = re.compile('^exit_status\s+(\d+)\s*$')
    for line in output.splitlines():
        matches = regex.match(line)
        if matches:
            return int(matches.group(1))
    raise eva.exceptions.GridEngineParseException('Could not parse exit_code from qacct output.')


def parse_qacct_metrics(stdout_lines):
    """!
    @brief Given a list of qacct standard output, return a dictionary of
    metric numbers and tags.
    """
    metrics = {}
    tags = {}
    parsed = {}

    base_regex = re.compile('^([\w_]+)\s+(.+)$')

    for line in stdout_lines:
        matches = base_regex.match(line)
        if not matches:
            continue
        parsed[matches.group(1)] = matches.group(2).strip()

    for key in ['qsub_time', 'start_time', 'end_time']:
        if key in parsed:
            try:
                parsed[key] = dateutil.parser.parse(parsed[key])
            except:
                pass

    if 'start_time' in parsed:
        if 'end_time' in parsed:
            metrics['eva_grid_engine_run_time'] = (parsed['end_time'] - parsed['start_time']).total_seconds() * 1000
        if 'qsub_time' in parsed:
            metrics['eva_grid_engine_qsub_delay'] = (parsed['start_time'] - parsed['qsub_time']).total_seconds() * 1000

    for key in ['ru_utime', 'ru_stime']:
        if key in parsed:
            metrics['eva_grid_engine_' + key] = int(float(parsed[key]) * 1000)

    for key in ['qname', 'hostname']:
        if key in parsed:
            tags['grid_engine_' + key] = parsed[key]

    return {
        'metrics': metrics,
        'tags': tags,
    }


class JobNotFinishedException(eva.exceptions.EvaException):
    pass


class GridEngineExecutor(eva.base.executor.BaseExecutor):
    """!
    Execute programs on Sun OpenGridEngine via an SSH connection to a submit host.
    """

    CONFIG = {
        'qacct_command': {
            'type': 'string',
            'help': 'How to call the qacct program to get finished job information',
            'default': 'qacct -j {{job_id}}',
        },
        'queue': {
            'type': 'string',
            'help': 'Which Grid Engine queue to run jobs in',
            'default': '',
        },
        'ssh_host': {
            'type': 'string',
            'help': 'Hostname of the Grid Engine submit host',
            'default': '',
        },
        'ssh_user': {
            'type': 'string',
            'help': 'Username on the Grid Engine submit host',
            'default': '',
        },
        'ssh_key_file': {
            'type': 'string',
            'help': 'Path to a SSH private key used for connecting to the Grid Engine submit host',
            'default': '',
        },
    }

    OPTIONAL_CONFIG = [
        'qacct_command',
        'queue',
    ]

    REQUIRED_CONFIG = [
        'ssh_host',
        'ssh_user',
        'ssh_key_file',
    ]

    def init(self):
        """!
        @brief Initialize the class.
        """
        # create command-line template for qacct.
        self.qacct_command_template = self.template.from_string(self.env['qacct_command'])

    def validate_configuration(self, *args, **kwargs):
        """!
        @brief Make sure that the SSH key file exists.
        """
        super(GridEngineExecutor, self).validate_configuration(*args, **kwargs)
        if not os.access(self.env['ssh_key_file'], os.R_OK):
            raise eva.exceptions.InvalidConfigurationException("The SSH key '%s' is not readable!" % self.env['ssh_key_file'])

    def create_qacct_command(self, job_id):
        """!
        @brief Return a string with a qacct command that should be used to
        check the status of a GridEngine job.
        """
        return self.qacct_command_template.render({'job_id': job_id})

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
            except Exception as e:
                job.logger.debug('SSH connection not working, trying to establish a working connection: %s', e)
        self.create_ssh_connection(job)

    def create_ssh_connection(self, job):
        """!
        @brief Open an SSH connection to the submit host, and open an SFTP channel.
        """
        job.logger.info('Creating SSH connection to %s@%s', self.env['ssh_user'], self.env['ssh_host'])
        self.ssh_client = paramiko.SSHClient()
        self.ssh_client.set_missing_host_key_policy(paramiko.client.AutoAddPolicy())
        self.ssh_client.connect(self.env['ssh_host'],
                                username=self.env['ssh_user'],
                                key_filename=self.env['ssh_key_file'],
                                timeout=SSH_TIMEOUT)
        session = self.ssh_client.get_transport().open_session()
        paramiko.agent.AgentRequestHandler(session)

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

        # Create SSH connection
        try:
            self.ensure_ssh_connection(job)
        except SSH_RETRY_EXCEPTIONS as e:
            raise eva.exceptions.RetryException(e)

        # Generate a GridEngine job name for this job
        job.job_name = create_job_unique_id(self.group_id, job.id)

        # Generate paths
        job.stdout_path = self.create_job_filename(job.job_name, 'stdout')
        job.stderr_path = self.create_job_filename(job.job_name, 'stderr')
        job.submit_script_path = self.create_job_filename(job.job_name, 'sh')

        # Check whether a GridEngine task is already running for this job. If
        # it is, we skip submitting the job and jump right to the qacct polling.
        job.logger.info('Querying if job is already running.')
        command = 'qstat -j %s' % job.job_name
        try:
            if job.pid is None:
                exit_code, stdout, stderr = self.execute_ssh_command(command)
                if exit_code == 0:
                    job.pid = get_job_id_from_qstat_output(stdout)
            if job.pid is not None:
                job.logger.warning('Job is already running with JOB_ID %d, will not submit a new job.', job.pid)
                job.set_status(eva.job.RUNNING)
                return
            else:
                job.logger.info('Job is not running, continuing with submission.')
        except SSH_RETRY_EXCEPTIONS as e:
            raise eva.exceptions.RetryException(e)

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
                   '-N', job.job_name,
                   '-b', 'n',
                   '-sync', 'n',
                   '-o', job.stdout_path,
                   '-e', job.stderr_path,
                   ]

        # Run jobs in a specified queue
        if self.env['queue']:
            command += ['-q', self.env['queue']]

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
            job.set_status(eva.job.RUNNING)
            job.set_next_poll_time(QACCT_CHECK_INTERVAL_MSECS)
        except SSH_RETRY_EXCEPTIONS as e:
            raise eva.exceptions.RetryException(e)

    def poll_qacct_job(self, job):
        """
        Run qacct to check if a job has completed.

        :param eva.job.Job job: the Job object to check.
        :raises JobNotFinishedException: when the job is not present in qacct output.
        :rtype: tuple
        :returns: Tuple of ``(exit_code, stdout, stderr)``. Note that the return values are those of the qacct poll command, and not the job submitted via qsub.
        """
        check_command = self.create_qacct_command(job.pid)
        job.logger.debug('Running: %s', check_command)
        exit_code, stdout, stderr = self.execute_ssh_command(check_command)
        if exit_code != EXIT_OK:
            raise JobNotFinishedException('Job %d is not present in qacct output.' % job.pid)
        return (exit_code, stdout, stderr)

    def sync(self, job):
        """!
        @brief Poll Grid Engine for job completion.
        """

        # Create SSH connection and poll for job completion
        try:
            self.ensure_ssh_connection(job)
            exit_code, stdout, stderr = self.poll_qacct_job(job)
        except SSH_RETRY_EXCEPTIONS as e:
            raise eva.exceptions.RetryException(e)
        except JobNotFinishedException as e:
            job.logger.debug(e)
            job.set_next_poll_time(QACCT_CHECK_INTERVAL_MSECS)
            return False

        # Parse job exit code
        try:
            job.exit_code = get_exit_code_from_qacct_output(stdout)
        except eva.exceptions.GridEngineParseException as e:
            raise eva.exceptions.RetryException(
                "Error while parsing exit code: %s" % e
            )

        # Reset process ID, it will interfere with re-running
        job.pid = None

        # Submit job metrics
        stats = parse_qacct_metrics(stdout.splitlines())
        for metric, value in stats['metrics'].items():
            stats['tags']['adapter'] = job.adapter.config_id
            self.statsd.timing(metric, value, stats['tags'])

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

        # Set job status based on exit code
        if job.exit_code == EXIT_OK:
            job.set_status(eva.job.COMPLETE)
        else:
            job.set_status(eva.job.FAILED)

        # Print stdout and stderr
        eva.executor.log_stdout_stderr(job, job.stdout, job.stderr)

        # Remove temporary cache files
        self.cleanup_job_data(job)

    def abort(self, job):
        """
        Try to delete the job from GridEngine, and delete the job files.
        """
        if job.pid is None:
            job.logger.info('Job does not have a JOB_ID, no deletion from GridEngine necessary.')
            return

        command = 'qdel %d' % job.pid
        try:
            exit_code, stdout, stderr = self.execute_ssh_command(command)
        except SSH_RETRY_EXCEPTIONS + (IOError,) as e:
            raise eva.exceptions.RetryException("Unable to submit job for deletion: %s" % e)

        if exit_code == 0:
            job.logger.info('Job successfully submitted for deletion.')
        else:
            job.logger.warning('Job deletion failed with exit code %d.' % exit_code)
            job.logger.warning('Ignoring error condition. Standand output and standard error of delete command follows.')
            eva.executor.log_stdout_stderr(job, job.stdout, job.stderr)

        self.cleanup_job_data(job)

    def cleanup_job_data(self, job):
        """
        Remove job script, stdout, and stderr caches.
        """
        try:
            self.sftp_client.unlink(job.submit_script_path)
            self.sftp_client.unlink(job.stdout_path)
            self.sftp_client.unlink(job.stderr_path)
        except SSH_RETRY_EXCEPTIONS + (IOError,) as e:
            job.logger.warning('Could not remove script file, stdout and stderr: %s', e)
