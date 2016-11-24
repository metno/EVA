"""!
@brief Example adapter, good for starting out writing EVA jobs.

This adapter serves as a reusable example of how to write your own EVA jobs.

To run this adapter on GridEngine, you might want to configure these environment variables:

    [executor.gridengine]
    class = eva.executor.GridEngineExecutor
    ssh_host = <gridengine-login-node>
    ssh_key_file = /home/<username>/.ssh/id_rsa
    ssh_user = <username>

    [adapter.example]
    class = eva.adapter.example.ExampleAdapter
    executor = executor.gridengine
    input_data_format = netcdf
    input_product = ecmwf-atmospheric-model-bc-surface
    input_service_backend = lustre-b
    output_filename_pattern = {{reference_time|timedelta(hours=6)|iso8601_compact}}

Then, run EVA:

    python -m eva --process_data_instance <data-instance-uuid>

"""

# Import required Python modules
import eva
import eva.job
import eva.base.adapter


class ExampleAdapter(eva.base.adapter.BaseAdapter):
    """!
    An adapter that is good for demonstration purposes.
    """

    # This list defines which input parameter your adapter requires to run.
    # Failing to include any of these parameters in your configuration
    # environment will prevent EVA from starting up.
    #
    # See `eva.base.adapter.BaseAdapter` for an exhaustive list of pre-defined
    # configuration variables.
    REQUIRED_CONFIG = [

        # Define the output filename of your data process. This variable will
        # be processed by the EVA template engine later on in the script.
        'output_filename_pattern',

        # Define the input product. Products are defined in Productstatus by
        # the IT-GEO team. You will most certainly want to select one or more
        # products so that your script only processed files that have your
        # required input data.
        'input_product',

        # Define the service backend. Service backends are physical storage
        # devices such as Lustre store A or Opdata. They are defined in
        # Productstatus by the IT-GEO team.
        'input_service_backend',

        # Define the file format of our input data set. File formats are
        # defined in Productstatus by the IT-GEO team.
        'input_data_format',
    ]

    # This function is called every time a data instance (an actual data entry)
    # is received by EVA, and matches all input configuration defined in
    # REQUIRED_CONFIG.
    #
    # The function must either:
    #
    #   * Return normally.
    #
    #   * Raise `eva.exceptions.JobNotGenerated`, with a message indicating why
    #     a job was not generated. The job will not be sent to the adapter
    #     again for further processing.
    #
    #   * Raise `eva.exceptions.RetryException` with an error message. The
    #     error message will appear in the log. In production, this log will be
    #     recorded and is searchable. Processing will be delayed by a short
    #     interval, then retried.
    #
    #   * Any other exception will result in a termination of EVA.
    #
    def create_job(self, job):

        # Create a string template based on the output_filename_pattern
        # environment variable. This allows us to do string substitution and
        # filtering later on.
        output_filename_template = self.template.from_string(
            self.env['output_filename_pattern']
        )

        # Run string substitution and filtering. The template language is
        # Jinja2, and available filters can be found in the module
        # `eva.template`.
        #
        # E.g.
        #  {{reference_time|timedelta(hours=6)|iso8601_compact}}
        # when reference_time is April 14th, 2016, 06:00:00 UTC, will yield
        #  20160414T120000Z
        output_filename = output_filename_template.render(
            reference_time=job.resource.data.productinstance.reference_time,
        )

        # The Job object contains a logger object, which you can use to print
        # status or debugging information. DO NOT USE "print", the output will
        # not be recorded in the production environment.
        #
        # Please read the Python logging tutorial:
        # https://docs.python.org/2/howto/logging.html#logging-basic-tutorial
        job.logger.info('Job resource: %s', job.resource)

        # Here, you write your processing script. There are no environment
        # variables; you must insert your variables using string interpolation.
        job.command = """
#!/bin/bash
#-S /bin/bash
echo convert_my_data \
    --input '%(input)s' \
    --output '%(output)s' \
    --date '%(date)s' \
    --backend '%(backend)s'
"""

        # Interpolate variables into the processing script.
        job.command = job.command % {

            # The input filename always comes from Productstatus, and is always
            # an URL. Use `url_to_filename` to strip away the protocol.
            'input': eva.url_to_filename(job.resource.url),

            # The output filename has already been put into a variable, now we
            # just supply it to the string interpolation hash.
            'output': output_filename,

            # Our script requires the date and reference hour of the product
            # instance. This information is available from Productstatus. To
            # access it, we traverse the objects until we find the required
            # DateTime object, and then format it using strftime.
            'date': job.resource.data.productinstance.reference_time.strftime('%Y-%m-%dT%H'),

            # For example purposes, we include more metadata information here.
            # In this example, we include the name of our storage backend.
            'backend': job.resource.servicebackend.name,

        }

        # You may assign variables to the Job object that can be accessed from finish_job().
        job.output_filename = output_filename

        # Our job is ready for execution. This command will run the job on an
        # Executor object, defined in the environment variable executor. To
        # run jobs on GridEngine, use executor=eva.executor.GridEngineExecutor.
