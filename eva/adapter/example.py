"""!
@brief Example adapter, good for starting out writing EVA jobs.

This adapter serves as a reusable example of how to write your own EVA jobs.

To run this adapter on GridEngine, you might want to configure these environment variables:

    export EVA_ADAPTER="eva.adapter.example.ExampleAdapter"
    export EVA_EXECUTOR="eva.executor.GridEngineExecutor"
    export EVA_GRIDENGINE_SSH_HOST="<gridengine-login-node>"
    export EVA_GRIDENGINE_SSH_KEY_FILE="/home/<username>/.ssh/id_rsa"
    export EVA_GRIDENGINE_SSH_USER="<username>"
    export EVA_INPUT_DATA_FORMAT_UUID="4fbc5c33-272a-4d5f-a29d-6d4c5e7e47f0"
    export EVA_INPUT_PRODUCT_UUID="7d955184-aa2e-4298-8f9c-2c4b63eae170"
    export EVA_INPUT_SERVICE_BACKEND_UUID="34615199-4941-496d-831e-1679d7b35f5a"
    export EVA_OUTPUT_FILENAME_PATTERN="{{reference_time|timedelta(hours=6)|iso8601_compact}}"

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
        'EVA_OUTPUT_FILENAME_PATTERN',

        # Define the input product. Products are defined in Productstatus by
        # the IT-GEO team. You will most certainly want to select one or more
        # products so that your script only processed files that have your
        # required input data.
        'EVA_INPUT_PRODUCT_UUID',

        # Define the service backend. Service backends are physical storage
        # devices such as Lustre store A or Opdata. They are defined in
        # Productstatus by the IT-GEO team.
        'EVA_INPUT_SERVICE_BACKEND_UUID',

        # Define the file format of our input data set. File formats are
        # defined in Productstatus by the IT-GEO team.
        'EVA_INPUT_DATA_FORMAT_UUID',
    ]

    # This function is called every time a data instance (an actual data entry)
    # is received by EVA, and matches all input configuration defined in
    # REQUIRED_CONFIG.
    #
    # You may exit this function in three ways:
    #
    # * Return normally. Return values are ignored. Returning from the function
    #   signifies completed and successful processing of the input data, and
    #   guarantees that the output data has been successfully created.
    #
    # * Throw `eva.exceptions.RetryException` with an error message. The error
    #   message will appear in the log. In production, this log will be
    #   recorded and is searchable. Processing will be delayed by a short time,
    #   then retried.
    #
    # * Any other exception will result in a termination of EVA.
    #
    def process_resource(self, message_id, resource):

        # Don't write any data to Productstatus.
        self.post_to_productstatus = False

        # Create a string template based on the EVA_OUTPUT_FILENAME_PATTERN
        # environment variable. This allows us to do string substitution and
        # filtering later on.
        output_filename_template = self.template.from_string(
            self.env['EVA_OUTPUT_FILENAME_PATTERN']
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
            reference_time=resource.data.productinstance.reference_time,
        )

        # Instantiate a Job object, required if you are going to run an
        # external process, e.g. on GridEngine.
        job = eva.job.Job(message_id, self.logger)

        # The Job object contains a logger object, which you can use to print
        # status or debugging information. DO NOT USE "print", the output will
        # not be recorded in the production environment.
        #
        # Please read the Python logging tutorial:
        # https://docs.python.org/2/howto/logging.html#logging-basic-tutorial
        job.logger.info('Job resource: %s', resource)

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
            'input': eva.url_to_filename(resource.url),

            # The output filename has already been put into a variable, now we
            # just supply it to the string interpolation hash.
            'output': output_filename,

            # Our script requires the date and reference hour of the product
            # instance. This information is available from Productstatus. To
            # access it, we traverse the objects until we find the required
            # DateTime object, and then format it using strftime.
            'date': resource.data.productinstance.reference_time.strftime('%Y-%m-%dT%H'),

            # For example purposes, we include more metadata information here.
            # In this example, we include the name of our storage backend.
            'backend': resource.servicebackend.name,

        }

        # Our job is ready for execution. This command will run the job on an
        # Executor object, defined in the environment variable EVA_EXECUTOR. To
        # run jobs on GridEngine, use EVA_EXECUTOR=eva.executor.GridEngineExecutor.
        self.execute(job)

        # Running the job populated `job.status`. You should always check this variable.
        # Throwing a RetryException will ensure that processing is retried.
        if job.status != eva.job.COMPLETE:
            raise eva.exceptions.RetryException(
                "Processing of '%s' failed." % output_filename
            )

        # We might want to register our completed data instance with Productstatus.
        # It is, however, not required. We can skip this step for now.
        if not self.post_to_productstatus:
            return

        # Here ends your responsibility. The code for registering new products
        # with Productstatus is added by IT-GEO.
        self.require_productstatus_credentials()

        # ...
