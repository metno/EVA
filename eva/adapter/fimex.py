import os

from datetime import timedelta

import eva
import eva.base.adapter
import eva.job
import eva.exceptions
import eva.template

import productstatus.api


class FimexAdapter(eva.base.adapter.BaseAdapter):
    """
    This adapter is a generic interface to FIMEX, that will accept virtually
    any parameter known to FIMEX.

    For flexibility, this adapter only takes two configuration options, that
    will allow users to set up any type of FIMEX job:

      * A generic command-line option string.
      * An output file name pattern.
      * (and, implicitly, the input filename).

    .. table::

       ===========================  ==============  ==============  ==========  ===========
       Variable                     Type            Default         Inclusion   Description
       ===========================  ==============  ==============  ==========  ===========
       fimex_parameters             |string|        (empty)         required    FIMEX command-line parameters.
       input_data_format                                            required    See |input_data_format|.
       input_product                                                required    See |input_product|.
       input_service_backend                                        required    See |input_service_backend|.
       output_filename_pattern                                      required    See |output_filename_pattern|.
       ===========================  ==============  ==============  ==========  ===========
    """

    CONFIG = {
        'fimex_parameters': {
            'type': 'string',
            'default': '',
        }
    }

    REQUIRED_CONFIG = [
        'fimex_parameters',
        'input_data_format',
        'input_product',
        'input_service_backend',
        'output_filename_pattern',
    ]

    OPTIONAL_CONFIG = [
        'input_partial',
        'output_base_url',
        'output_data_format',
        'output_lifetime',
        'output_product',
        'output_service_backend',
    ]

    PRODUCTSTATUS_REQUIRED_CONFIG = [
        'output_base_url',
        'output_data_format',
        'output_product',
        'output_service_backend',
    ]

    def adapter_init(self):
        self.fimex_parameters = self.template.from_string(self.env['fimex_parameters'])
        self.output_filename = self.template.from_string(self.env['output_filename_pattern'])

    def create_job(self, job):
        """!
        @brief Create a generic FIMEX job.
        """
        job.input_filename = eva.url_to_filename(job.resource.url)
        job.reference_time = job.resource.data.productinstance.reference_time
        met_timeformat = "%Y%m%dT%HZ"
        job.template_variables = {
            'datainstance': job.resource,
            'input_filename': os.path.basename(job.input_filename),
            'reference_time': job.reference_time.strftime(met_timeformat),
            'valid_at_time': (job.reference_time + timedelta(hours=1)).strftime(met_timeformat),
        }

        # Render the Jinja2 templates and report any errors
        try:
            params = self.fimex_parameters.render(**job.template_variables)
            job.output_filename = self.output_filename.render(**job.template_variables)
        except Exception as e:
            raise eva.exceptions.InvalidConfigurationException(e)

        # Generate Fimex job
        job.command = ["time fimex --input.file '%(input.file)s' --output.file '%(output.file)s' %(params)s" % {
            'input.file': job.input_filename,
            'output.file': job.output_filename,
            'params': params,
        }]

    def finish_job(self, job):
        """!
        @brief Retry on failures.
        """
        if not job.complete():
            raise eva.exceptions.RetryException(
                "Fimex conversion of '%s' to '%s' failed." % (job.input_filename, job.output_filename)
            )

    def generate_resources(self, job, resources):
        """!
        @brief Generate a set of Productstatus resources based on job output.

        The adapter will try to re-use the existing ProductInstance if the
        input and output products are the same. Otherwise, it will create a new
        ProductInstance. Existing Data and DataInstance objects are re-used.
        """
        # Generate ProductInstance resource
        parameters = {
            'product': self.output_product,
            'reference_time': job.resource.data.productinstance.reference_time,
        }
        if self.output_product == job.resource.data.productinstance.product:
            parameters['version'] = job.resource.data.productinstance.version
        product_instance = productstatus.api.EvaluatedResource(self.api.productinstance.find_or_create_ephemeral, parameters)
        resources['productinstance'] += [product_instance]

        # Generate Data resource
        data = productstatus.api.EvaluatedResource(
            self.api.data.find_or_create_ephemeral, {
                'productinstance': product_instance,
                'time_period_begin': job.resource.data.time_period_begin,
                'time_period_end': job.resource.data.time_period_end,
            }
        )
        resources['data'] = [data]

        # Generate DataInstance resource
        datainstance = productstatus.api.EvaluatedResource(
            self.api.datainstance.find_or_create_ephemeral, {
                'data': data,
                'expires': self.expiry_from_lifetime(),
                'format': self.output_data_format,
                'servicebackend': self.output_service_backend,
                'url': os.path.join(self.env['output_base_url'], os.path.basename(job.output_filename)),
            }
        )
        resources['datainstance'] = [datainstance]
