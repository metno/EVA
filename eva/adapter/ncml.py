import os

import eva
import eva.base.adapter
import eva.job
import eva.exceptions

import productstatus.api


XML_TEMPLATE = """
<netcdf xmlns="http://www.unidata.ucar.edu/namespaces/netcdf/ncml-2.2">
    <attribute name="title" type="string" value="%(title)s" />
    <aggregation dimName="time" type="joinExisting">
        %(children)s
    </aggregation>
</netcdf>
"""


def make_xml(title, filenames):
    """
    Create an XML file containing a list of filenames.
    """
    locations = ['<netcdf location="%s" />' % x for x in filenames]
    var = {
        'title': title,
        'children': '\n'.join(locations),
    }
    return XML_TEMPLATE % var


class NcMLAggregationAdapter(eva.base.adapter.BaseAdapter):
    """
    NcMLAggregationAdapter creates an aggregation NcML file, containing a set
    of NetCDF files belonging to the same ``ProductInstance``. The file is
    created on the same ``ServiceBackend`` as the source files.

    This adapter is triggered when ``ncml_aggregation_input_count`` matches the
    number of NetCDF files belonging to the current ``ProductInstance``,
    ``Format`` and ``ServiceBackend``.

    The generated NcML file looks similar to this:

    .. code-block:: xml

       <netcdf xmlns="http://www.unidata.ucar.edu/namespaces/netcdf/ncml-2.2">
         <attribute name="title" type="string" value="Product @ reference time" />
         <aggregation dimName="time" type="joinExisting">
           <netcdf location="file1.nc" />
           <netcdf location="file2.nc" />
         </aggregation>
       </netcdf>

    .. table::

       ===============================  ==============  ==============  ==========  ===========
       Variable                         Type            Default         Inclusion   Description
       ===============================  ==============  ==============  ==========  ===========
       input_data_format                                                required    See |input_data_format|.
       input_product                                                    required    See |input_product|.
       input_service_backend                                            required    See |input_service_backend|.
       ncml_aggregation_input_count     |positive_int|  (empty)         required    The number of input files required to generate the NcML file.
       output_data_format                                               optional    See |output_data_format|.
       output_filename_pattern                                          required    See |output_filename_pattern|.
       output_lifetime                                                  optional    See |output_lifetime|.
       ===============================  ==============  ==============  ==========  ===========
    """

    CONFIG = {
        'ncml_aggregation_input_count': {
            'type': 'int',
            'default': '',
        },
    }

    REQUIRED_CONFIG = [
        'input_data_format',
        'input_product',
        'input_service_backend',
        'ncml_aggregation_input_count',
        'output_filename_pattern',
    ]

    OPTIONAL_CONFIG = [
        'output_data_format',
        'output_lifetime',
    ]

    PRODUCTSTATUS_REQUIRED_CONFIG = [
        'output_data_format',
    ]

    def adapter_init(self):
        """
        Initialize template variables.
        """
        self.output_filename = self.template.from_string(self.env['output_filename_pattern'])

    def create_job(self, job):
        """
        Check if the number of DataInstance resources belonging to the same
        ProductInstance matches ``ncml_aggregation_input_count``. If so, create
        a new job that creates an NcML file listing all the DataInstance
        resources.
        """
        qs = self.api.datainstance.objects.filter(
            data__productinstance=job.resource.data.productinstance,
            format=job.resource.format,
            servicebackend=job.resource.servicebackend,
        )

        actual = qs.count()
        required = self.env['ncml_aggregation_input_count']

        if actual != required:
            raise eva.exceptions.JobNotGenerated("Number of input files does not match required number of files (%d against %d)" % (actual, required))

        # Render the Jinja2 templates and report any errors
        job.template_variables = {
            'datainstance': job.resource,
            'input_filename': os.path.basename(eva.url_to_filename(job.resource.url)),
            'reference_time': job.resource.data.productinstance.reference_time,
        }
        try:
            job.output_filename = self.output_filename.render(**job.template_variables)
        except Exception as e:
            raise eva.exceptions.InvalidConfigurationException(e)

        # Generate XML
        urls = [x.url for x in qs]
        paths = [eva.url_to_filename(x) for x in urls]
        title = '%s @ %s' % (job.resource.data.productinstance.product.name,
                             eva.strftime_iso8601(job.resource.data.productinstance.reference_time))
        xml = make_xml(title, paths)

        # Generate shell script
        job.command = [
            "cat > %s <<EVA_NCML_EOF" % job.output_filename,
            xml,
            "EVA_NCML_EOF",
        ]

    def finish_job(self, job):
        """
        Retry on failure, log on completion.
        """
        if not job.complete():
            raise eva.exceptions.RetryException("NcML aggregation to file '%s' failed.", job.output_filename)
        job.logger.info("NcML aggregation to file '%s' successful.", job.output_filename)

    def generate_resources(self, job, resources):
        """
        Generate a set of Productstatus resources based on job output.

        This adapter will post a new DataInstance using the same ProductInstance as the input resource.
        """
        # Generate Data resource with empty time period.
        data = productstatus.api.EvaluatedResource(
            self.api.data.find_or_create_ephemeral, {
                'productinstance': job.resource.data.productinstance,
                'time_period_begin': None,
                'time_period_end': None,
            }
        )
        resources['data'] = [data]

        # Generate DataInstance resource pointing to NcML file.
        datainstance = productstatus.api.EvaluatedResource(
            self.api.datainstance.find_or_create_ephemeral, {
                'data': data,
                'expires': self.expiry_from_lifetime(),
                'format': self.api.dataformat[self.env['output_data_format']],
                'servicebackend': job.resource.servicebackend,
                'url': 'file://' + job.output_filename,
            }
        )

        resources['datainstance'] = [datainstance]
