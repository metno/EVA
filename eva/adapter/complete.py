import datetime

import eva.base.adapter
import eva.exceptions
import eva.job


class CompleteAdapter(eva.base.adapter.BaseAdapter):
    """
    @brief Re-post fragmented datasets back to Productstatus when they are complete

    Using a combination of product, data format and backend UUID parameters,
    CompleteAdapter checks if the incoming DataInstance is a single time step
    part of a complete forecast. If the forecast is complete according to the
    Product.file_count variable, CompleteAdapter will post a new Data item and
    a corresponding DataInstance item to Productstatus, with
    Data.time_period_begin and Data.time_period_end corresponding to the
    prognosis length.
    """

    REQUIRED_CONFIG = [
        'EVA_INPUT_DATA_FORMAT_UUID',
        'EVA_INPUT_PRODUCT_UUID',
        'EVA_OUTPUT_LIFETIME',
    ]

    # How long to cache Productstatus resources retrieved earlier, in seconds
    CACHE_TTL = 7200

    def init(self):
        """
        @brief Require Productstatus credentials, and set dataset lifetime.
        """
        self.require_productstatus_credentials()
        self.lifetime = datetime.timedelta(hours=int(self.env['EVA_OUTPUT_LIFETIME']))
        self.resource_cache = {}

    def validate_prognosis_length(self, datainstance):
        """
        @brief Raise an exception if the DataInstance's
        Product.prognosis_length is not a positive integer.
        """
        if not datainstance.data.productinstance.product.prognosis_length:
            raise eva.exceptions.InvalidConfigurationException(
                "Productstatus configuration error: prognosis length is null or zero, must be > 0"
            )

    def validate_file_count(self, datainstance):
        """
        @brief Raise an exception if the DataInstance's Product.file_count is
        not a positive integer.
        """
        if not datainstance.data.productinstance.product.file_count:
            raise eva.exceptions.InvalidConfigurationException(
                "Productstatus configuration error: file count is null or zero, must be > 0"
            )

    def check_time_period_equality(self, datainstance):
        """
        @returns True if the DataInstance's Data start and end time are the same, False otherwise.
        """
        return datainstance.data.time_period_end == datainstance.data.time_period_begin

    def complete_time_period_begin(self, datainstance):
        """
        @returns The start time of a complete DataInstance.
        """
        return datainstance.data.productinstance.reference_time

    def complete_time_period_end(self, datainstance):
        """
        @returns The end time of a complete DataInstance.
        """
        return datainstance.data.productinstance.reference_time + \
            datetime.timedelta(
                hours=datainstance.data.productinstance.product.prognosis_length
            )

    def complete_datainstance_exists(self, datainstance):
        """
        @returns True if a DataInstance spanning the entire prognosis length of
        this Product already exists on the configured service backend.
        """
        datainstances = self.api.datainstance.objects.filter(
            data__time_period_begin=self.complete_time_period_begin(datainstance),
            data__time_period_end=self.complete_time_period_end(datainstance),
            format=datainstance.format,
            servicebackend=datainstance.servicebackend,
            data__productinstance=datainstance.data.productinstance,
        )
        return datainstances.count() > 0

    def get_sibling_datainstances(self, datainstance):
        """
        @returns a queryset with all DataInstance resources with the same
        product instance, data format, service backend, and URL as the supplied DataInstance.
        """
        return self.api.datainstance.objects.filter(
            data__productinstance=datainstance.data.productinstance,
            format=datainstance.format,
            servicebackend=datainstance.servicebackend,
            url=datainstance.url,
        )

    def add_cache(self, resource):
        """
        @brief Add a Productstatus Resource to a local cache. If the Resource
        already is cached, it is ignored.
        """
        if resource.resource_uri in self.resource_cache:
            return
        self.resource_cache[resource.resource_uri] = {
            'expires': datetime.datetime.now() + datetime.timedelta(seconds=self.CACHE_TTL),
            'resource': resource,
        }
        self.logger.debug('Added to cache: %s, expires %s',
                   resource,
                   self.resource_cache[resource.resource_uri]['expires'])

    def get_cache(self, id):
        """
        @brief Retrieve a Productstatus Resource from a local cache.
        """
        return self.resource_cache[id]['resource']

    def clean_cache(self):
        """
        @brief Remove expired cache entries from the local cache.
        """
        now = datetime.datetime.now()
        for key in self.resource_cache.keys():
            if self.resource_cache[key]['expires'] < now:
                self.logger.debug('Removed from cache: %s', self.resource_cache[key])
                del self.resource_cache[key]

    def cache_queryset(self, queryset):
        """
        @brief Given a Productstatus query set consisting of DataInstance
        resources, store them in a local cache.
        """
        for datainstance in queryset:
            self.add_cache(datainstance)

    def get_filtered_datainstances(self, queryset):
        """
        @returns Given a Productstatus queryset with DataInstances, returns a
        sorted list of related Data resources where time_period_begin equals
        time_period_end.
        """
        data_instances = []
        for datainstance in queryset:
            datainstance = self.get_cache(datainstance.resource_uri)
            data = datainstance.data
            if data.time_period_begin == data.time_period_end:
                data_instances.append(datainstance)
        return sorted(data_instances, key=lambda x: x.data.time_period_begin)

    def datainstance_set_has_correct_prognosis_length(self, resource, datainstances):
        """
        @brief Check that a collection of DataInstance resources spans the time
        period defined by Product.prognosis_length.
        """
        begin = datainstances[0].data.time_period_begin
        end = datainstances[-1].data.time_period_end
        total_hours = (end - begin).total_seconds() / 3600.0

        self.logger.debug("Data resources cover a total of %d hours, required is %d hours",
                      total_hours,
                      resource.data.productinstance.product.prognosis_length,
                      )

        return total_hours == resource.data.productinstance.product.prognosis_length

    def datainstance_set_has_correct_file_count(self, resource, datainstances):
        """
        @brief Check that the number of DataInstance resources is the same as
        required by Product.file_count.
        """
        file_count = len(datainstances)
        self.logger.debug("Data resources file count: %d", file_count)

        if file_count < resource.data.productinstance.product.file_count:
            raise eva.exceptions.InvalidConfigurationException(
                "Too many files in data set, this is possibly a misconfiguration of Productstatus."
            )

        return file_count == resource.data.productinstance.product.file_count

    def get_or_create_data(self, product_instance, time_period_begin, time_period_end):
        """
        @returns a Data resource for a product instance, spanning the given
        time period. If the resource already exists, it is used. Otherwise, it
        is created.
        """
        datas = self.api.data.objects.filter(
            time_period_begin=time_period_begin,
            time_period_end=time_period_end,
            productinstance=product_instance,
        )

        if datas.count() > 0:
            if datas.count() > 1:
                raise eva.exceptions.EvaException(
                    "More than one Data resource found for time period spanning from %s to %s." % (
                        time_period_begin,
                        time_period_end,
                    )
                )
            return datas[0]

        data = self.api.data.create()
        data.time_period_begin = time_period_begin
        data.time_period_end = time_period_end
        data.productinstance = product_instance
        data.save()

        return data

    def create_datainstance(self, data, source_instance):
        """
        @returns a newly created DataInstance resource of a complete dataset.
        """
        datainstance = self.api.datainstance.create()
        datainstance.expires = datetime.datetime.now() + self.lifetime
        datainstance.data = data
        datainstance.url = source_instance.url
        datainstance.format = source_instance.format
        datainstance.servicebackend = source_instance.servicebackend
        datainstance.save()
        return datainstance

    def process_resource(self, resource):
        """
        @brief post to productstatus if dataset is complete
        """

        # Product must be configured with prognosis length
        self.validate_prognosis_length(resource)

        # Product must be configured with file count
        self.validate_file_count(resource)

        # Start and end time must be equal
        if not self.check_time_period_equality(resource):
            self.logger.debug('DataInstance start and end time differs, ignoring.')
            return

        # Skip processing if already done
        if self.complete_datainstance_exists(resource):
            self.logger.debug('A complete DataInstance already exists, ignoring.')
            return

        # Get all datainstances for the product/format/servicebackend combo
        datainstances = self.get_sibling_datainstances(resource)
        self.logger.debug('Number of sibling data instances: %d', datainstances.count())

        # Clean out old Productstatus resources from the cache
        self.logger.debug('Removing expired cache entries...')
        self.clean_cache()

        # Add Productstatus resources to local cache
        self.logger.debug('Adding new data instances to cache...')
        self.cache_queryset(datainstances)

        # Get a sorted list of datainstances from the local cache that corresponds to our query set
        datainstances = self.get_filtered_datainstances(datainstances)
        self.logger.debug('Number of eligible data instances for complete data set: %d', len(datainstances))

        # Check file count
        if not self.datainstance_set_has_correct_file_count(resource, datainstances):
            return

        # Check prognosis length
        if not self.datainstance_set_has_correct_prognosis_length(resource, datainstances):
            return

        # Data set is complete; create DataInstance resource
        self.logger.info("Data set is complete: %d hours, %d files, start %s, end %s",
                         resource.data.productinstance.product.prognosis_length,
                         resource.data.productinstance.product.file_count,
                         datainstances[0].data.time_period_begin,
                         datainstances[-1].data.time_period_end)

        data = self.get_or_create_data(resource.data.productinstance,
                                       datainstances[0].data.time_period_begin,
                                       datainstances[-1].data.time_period_end)
        self.logger.info("Using Data resource: %s", data)

        datainstance = self.create_datainstance(data, resource)
        self.logger.info("Created DataInstance resource: %s", datainstance)
