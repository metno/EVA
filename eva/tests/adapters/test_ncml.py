import eva
import eva.tests
import eva.tests.schemas
import eva.adapter
import eva.exceptions
import eva.job


class TestNcMLAggregationAdapter(eva.tests.BaseTestAdapter):
    adapter_class = eva.adapter.NcMLAggregationAdapter
    config_ini = \
"""
[adapter]
input_data_format = foo
input_product = foo
input_service_backend = foo
ncml_aggregation_input_count = 2
output_filename_pattern = /out/{{reference_time|iso8601_compact}}
"""  # NOQA
