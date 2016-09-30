import eva.tests
import eva.tests.schemas
import eva.adapter
import eva.exceptions
import eva.job

import mock


BLANK_UUID = '00000000-0000-0000-0000-000000000000'
RANDOM_UUID = 'f194279e-dfa8-45ff-ab62-1b03d89e9705'
RANDOM_USERNAME = 'nonamewrongname'
RANDOM_KEY = 'nokeywrongkey'


class TestThreddsAdapter(eva.tests.BaseTestAdapter):
    adapter_class = eva.adapter.ThreddsAdapter
    environment = {
        'EVA_INPUT_DATA_FORMAT': BLANK_UUID,
        'EVA_INPUT_PRODUCT': BLANK_UUID,
        'EVA_INPUT_SERVICE_BACKEND': BLANK_UUID,
        'EVA_OUTPUT_SERVICE_BACKEND': BLANK_UUID,
        'EVA_PRODUCTSTATUS_USERNAME': RANDOM_USERNAME,
        'EVA_PRODUCTSTATUS_API_KEY': RANDOM_KEY,
        'EVA_THREDDS_BASE_URL': 'http://bar/baz',
    }

    def test_create_job(self):
        """!
        @brief Test that the job is created correctly.
        """
        self.create_adapter()
        resource = mock.MagicMock()
        resource.url = 'file:///path/to/foo.bar'
        job = self.create_job(resource)
        self.assertEqual(job.thredds_url, 'http://bar/baz/foo.bar')
        self.assertEqual(job.thredds_html_url, 'http://bar/baz/foo.bar.html')

    def test_finish_job_ignore(self):
        """!
        @brief Test that the job is created correctly.
        """
        self.create_adapter()
        resource = mock.MagicMock()
        resource.url = 'file:///path/to/foo.bar'
        job = self.create_job(resource)
        job.set_status(eva.job.FAILED)
        self.adapter.finish_job(job)

    def test_generate_resources(self):
        """!
        @brief Test that the adapter generates correct resources for the job output.
        """
        self.create_adapter()
        resource = mock.MagicMock()
        resource.url = 'file:///path/to/foo.bar'
        job = self.create_job(resource)
        job.set_status(eva.job.COMPLETE)
        resources = self.adapter.default_resource_dictionary()
        self.adapter.generate_resources(job, resources)
        self.assertEqual(len(resources['productinstance']), 0)
        self.assertEqual(len(resources['data']), 0)
        self.assertEqual(len(resources['datainstance']), 1)
        self.assertEqual(resources['datainstance'][0].args[0]['url'], 'http://bar/baz/foo.bar')
