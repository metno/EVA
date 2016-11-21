import eva.tests
import eva.tests.schemas
import eva.adapter
import eva.exceptions
import eva.job

import mock
import httmock


class TestDeleteAdapter(eva.tests.BaseTestAdapter):
    adapter_class = eva.adapter.DeleteAdapter
    config_ini = \
"""
[adapter]
input_service_backend = foo
"""  # NOQA

    def test_create_job(self):
        """!
        @brief Test that job creation filters for DataInstance objects.
        """
        self.create_adapter()
        resource = mock.MagicMock()
        self.create_job(resource)
        self.assertEqual(self.adapter.api.datainstance.objects.filter.call_count, 1)

    def test_finish_job_and_generate_resources(self):
        """!
        @brief Test that job finish works and doesn't throw any exceptions, and
        that generated resources has their deleted flag set to True.
        """
        self.create_adapter()
        resource = mock.MagicMock()
        job = self.create_job(resource)
        self.setup_productstatus()
        job.set_status(eva.job.COMPLETE)
        self.adapter.finish_job(job)
        job.instance_list = [mock.MagicMock(), mock.MagicMock()]
        with httmock.HTTMock(*eva.tests.schemas.SCHEMAS):
            resources = self.generate_resources(job)
        self.assertTrue(resources['datainstance'][0].deleted)
        self.assertTrue(resources['datainstance'][1].deleted)
