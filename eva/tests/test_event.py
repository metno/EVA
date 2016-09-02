import unittest
import datetime

import eva
import eva.event

import productstatus.event


serialized_event = productstatus.event.Message({
    'id': '00000000-0000-0000-0000-000000000000',
    'type': 'resource',
    'uri': '/000/00/0000/00000000-0000-0000-0000-000000000000/',
    'message_timestamp': '2016-01-01T12:00:00Z',
    'message_id': '00000000-0000-0000-0000-000000000000',
    'resource': 'data',
    'url': 'https://localhost/api/v1/data/00000000-0000-0000-0000-000000000000/',
    'version': [1, 4, 0]
})


class TestEvent(unittest.TestCase):
    def setUp(self):
        pass

    def test_productstatus_event_factory(self):
        event = eva.event.ProductstatusEvent.factory(serialized_event)
        self.assertIsInstance(event, eva.event.ProductstatusEvent)
        self.assertEqual(event.id(), '00000000-0000-0000-0000-000000000000')
        self.assertEqual(event.timestamp(), eva.coerce_to_utc(datetime.datetime(2016, 1, 1, 12, 0, 0)))
