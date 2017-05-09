import unittest
import datetime

import eva
import eva.event

import productstatus.event


resource_serialized_event = productstatus.event.Message({
    'id': '00ffff00-0000-0000-0000-000000000000',
    'type': 'resource',
    'uri': '/api/v1/data/00000000-0000-0000-0000-000000000000/',
    'message_timestamp': '2016-01-01T12:00:00Z',
    'message_id': '00000000-0000-0000-0000-000000000000',
    'resource': 'data',
    'url': 'https://localhost/api/v1/data/00000000-0000-0000-0000-000000000000/',
    'version': [1, 5, 0],
})


heartbeat_serialized_event = productstatus.event.Message({
    'id': '10000000-0000-0000-0000-000000000001',
    'type': 'heartbeat',
    'message_timestamp': '2016-01-05T12:00:00Z',
    'message_id': '20000000-0000-0000-0000-000000000002',
    'version': [1, 5, 0],
})


expired_serialized_event = productstatus.event.Message({
    'id': '10000000-0000-0000-0000-000000000001',
    'type': 'expired',
    'message_timestamp': '2016-01-05T12:00:00Z',
    'message_id': '20000000-0000-0000-0000-000000000002',
    'version': [1, 6, 0],
})


unrecognized_serialized_event = productstatus.event.Message({
    'id': '10000000-0000-0000-0000-000000000001',
    'type': 'this type will never be used for anything in production unless an infinite number of monkeys starts hacking',
    'message_timestamp': '2016-01-05T12:00:00Z',
    'message_id': '20000000-0000-0000-0000-000000000002',
    'version': [1, 5, 0],
})


class TestEvent(unittest.TestCase):
    def setUp(self):
        pass

    def test_productstatus_event_factory_resource(self):
        event = eva.event.ProductstatusBaseEvent.factory(resource_serialized_event)
        self.assertIsInstance(event, eva.event.ProductstatusResourceEvent)
        self.assertEqual(event.id(), '00000000-0000-0000-0000-000000000000')
        self.assertEqual(event.timestamp(), eva.coerce_to_utc(datetime.datetime(2016, 1, 1, 12, 0, 0)))
        self.assertEqual(event.protocol_version(), [1, 5, 0])

    def test_productstatus_event_factory_heartbeat(self):
        event = eva.event.ProductstatusBaseEvent.factory(heartbeat_serialized_event)
        self.assertIsInstance(event, eva.event.ProductstatusHeartbeatEvent)
        self.assertEqual(event.id(), '20000000-0000-0000-0000-000000000002')
        self.assertEqual(event.timestamp(), eva.coerce_to_utc(datetime.datetime(2016, 1, 5, 12, 0, 0)))
        self.assertEqual(event.protocol_version(), [1, 5, 0])

    def test_productstatus_event_factory_expired(self):
        event = eva.event.ProductstatusBaseEvent.factory(expired_serialized_event)
        self.assertIsInstance(event, eva.event.ProductstatusExpiredEvent)
        self.assertEqual(event.id(), '20000000-0000-0000-0000-000000000002')
        self.assertEqual(event.timestamp(), eva.coerce_to_utc(datetime.datetime(2016, 1, 5, 12, 0, 0)))
        self.assertEqual(event.protocol_version(), [1, 6, 0])

    def test_productstatus_event_factory_unrecognized(self):
        event = eva.event.ProductstatusBaseEvent.factory(unrecognized_serialized_event)
        self.assertIsInstance(event, eva.event.ProductstatusBaseEvent)
        self.assertEqual(event.id(), '20000000-0000-0000-0000-000000000002')
        self.assertEqual(event.timestamp(), eva.coerce_to_utc(datetime.datetime(2016, 1, 5, 12, 0, 0)))
        self.assertEqual(event.protocol_version(), [1, 5, 0])
