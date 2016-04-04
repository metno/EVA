import unittest
import dateutil.tz
import datetime

import eva.template


class TestTemplate(unittest.TestCase):

    def setUp(self):
        self.dt = datetime.datetime(2016, 01, 01, 12, 30, 05, tzinfo=dateutil.tz.tzutc())
        self.environment = eva.template.Environment()

    def test_filter_iso8601(self):
        filtered = eva.template.filter_iso8601(self.dt)
        self.assertEqual(filtered, '2016-01-01T12:30:05Z')

    def test_filter_iso8601_compact(self):
        filtered = eva.template.filter_iso8601_compact(self.dt)
        self.assertEqual(filtered, '20160101T123005Z')

    def test_filter_timedelta(self):
        filtered = eva.template.filter_timedelta(self.dt, days=6, hours=6, minutes=29, seconds=55)
        self.assertEqual(filtered, datetime.datetime(2016, 01, 07, 19, 00, 00, tzinfo=dateutil.tz.tzutc()))

    def test_render_iso8601(self):
        template = self.environment.from_string('{{dt|iso8601}}')
        rendered = template.render(dt=self.dt)
        self.assertEqual(rendered, '2016-01-01T12:30:05Z')

    def test_render_iso8601_compact(self):
        template = self.environment.from_string('{{dt|iso8601_compact}}')
        rendered = template.render(dt=self.dt)
        self.assertEqual(rendered, '20160101T123005Z')

    def test_render_timedelta(self):
        template = self.environment.from_string('{{dt|timedelta(days=2)|iso8601}}')
        rendered = template.render(dt=self.dt)
        self.assertEqual(rendered, '2016-01-03T12:30:05Z')
