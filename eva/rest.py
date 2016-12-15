"""
RESTful API for controlling and monitoring EVA.
"""


import eva
import eva.globe

import datetime
import falcon
import json
import wsgiref.simple_server


class BaseResource(eva.globe.GlobalMixin):
    def set_eventloop_instance(self, eventloop):
        self.eventloop = eventloop

    def set_response_message(self, resp, message):
        resp.body = json.dumps({'message': message})


class HealthResource(BaseResource):
    """
    Accept health updates from daemon, and answer health check requests.
    """

    def __init__(self):
        self.skip_heartbeat = False
        self.heartbeat_interval = 0
        self.heartbeat_timeout = 0
        self.heartbeat_timestamp = eva.now_with_timezone()

    def ok(self):
        if self.skip_heartbeat or self.heartbeat_interval == 0:
            return True
        next_heartbeat = self.heartbeat_timestamp + datetime.timedelta(seconds=self.heartbeat_interval + self.heartbeat_timeout)
        return next_heartbeat > eva.now_with_timezone()

    def set_skip_heartbeat(self, skip):
        self.skip_heartbeat = skip

    def set_heartbeat_timeout(self, timeout):
        self.heartbeat_timeout = int(timeout)

    def set_heartbeat_interval(self, interval):
        self.heartbeat_interval = int(interval)

    def heartbeat(self, timestamp):
        self.heartbeat_timestamp = timestamp

    def on_get(self, req, resp):
        if self.ok():
            resp.status = falcon.HTTP_200
            self.set_response_message(resp, 'Last heartbeat was received %s' % str(self.heartbeat_timestamp))
        else:
            resp.status = falcon.HTTP_503
            self.set_response_message(resp, 'Last heartbeat was received %s; over age threshold of %d seconds' % (str(self.heartbeat_timestamp), self.heartbeat_interval))


class ControlResource(BaseResource):
    """
    Accept requests to control program execution.
    """

    def shutdown(self, req, resp):
        self.eventloop.shutdown()
        self.set_response_message(resp, 'Shutting down immediately.')

    def drain(self, req, resp):
        self.eventloop.set_drain()
        self.set_response_message(resp, 'Drain has been enabled.')

    def on_post(self, req, resp, method=None):
        if method in ['shutdown', 'drain']:
            func = getattr(self, method)
            return func(req, resp)
        resp.status = falcon.HTTP_404


class Server(eva.globe.GlobalMixin):
    """
    Run a HTTP REST API based on Falcon web framework.
    """

    def __init__(self, globe, host=None, port=None):
        self.set_globe(globe)
        self.app = falcon.API()
        self._resources = []
        self._setup_resources()
        if host is None and port is None:
            self.server = None
            return
        self.server = wsgiref.simple_server.make_server(host, port, self.app)
        self.server.timeout = 0.001

    def _setup_resources(self):
        self._add_resource('health', '/health', HealthResource())
        self._add_resource('control', '/control/{method}', ControlResource())

    def _add_resource(self, name, path, resource):
        self._resources += [name]
        setattr(self, name, resource)
        resource.set_globe(self.globe)
        self.app.add_route(path, resource)

    def set_eventloop_instance(self, eventloop):
        for resource in self._resources:
            instance = getattr(self, resource)
            instance.set_eventloop_instance(eventloop)

    def respond_to_next_request(self):
        if not self.server:
            return
        self.server.handle_request()
