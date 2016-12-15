"""
RESTful API for controlling and monitoring EVA.
"""


import eva
import eva.globe
import eva.rest.resources

import falcon
import json
import wsgiref.simple_server


class RequireJSON(object):
    def process_request(self, req, resp):
        if not req.client_accepts_json:
            raise falcon.HTTPNotAcceptable('This API only supports responses encoded as JSON.')

        if req.method in ('POST', 'PUT') and req.content_length not in (None, 0):
            if 'application/json' not in req.content_type:
                raise falcon.HTTPUnsupportedMediaType('This API only supports requests encoded as JSON.')


class JSONTranslator(object):
    def process_request(self, req, resp):
        if req.content_length in (None, 0):
            return

        body = req.stream.read()
        if not body:
            raise falcon.HTTPBadRequest('Empty request body', 'A valid JSON document is required.')

        try:
            req.context['doc'] = json.loads(body.decode('utf-8'))

        except (ValueError, UnicodeDecodeError):
            raise falcon.HTTPError(
                falcon.HTTP_753,
                'Malformed JSON', 'Could not decode the request body. The JSON was incorrect or not encoded as UTF-8.',
            )

    def process_response(self, req, resp, resource):
        if 'result' not in req.context:
            return

        resp.body = json.dumps(req.context['result'])


class Server(eva.globe.GlobalMixin):
    """
    Run a HTTP REST API based on Falcon web framework.
    """

    def __init__(self, globe, host=None, port=None):
        self.set_globe(globe)
        self.app = falcon.API(middleware=[
            RequireJSON(),
            JSONTranslator(),
        ])
        self._resources = []
        self._setup_resources()
        if host is None and port is None:
            self.server = None
            return
        self.server = wsgiref.simple_server.make_server(host, port, self.app)
        self.server.timeout = 0.001

    def _setup_resources(self):
        self._add_resource('health', '/health', eva.rest.resources.HealthResource())
        self._add_resource('control', '/control/{method}', eva.rest.resources.ControlResource())
        self._add_resource('process', '/process/{method}', eva.rest.resources.ProcessResource())

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
