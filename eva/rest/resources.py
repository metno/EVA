import eva
import eva.globe
import eva.rest.resources

import productstatus.exceptions

import datetime
import falcon


class BaseResource(eva.globe.GlobalMixin):
    def set_eventloop_instance(self, eventloop):
        self.eventloop = eventloop

    def set_response_message(self, req, message):
        req.context['result'] = {'message': message}

    def exec_functions(self, req, resp, method, functions):
        if method in functions:
            func = getattr(self, method)
            return func(req, resp)
        resp.status = falcon.HTTP_404


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
            self.set_response_message(req, 'Last heartbeat was received %s' % str(self.heartbeat_timestamp))
        else:
            resp.status = falcon.HTTP_503
            self.set_response_message(req, 'Last heartbeat was received %s; over age threshold of %d seconds' % (str(self.heartbeat_timestamp), self.heartbeat_interval))


class ControlResource(BaseResource):
    """
    Accept requests to control program execution.
    """

    def shutdown(self, req, resp):
        self.eventloop.shutdown()
        self.set_response_message(req, 'Shutting down immediately.')

    def drain(self, req, resp):
        self.eventloop.set_drain()
        self.set_response_message(req, 'Drain has been enabled.')

    def on_post(self, req, resp, method=None):
        return self.exec_functions(req, resp, method, ['shutdown', 'drain'])


class ProcessResource(BaseResource):
    """
    Accept requests to add Productstatus resources to the processing queue.
    """

    def productinstance(self, req, resp):
        self.eventloop.process_all_in_product_instance(req.context['doc']['uuid'])
        self.set_response_message(req, "All DataInstances resources descended from ProductInstance UUID '%s' has been added to the event queue." % req.context['doc']['uuid'])

    def datainstance(self, req, resp):
        self.eventloop.process_data_instance(req.context['doc']['uuid'])
        self.set_response_message(req, "DataInstance UUID '%s' has been added to the event queue." % req.context['doc']['uuid'])

    def on_post(self, req, resp, method=None):
        if 'uuid' not in req.context['doc'] or not req.context['doc']['uuid']:
            raise falcon.HTTPBadRequest("Please provide the 'uuid' parameter, specifying which Productstatus resource to process.")
        try:
            self.exec_functions(req, resp, method, ['productinstance', 'datainstance'])
            resp.status = falcon.HTTP_202
        except productstatus.exceptions.NotFoundException as e:
            raise falcon.HTTPBadRequest('The Productstatus resource could not be found: %s' % e)
        except productstatus.exceptions.ServiceUnavailableException as e:
            raise falcon.HTTPServiceUnavailable('An error occurred when retrieving Productstatus resources: %s' % e)
