import eva
import eva.globe
import eva.job
import eva.rest.resources

import productstatus.exceptions

import datetime
import falcon


class BaseResource(eva.globe.GlobalMixin):
    def set_eventloop_instance(self, eventloop):
        self.eventloop = eventloop

    def set_response_message(self, req, message):
        self.set_result(req, {'message': message})

    def set_result(self, req, result):
        req.context['result'] = result

    def exec_functions(self, req, resp, method, functions):
        if method in functions:
            func = getattr(self, method)
            return func(req, resp)
        resp.status = falcon.HTTP_404

    def has_param(self, req, param):
        return param in req.context['doc'] and req.context['doc'][param]

    def param(self, req, param):
        return req.context['doc'][param]


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
        next_heartbeat = self.heartbeat_timestamp + datetime.timedelta(seconds=self.heartbeat_threshold())
        return next_heartbeat > eva.now_with_timezone()

    def set_skip_heartbeat(self, skip):
        self.skip_heartbeat = skip

    def set_heartbeat_timeout(self, timeout):
        self.heartbeat_timeout = int(timeout)

    def set_heartbeat_interval(self, interval):
        self.heartbeat_interval = int(interval)

    def heartbeat(self, timestamp):
        self.heartbeat_timestamp = timestamp

    def heartbeat_threshold(self):
        return self.heartbeat_interval + self.heartbeat_timeout

    def on_get(self, req, resp):
        if self.ok():
            resp.status = falcon.HTTP_200
            self.set_response_message(req, 'Last heartbeat was received %s' % str(self.heartbeat_timestamp))
        else:
            resp.status = '555 Heart Has Stopped'
            self.set_response_message(req, 'Last heartbeat was received %s; over age threshold of %d seconds' % (str(self.heartbeat_timestamp), self.heartbeat_threshold()))


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


class JobResource(BaseResource):
    """
    Provides an endpoint to list jobs.
    """

    def on_delete(self, req, resp, job_id=None):
        job = self.eventloop.job_by_id(job_id)
        if not job:
            raise falcon.HTTPNotFound()
        job.set_status(eva.job.DELETED)
        self.set_response_message(resp, "The job '%s' has been marked for deletion.")


class JobsResource(BaseResource):
    """
    Provides an endpoint to list jobs.
    """

    def on_get(self, req, resp):
        jobs = []
        for item in self.eventloop.event_queue:
            for job in item:
                jobs += [{
                    'adapter_id': job.adapter.config_id,
                    'event_id': item.id(),
                    'failures': job.failures(),
                    'job_id': job.id,
                    'resource_uri': '/jobs/%s' % job.id,
                    'status': job.status,
                }]
        self.set_result(req, jobs)


class ProcessResource(BaseResource):
    """
    Accept requests to add Productstatus resources to the processing queue.
    """

    def get_adapter_or_bust(self, adapter_config_id):
        adapter = self.eventloop.adapter_by_config_id(adapter_config_id)
        if not adapter:
            raise falcon.HTTPBadRequest("The adapter '%s' is not configured." % adapter_config_id)
        return adapter

    def productinstance(self, req, resp):
        adapter = self.get_adapter_or_bust(self.param(req, 'adapter'))
        uuid = self.param(req, 'uuid')
        self.eventloop.process_all_in_product_instance(uuid, [adapter])
        self.set_response_message(req, "All DataInstances resources descended from ProductInstance UUID '%s' has been added to the event queue." % uuid)

    def datainstance(self, req, resp):
        adapter = self.get_adapter_or_bust(self.param(req, 'adapter'))
        uuid = self.param(req, 'uuid')
        self.eventloop.process_data_instance(uuid, [adapter])
        self.set_response_message(req, "DataInstance UUID '%s' has been added to the event queue." % uuid)

    def on_post(self, req, resp, method=None):
        if not self.has_param(req, 'uuid'):
            raise falcon.HTTPBadRequest("Please provide the 'uuid' parameter, specifying which Productstatus resource to process.")

        if not self.has_param(req, 'adapter'):
            raise falcon.HTTPBadRequest("Please provide the 'adapter' parameter, specifying which adapter should process the resource.")

        try:
            self.exec_functions(req, resp, method, ['productinstance', 'datainstance'])
            resp.status = falcon.HTTP_202

        except productstatus.exceptions.NotFoundException as e:
            raise falcon.HTTPBadRequest('The Productstatus resource could not be found: %s' % e)

        except productstatus.exceptions.ServiceUnavailableException as e:
            raise falcon.HTTPServiceUnavailable('An error occurred when retrieving Productstatus resources: %s' % e)
