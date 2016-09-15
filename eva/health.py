"""!
@brief Healthcheck module

This module provides an extremely simple HTTP server that responds with 204 No
Content when some arbitrary URL is accessed.
"""

import http.server

import datetime

import eva


class HTTPServer(http.server.HTTPServer):
    """!
    @brief HTTP server which blocks for 1ms while waiting for the next request.
    """
    timeout = 0.001


class HTTPRequestHandler(http.server.BaseHTTPRequestHandler):
    """!
    @brief HTTP request handler returning code 204 on all URLs, and closes
    connections immediately.
    """
    close_connection = True

    def do_GET(self):
        if self.server.ok:
            self.send_response(204)
        else:
            self.send_response(503)
        self.end_headers()


class HealthCheckServer(object):
    """!
    @brief Health check server. Instantiates a HTTPServer and HTTPRequestHandler
    object, which are used to respond to health checks via HTTP.
    """
    def __init__(self, host, port):
        self.server = HTTPServer((host, port), HTTPRequestHandler)
        self.server.ok = True
        self.heartbeat_interval = 0
        self.heartbeat_timeout = 0
        self.heartbeat_timestamp = eva.epoch_with_timezone()

    def respond_to_next_request(self):
        self.calculate_status()
        self.server.handle_request()

    def calculate_status(self):
        if self.heartbeat_interval == 0:
            return
        next_heartbeat = self.heartbeat_timestamp + datetime.timedelta(seconds=self.heartbeat_interval + self.heartbeat_timeout)
        if next_heartbeat > eva.now_with_timezone():
            self.server.ok = True
        else:
            self.server.ok = False

    def set_heartbeat_timeout(self, timeout):
        self.heartbeat_timeout = int(timeout)

    def set_heartbeat_interval(self, interval):
        self.heartbeat_interval = int(interval)

    def heartbeat(self, timestamp):
        self.heartbeat_timestamp = timestamp
