"""!
@brief Healthcheck module

This module provides an extremely simple HTTP server that responds with 204 No
Content when some arbitrary URL is accessed.
"""

import http.server


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
        self.send_response(204)
        self.flush_headers()


class HealthCheckServer(object):
    """!
    @brief Health check server. Instantiates a HTTPServer and HTTPRequestHandler
    object, which are used to respond to health checks via HTTP.
    """
    def __init__(self, host, port):
        self.server = HTTPServer((host, port), HTTPRequestHandler)

    def respond_to_next_request(self):
        self.server.handle_request()
