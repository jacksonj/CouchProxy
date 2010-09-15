import socket
import httplib2

class CouchProxyRequest:
    """
    Handles onwards requests to the remote couch / front end. No
    processing of data is performed - intended to be used as the
    outward bound leg of a proxy
    """
    def __init__(self, host, cert_file = None, key_file = None):
        """
        Configures the request object, loading the required
        URL opener depending on whether key / cert is provided
        """
        self.host = host
        if cert_file and key_file:
            self.conn = self._getSSLURLOpener(cert_file, key_file)
        else:
            self.conn = self._getURLOpener()
    
    def makeRequest(self, resource, verb='GET', headers={}, body=""):
        """
        Make a request to the remote host
        """
        # Form complete URI
        uri = self.host + resource
    
        # Now attempt to send the request
        try:
            response, result = self.conn.request(uri, method = verb,
                                    body = body, headers = headers)
            if response.status == 408: # timeout can indicate a socket error
                response, result = self.conn.request(uri, method = verb,
                                        body = body, headers = headers)
        except (socket.error, AttributeError):
            [conn.close() for conn in self.conn.connections.values()]
            # ... try again... if this fails propagate error to client
            try:
                response, result = self.conn.request(uri, method = verb,
                                        body = body, headers = headers)
            except AttributeError:
                # socket/httplib really screwed up - nuclear option
                self.conn.connections = {}
                raise socket.error, 'Error contacting: %s' % self.host
    
        # Pass back the response
        return result, response
    
    def _getURLOpener(self):
        """
        method getting an HTTPConnection
        """
        return httplib2.Http(".cache", 30)
    
    def _getSSLURLOpener(self, cert, key, hostnam):
        """
        method getting a secure (HTTPS) connection
        """
        http = httplib2.Http(".cache", 30)
        http.add_certificate(key=key, cert=cert, domain=self.host)
        return http
