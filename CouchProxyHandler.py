import BaseHTTPServer

class CouchProxyHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    """
    The HTTP handler for incoming proxy requests
    """
    
    # The list of headers from the CouchDB client which will be
    # forwaded to the onward host
    FWD_HEADERS = ("Accept", "Accept-Charset", "Accept-Encoding",
                   "Content-Type", "User-Agent", "Content-Length",
                   "X-Couch-Full-Commit", "Cookie", "Set-Cookie")
    
    def send_response(self, code, message=None):
        """Send the response header and log the response code.

        Also send two standard headers with the server software
        version and the current date.

        """
        if message is None:
            if code in self.responses:
                message = self.responses[code][0]
            else:
                message = ''
        if self.request_version != 'HTTP/0.9':
            self.wfile.write("%s %d %s\r\n" %
                             (self.protocol_version, code, message))
            # print (self.protocol_version, code, message)
        self.send_header('Server', self.version_string())
        self.send_header('Date', self.date_time_string())
    
    def log_message(self, format, *args):
        """
        Logs a message, appending useful info"
        """
        self.logger.log_info(self.address_string(), format, *args)
        
    def log_debug(self, format, *args):
        """
        Logs a message, appending useful info"
        """
        self.logger.log_debug(self.address_string(), format, *args)
    
    def get_request_headers(self):
        """
        Parses the request headers and returns a dictionary
        of all to be forwarded to the remote host
        """
        ret_headers = {}
        for k in CouchProxyHandler.FWD_HEADERS:
            v = self.headers.get(k, None)
            if v:
                ret_headers[k] = v
                
        return ret_headers
        
    def get_response_headers(self, response):
        """
        Parses the request headers and returns a dictionary
        of all the to be forwaded back to the calling client.
        Strips off the affinity session SetCookie request, if
        present
        """
        ret_headers = {}
        for h in response:
            # Ignore httplib2 stuff
            if h not in ('fromcache', 'version', 'status',
                         'reason', 'previous', 'content-location'):
                # Do not pass back the cmsweb front-end cookie
                if h == 'set-cookie':
                    k, v = response[h].strip().split('=')
                    if k != 'CmsAffinity':
                        # This Set-Cookie header can be passed back
                        ret_headers[h] = response[h]
                else:
                    # Return this header
                    ret_headers[h] = response[h]
        
        return ret_headers
                
    def add_cookie(self, headers, cookie):
        """
        Adds a cookie to the request headers
        """
        newCookies = []
        # Remove the cookie if it already exists in the request
        # All others will be forwaded
        k = cookie.split('=')
        if headers.has_key('Cookie'):
            curCookies = headers['Cookie'].strip().split(';')
            curCookies = [C.strip() for C in curCookies]
            for c in curCookies:
                k2 = c.split('=')
                if k2[0] != k[0]:
                    newCookies.append(c)
                    
        # Create the new cookie header
        newCookies.append(cookie)
        headers['Cookie'] = "; ".join(newCookies)
    
    def generic_request(self, method):
        """
        All methods should be treated the same...
        """
        try:
            # Read the request
            host, port = self.client_address
            content_length = int(self.headers.getheader("Content-Length", 0))
            fwdHeaders = self.get_request_headers()
            body = self.rfile.read(content_length)
        
            # Debug logging
            self.log_debug("  Request headers:")
            for k in self.headers:
                self.log_debug("    %s: %s", k, self.headers[k])
            self.log_debug("  Forwarded headers:")
            for k in fwdHeaders:
                self.log_debug("    %s: %s", k, fwdHeaders[k])
        
            # Get affinity header if required
            affinity = self.server.affinity.get_session(host, self)
            if affinity:
                self.add_cookie(fwdHeaders, affinity)
        
            # Forward on the request
            result, response = self.client.makeRequest(self.path, method, fwdHeaders, body)
        
            # Start an affinity session if required
            self.server.affinity.start_session(host, response, self)
        
            # Return the result
            self.send_response(response.status)
        
            # Send / log headers
            # TODO: Strip affinity cookie SetCookie header
            self.log_debug("  Response headers:")
            retHeaders = self.get_response_headers(response)
            
            # Handle a chunked response - the client has unfolded this
            # so we need to add a content-length header
            if retHeaders.has_key('transfer-encoding') and retHeaders['transfer-encoding'] == 'chunked':
                del retHeaders['transfer-encoding']
                retHeaders['content-length'] = len(result)
            
            # Send all headers
            for k in retHeaders:
                self.send_header(k, retHeaders[k])
                self.log_debug("      %s: %s", k, retHeaders[k])
            self.end_headers()
        
            # Write the response data
            self.wfile.write(result)
        
            # All done!
            self.log_request(response.status, len(result))
        except:
            self.send_response(500)
            self.end_headers()
            message = "Error handling request"
            self.wfile.write(message)
            self.log_request(500, len(message))
    
    def do_PUT(self):
        self.log_request()
        self.generic_request('PUT')
        
    def do_GET(self):
        self.log_request()
        self.generic_request('GET')
        
    def do_POST(self):
        self.log_request()
        # POST can be asking for a new affinity session to start...
        if self.path == "/ProxyAffinity/Session":
            try:
                # Queue the session
                host, port = self.client_address
                self.server.affinity.queue_session(host, self)
                self.send_response(200)
                self.end_headers()
                self.log_request(200, 0)
            except:
                self.send_response(500)
                self.end_headers()
                message = "Error starting affinity session"
                self.wfile.write(message)
                senf.log_request(500, len(message))
        else:
            # Just a normal POST request
            self.generic_request('POST')
        
    def do_DELETE(self):
        self.log_request()
        # DELETE can be asking for an affinity session to end...
        if self.path == "/ProxyAffinity/Session":
            try:
                # Remove the session
                host, port = self.client_address
                self.server.affinity.end_session(host, self)
                self.send_response(200)
                self.end_headers()
                self.log_request(200, 0)
            except:
                self.send_response(500)
                self.end_headers()
                message = "Error ending affinity session"
                self.wfile.write(message)
                senf.log_request(500, len(message))
        else:
            # Just a normal DELETE request
            self.generic_request('DELETE')
        
    def do_HEAD(self):
        self.log_request()
        self.generic_request('HEAD')
