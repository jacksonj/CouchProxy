#!/usr/bin/env python

import BaseHTTPServer

from Daemon import Daemon
import logging
from optparse import OptionParser
import time
import sys
from AffinityManager import AffinityManager
from CouchProxyRequest import CouchProxyRequest
from CouchProxyHandler import CouchProxyHandler

class CouchProxy(Daemon):
    """
    A simply proxy designed to pass CouchDB requests on to the cmsweb
    front end. Adds SSL authentication to the requests.
    """
    def __init__(self, local_host = "localhost", local_port = 8080,
                       remote_host = "http://localhost:5984",
                       key_file = None, cert_file = None, logger=None,
                       pid_file = "/tmp/couchproxy.pid"):
        # Construction business
        Daemon.__init__(self, pid_file)
        self.logger = logger
            
        # Configure the handler
        self.handler = CouchProxyHandler
        self.handler.remote_address = remote_host
        self.handler.client = CouchProxyRequest(remote_host,
                                key_file=key_file, cert_file=cert_file)
        self.handler.logger = self.logger
        
        # Configure the server
        self.server_address = (local_host, local_port)
        
    def run(self):
        """
        Starts the proxy
        """
        # Instantiate the server
        self.httpd = BaseHTTPServer.HTTPServer(self.server_address, self.handler)
        
        # Add the proxy session affinity manager
        self.httpd.affinity = AffinityManager(self.logger)
        
        # Log the initialisation
        self.logger.log_info("CouchProxy", "CouchProxy initialised on %s:%s",
                            self.server_address[0], self.server_address[1])
        self.logger.log_info("CouchProxy", "Forwarding to %s",
                            self.handler.remote_address)
        
        # Start it up!
        self.httpd.serve_forever()

def check_server_url(srvurl):
    """
    Basic sanity check that a server name is valid
    """
    good_name = srvurl.startswith('http://') or srvurl.startswith('https://')
    if not good_name:
        msg = "You must include http(s):// in your servers address, %s doesn't" % srvurl
        raise ValueError(msg)

def parse_args():
    usage = "usage: %prog [options] [start | stop | restart]"
    parser = OptionParser()
    parser.add_option("-l", "--listenhost", dest="local_host",
        default="127.0.0.1", help="Local address to listen on. Defaults to 127.0.0.1")
    parser.add_option("-p", "--listenport", dest="local_port", type="int",
        default=8080, help="Local port to list on. Defaults to 8080")
    parser.add_option("-r", "--remote", dest="remote_host", default="http://127.0.0.1:5985",
        help="Remote host to forward request to. Defaults to http://127.0.0.1:5985")
    parser.add_option("-k", "--keyfile", dest="key_file", default=None,
        help="Location of SSL key if forward authentication is required")
    parser.add_option("-c", "--certfile", dest="cert_file", default=None,
        help="Location of SSL certificate if forward authentication is required")
    parser.add_option("-o", "--logfile", dest="log_file", default=None,
        help="Desired location of log. If not specified, no logging (daemon), or console (not daemon)")
    parser.add_option("-d", "--pidfile", dest="pid_file", default="/tmp/couchproxy.pid",
        help="Desired location of deamon pid file. Defaults to /tmp/couchproxy.pid")
    parser.add_option("-v", "--verbose", dest="verbose", default=False,
        action="store_true", help="Turns on verbose logging")
        
    return parser.parse_args()

class DateTimeFormatter:
    """
    A simple helper class to pass log date / time formatting around
    """
    weekdayname = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']

    monthname = [None,
                 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    def log_date_time_string(self):
        """Return the current time formatted for logging."""
        now = time.time()
        year, month, day, hh, mm, ss, x, y, z = time.localtime(now)
        s = "%02d/%3s/%04d %02d:%02d:%02d" % (
                day, self.monthname[month], year, hh, mm, ss)
        return s

def get_logger(verbose, log_file):
    """
    Helper function to return a properly setup logger
    """
    logger = logging.getLogger("CouchProxy")
    if verbose:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    if log_file:
        fileHandler = logging.FileHandler(log_file)
        logger.addHandler(fileHandler)
    elif len(args) == 0:
        streamHandler = logging.StreamHandler()
        logger.addHandler(streamHandler)
        
    # Now add the custom formatter business
    logger.date_time_formatter = DateTimeFormatter()
    def log_info(source, format, *args):
        logger.info("%s - - [%s] %s" %
                         (source,
                          logger.date_time_formatter.log_date_time_string(),
                          format%args))
    def log_debug(source, format, *args):
        logger.debug("%s - - [%s] %s" %
                         (source,
                          logger.date_time_formatter.log_date_time_string(),
                          format%args))
    logger.log_info = log_info
    logger.log_debug = log_debug
    return logger

# The script entry point
if __name__ == "__main__":
    # Parse the arguments
    (options, args) = parse_args()
    
    # Check the validity of the outwards host
    check_server_url(options.remote_host)
    
    # Setup logging
    logger = get_logger(options.verbose, options.log_file)
    
    # Prepare the proxy
    daemon = CouchProxy(local_host = options.local_host, local_port = options.local_port,
                        remote_host = options.remote_host, pid_file = options.pid_file,
                        key_file = options.key_file, cert_file = options.cert_file,
                        logger=logger)
    
    if len(args) == 1:
        # Perform the daemon magic
        if 'start' == args[0]:
            daemon.start()
        elif 'stop' == args[0]:
            daemon.stop()
        elif 'restart' == args[0]:
            daemon.restart()
        else:
            print "Unknown daemon command"
            sys.exit(2)
        sys.exit(0)
    else:
        # This skips all the daemon machinery
        daemon.run()
