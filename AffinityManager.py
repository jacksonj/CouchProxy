from threading import Lock

class AffinityManager:
    """
    Manages session cookies by host / port. Reentrant safe for threading
    or forking in the server. Assumes that only ever one session will
    be required by one host:port combination at a time
    """
    def __init__(self, logger):
        self.logger = logger
        self.queue_lock = Lock()
        self.session_lock = Lock()
        self.pending_sessions = {}
        self.sessions = {}
        
    def session_key(self, host):
        """
        Returns a key for the given session
        """
        return "%s" % (host)
    
    def queue_session(self, host, handler):
        """
        Adds an entry to the structure indicating that the next request
        should result in an affinity sessions cookie
        """
        self.queue_lock.acquire()
        try:
            # Get the session key
            key = self.session_key(host)
            
            # If there is an active or queued session, remove them
            if self.pending_sessions.has_key(key):
                handler.log_debug("Start affinity session request already exists - removing")
                del self.pending_sessions[key]
            self.session_lock.acquire()
            try:
                if self.sessions.has_key(key):
                    handler.log_debug("Active affinity session already exists - removing")
                    del self.sessions[key]
            finally:
                self.session_lock.release()
            
            # Add the session key to the queued list
            self.pending_sessions[key] = True
            handler.log_debug("Affinity session request queued for %s", key)
        finally:
            self.queue_lock.release()
        
    def start_session(self, host, headers, handler):
        """
        Initialises a proxy affinity session for a given host and port
        """
        self.session_lock.acquire()
        self.queue_lock.acquire()
        try:
            # Get the session key
            key = self.session_key(host)
                
            # Check if there is a pending session
            if self.pending_sessions.has_key(key):
                # Remove from the queue
                del self.pending_sessions[key]
                  
                # Register the new session
                cookie = None
                if headers.has_key('set-cookie'):
                    cookie = headers['set-cookie']
                if cookie:
                    self.sessions[key] = cookie
                    handler.log_debug("Affinity session started")
                else:
                    handler.log_message("Affinity session could not start")
        finally:
            self.queue_lock.release()
            self.session_lock.release()
    
    def end_session(self, host, handler):
        """
        Ends a proxy affinity session for a given host and port
        """
        # Get the session key
        key = self.session_key(host)
            
        # Remove the session key from current list
        self.session_lock.acquire()
        try:
            if self.sessions.has_key(key):
                del self.sessions[key]
                handler.log_debug("Affinity session ended")
            else:
                handler.log_message("No affinity session to end")
        finally:
            self.session_lock.release()
        
        # Remove the session key from queued list
        self.queue_lock.acquire()
        try:
            if self.pending_sessions.has_key(key):
                del self.pending_sessions[key]
                handler.log_debug("Queued affinity request removed")
        finally:
            self.queue_lock.release()
    
    def get_session(self, host, handler):
        """
        Either returns the cookie associated with the given host / port,
        or None if no session is active
        """
        ret = None
        self.session_lock.acquire()
        try:
            # Get the session key
            key = self.session_key(host)
            
            # Return the key if present
            if self.sessions.has_key(key):
                ret = self.sessions[key]
                handler.log_debug("Got affinity session cookie")
        finally:
            self.session_lock.release()

        return ret
        
