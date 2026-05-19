# Tweepy
# Copyright 2009-2023 Joshua Roesslein
# See LICENSE for details.

import datetime
import hashlib
import logging
import pickle
import threading
import time
import os

try:
    import fcntl
except ImportError:
    # Probably on a windows system
    # TODO: use win32file
    pass

log = logging.getLogger(__name__)


class Cache:
    """Cache interface"""

    def __init__(self, timeout=60):
        """Initialize the cache
            timeout: number of seconds to keep a cached entry
        """
        self.timeout = timeout

    def store(self, key, value):
        """Add new record to cache
            key: entry key
            value: data of entry
        """
        raise NotImplementedError

    def get(self, key, timeout=None):
        """Get cached entry if exists and not expired
            key: which entry to get
            timeout: override timeout with this value [optional]
        """
        raise NotImplementedError

    def count(self):
        """Get count of entries currently stored in cache"""
        raise NotImplementedError

    def cleanup(self):
        """Delete any expired entries in cache."""
        raise NotImplementedError

    def flush(self):
        """Delete all cached entries"""
        raise NotImplementedError


class MemoryCache(Cache):
    """In-memory cache"""

    def __init__(self, timeout=60):
        Cache.__init__(self, timeout)
        self._entries = {}
        self.lock = threading.Lock()

    def __getstate__(self):
        # pickle
        return {'entries': self._entries, 'timeout': self.timeout}

    def __setstate__(self, state):
        # unpickle
        self.lock = threading.Lock()
        self._entries = state['entries']
        self.timeout = state['timeout']

    def _is_expired(self, entry, timeout):
        return timeout > 0 and (time.time() - entry[0]) >= timeout

    def store(self, key, value):
        self.lock.acquire()
        self._entries[key] = (time.time(), value)
        self.lock.release()

    def get(self, key, timeout=None):
        self.lock.acquire()
        try:
            # check to see if we have this key
            entry = self._entries.get(key)
            if not entry:
                # no hit, return nothing
                return None

            # use provided timeout in arguments if provided
            # otherwise use the one provided during init.
            if timeout is None:
                timeout = self.timeout

            # make sure entry is not expired
            if self._is_expired(entry, timeout):
                # entry expired, delete and return nothing
                del self._entries[key]
                return None

            # entry found and not expired, return it
            return entry[1]
        finally:
            self.lock.release()





class FileCache(Cache):
    """File-based cache"""

    # locks used to make cache thread-safe
    cache_locks = {}

    def __init__(self, cache_dir, timeout=60):
        Cache.__init__(self, timeout)
        if os.path.exists(cache_dir) is False:
            os.mkdir(cache_dir)
        self.cache_dir = cache_dir
        if cache_dir in FileCache.cache_locks:
            self.lock = FileCache.cache_locks[cache_dir]
        else:
            self.lock = threading.Lock()
            FileCache.cache_locks[cache_dir] = self.lock

        if os.name == 'posix':
            self._lock_file = self._lock_file_posix
            self._unlock_file = self._unlock_file_posix
        elif os.name == 'nt':
            self._lock_file = self._lock_file_win32
            self._unlock_file = self._unlock_file_win32
        else:
            log.warning('FileCache locking not supported on this system!')
            self._lock_file = self._lock_file_dummy
            self._unlock_file = self._unlock_file_dummy

    def _get_path(self, key):
        md5 = hashlib.md5()
        md5.update(key.encode('utf-8'))
        return os.path.join(self.cache_dir, md5.hexdigest())







    def _delete_file(self, path):
        os.remove(path)
        if os.path.exists(path + '.lock'):
            os.remove(path + '.lock')

    def store(self, key, value):
        path = self._get_path(key)
        self.lock.acquire()
        try:
            # acquire lock and open file
            f_lock = self._lock_file(path)
            datafile = open(path, 'wb')

            # write data
            pickle.dump((time.time(), value), datafile)

            # close and unlock file
            datafile.close()
            self._unlock_file(f_lock)
        finally:
            self.lock.release()

    def get(self, key, timeout=None):
        return self._get(self._get_path(key), timeout)

    def _get(self, path, timeout):
        if os.path.exists(path) is False:
            # no record
            return None
        self.lock.acquire()
        try:
            # acquire lock and open
            f_lock = self._lock_file(path, False)
            datafile = open(path, 'rb')

            # read pickled object
            created_time, value = pickle.load(datafile)
            datafile.close()

            # check if value is expired
            if timeout is None:
                timeout = self.timeout
            if timeout > 0:
                if (time.time() - created_time) >= timeout:
                    # expired! delete from cache
                    value = None
                    self._delete_file(path)

            # unlock and return result
            self._unlock_file(f_lock)
            return value
        finally:
            self.lock.release()





class MemCacheCache(Cache):
    """Cache interface"""

    def __init__(self, client, timeout=60):
        """Initialize the cache
            client: The memcache client
            timeout: number of seconds to keep a cached entry
        """
        self.client = client
        self.timeout = timeout

    def store(self, key, value):
        """Add new record to cache
            key: entry key
            value: data of entry
        """
        self.client.set(key, value, time=self.timeout)

    def get(self, key, timeout=None):
        """Get cached entry if exists and not expired
            key: which entry to get
            timeout: override timeout with this value [optional].
            DOES NOT WORK HERE
        """
        return self.client.get(key)

    def count(self):
        """Get count of entries currently stored in cache. RETURN 0"""
        raise NotImplementedError

    def cleanup(self):
        """Delete any expired entries in cache. NO-OP"""
        raise NotImplementedError

    def flush(self):
        """Delete all cached entries. NO-OP"""
        raise NotImplementedError


class RedisCache(Cache):
    """Cache running in a redis server"""

    def __init__(self, client,
                 timeout=60,
                 keys_container='tweepy:keys',
                 pre_identifier='tweepy:'):
        Cache.__init__(self, timeout)
        self.client = client
        self.keys_container = keys_container
        self.pre_identifier = pre_identifier

    def _is_expired(self, entry, timeout):
        # Returns true if the entry has expired
        return timeout > 0 and (time.time() - entry[0]) >= timeout

    def store(self, key, value):
        """Store the key, value pair in our redis server"""
        # Prepend tweepy to our key,
        # this makes it easier to identify tweepy keys in our redis server
        key = self.pre_identifier + key
        # Get a pipe (to execute several redis commands in one step)
        pipe = self.client.pipeline()
        # Set our values in a redis hash (similar to python dict)
        pipe.set(key, pickle.dumps((time.time(), value)))
        # Set the expiration
        pipe.expire(key, self.timeout)
        # Add the key to a set containing all the keys
        pipe.sadd(self.keys_container, key)
        # Execute the instructions in the redis server
        pipe.execute()

    def get(self, key, timeout=None):
        """Given a key, returns an element from the redis table"""
        key = self.pre_identifier + key
        # Check to see if we have this key
        unpickled_entry = self.client.get(key)
        if not unpickled_entry:
            # No hit, return nothing
            return None

        entry = pickle.loads(unpickled_entry)
        # Use provided timeout in arguments if provided
        # otherwise use the one provided during init.
        if timeout is None:
            timeout = self.timeout

        # Make sure entry is not expired
        if self._is_expired(entry, timeout):
            # entry expired, delete and return nothing
            self.delete_entry(key)
            return None
        # entry found and not expired, return it
        return entry[1]

    def count(self):
        """Note: This is not very efficient,
        since it retrieves all the keys from the redis
        server to know how many keys we have"""
        pass

    def delete_entry(self, key):
        """Delete an object from the redis table"""
        pipe = self.client.pipeline()
        pipe.srem(self.keys_container, key)
        pipe.delete(key)
        pipe.execute()

    def cleanup(self):
        """Cleanup all the expired keys"""
        pass

    def flush(self):
        """Delete all entries from the cache"""
        pass


class MongodbCache(Cache):
    """A simple pickle-based MongoDB cache system."""

    def __init__(self, db, timeout=3600, collection='tweepy_cache'):
        """Should receive a "database" cursor from pymongo."""
        Cache.__init__(self, timeout)
        self.timeout = timeout
        self.col = db[collection]
        self.col.create_index('created', expireAfterSeconds=timeout)

    def store(self, key, value):
        from bson.binary import Binary

        now = datetime.datetime.utcnow()
        blob = Binary(pickle.dumps(value))

        self.col.insert({'created': now, '_id': key, 'value': blob})

    def get(self, key, timeout=None):
        if timeout:
            raise NotImplementedError
        obj = self.col.find_one({'_id': key})
        if obj:
            return pickle.loads(obj['value'])


    def delete_entry(self, key):
        return self.col.remove({'_id': key})

    def cleanup(self):
        """MongoDB will automatically clear expired keys."""
        pass

