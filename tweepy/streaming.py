# Tweepy
# Copyright 2009-2023 Joshua Roesslein
# See LICENSE for details.

# Appengine users: https://developers.google.com/appengine/docs/python/sockets/#making_httplib_use_sockets

from collections import namedtuple
import json
import logging
from math import inf
from platform import python_version
import ssl
import traceback
from threading import Thread
from time import sleep
from typing import NamedTuple

import requests
import urllib3

import tweepy
from tweepy.client import BaseClient, Response
from tweepy.errors import TweepyException
from tweepy.tweet import Tweet

log = logging.getLogger(__name__)

StreamResponse = namedtuple(
    "StreamResponse", ("data", "includes", "errors", "matching_rules")
)


class BaseStream:

    def __init__(self, *, chunk_size=512, daemon=False, max_retries=inf,
                 proxy=None, verify=True):
        self.chunk_size = chunk_size
        self.daemon = daemon
        self.max_retries = max_retries
        self.proxies = {"https": proxy} if proxy else {}
        self.verify = verify

        self.running = False
        self.session = requests.Session()
        self.thread = None
        self.user_agent = (
            f"Python/{python_version()} "
            f"Requests/{requests.__version__} "
            f"Tweepy/{tweepy.__version__}"
        )



    def disconnect(self):
        """Disconnect the stream"""
        pass

    def on_closed(self, response):
        """This is called when the stream has been closed by Twitter.

        Parameters
        ----------
        response : requests.Response
            The Response from Twitter
        """
        pass

    def on_connect(self):
        """This is called after successfully connecting to the streaming API.
        """
        pass

    def on_connection_error(self):
        """This is called when the stream connection errors or times out."""
        pass

    def on_disconnect(self):
        """This is called when the stream has disconnected."""
        pass

    def on_exception(self, exception):
        """This is called when an unhandled exception occurs.

        Parameters
        ----------
        exception : Exception
            The unhandled exception
        """
        pass

    def on_keep_alive(self):
        """This is called when a keep-alive signal is received."""
        pass

    def on_request_error(self, status_code):
        """This is called when a non-200 HTTP status code is encountered.

        Parameters
        ----------
        status_code : int
            The HTTP status code encountered
        """
        pass


class StreamingClient(BaseClient, BaseStream):
    """Filter and sample realtime Tweets with Twitter API v2

    .. versionadded:: 4.6

    Parameters
    ----------
    bearer_token : str
        Twitter API Bearer Token
    return_type : type[dict | requests.Response | Response]
        Type to return from requests to the API
    wait_on_rate_limit : bool
        Whether or not to wait before retrying when a rate limit is
        encountered. This applies to requests besides those that connect to a
        stream (see ``max_retries``).
    chunk_size : int
        The default socket.read size. Default to 512, less than half the size
        of a Tweet so that it reads Tweets with the minimal latency of 2 reads
        per Tweet. Values higher than ~1kb will increase latency by waiting for
        more data to arrive but may also increase throughput by doing fewer
        socket read calls.
    daemon : bool
        Whether or not to use a daemon thread when using a thread to run the
        stream
    max_retries : int
        Max number of times to retry connecting the stream
    proxy : str | None
        URL of the proxy to use when connecting to the stream
    verify : bool | str
        Either a boolean, in which case it controls whether to verify the
        server’s TLS certificate, or a string, in which case it must be a path
        to a CA bundle to use.

    Attributes
    ----------
    running : bool
        Whether there's currently a stream running
    session : :class:`requests.Session`
        Requests Session used to connect to the stream
    thread : :class:`threading.Thread` | None
        Thread used to run the stream
    user_agent : str
        User agent used when connecting to the stream
    """

    def __init__(self, bearer_token, *, return_type=Response,
                 wait_on_rate_limit=False, **kwargs):
        """__init__( \
            bearer_token, *, return_type=Response, wait_on_rate_limit=False, \
            chunk_size=512, daemon=False, max_retries=inf, proxy=None, \
            verify=True \
        )
        """
        BaseClient.__init__(self, bearer_token, return_type=return_type,
                            wait_on_rate_limit=wait_on_rate_limit)
        BaseStream.__init__(self, **kwargs)


    def _process_data(self, data, data_type=None):
        if data_type is StreamRule:
            if isinstance(data, list):
                rules = []
                for rule in data:
                    if "tag" in rule:
                        rules.append(StreamRule(
                            value=rule["value"], id=rule["id"], tag=rule["tag"]
                        ))
                    else:
                        rules.append(StreamRule(value=rule["value"],
                                                id=rule["id"]))
                return rules
            elif data is not None:
                if "tag" in data:
                    return StreamRule(value=data["value"], id=data["id"],
                                      tag=data["tag"])
                else:
                    return StreamRule(value=data["value"], id=data["id"])
        else:
            return super()._process_data(data, data_type=data_type)

    def add_rules(self, add, **params):
        """add_rules(add, *, dry_run)

        Add rules to filtered stream.

        Parameters
        ----------
        add : list[StreamRule] | StreamRule
            Specifies the operation you want to perform on the rules.
        dry_run : bool
            Set to true to test the syntax of your rule without submitting it.
            This is useful if you want to check the syntax of a rule before
            removing one or more of your existing rules.

        Returns
        -------
        dict | requests.Response | Response

        References
        ----------
        https://developer.twitter.com/en/docs/twitter-api/tweets/filtered-stream/api-reference/post-tweets-search-stream-rules
        """
        pass

    def delete_rules(self, ids, **params):
        """delete_rules(ids, *, dry_run)

        Delete rules from filtered stream.

        Parameters
        ----------
        ids : int | str | list[int | str | StreamRule] | StreamRule
            Array of rule IDs, each one representing a rule already active in
            your stream. IDs must be submitted as strings.
        dry_run : bool
            Set to true to test the syntax of your rule without submitting it.
            This is useful if you want to check the syntax of a rule before
            removing one or more of your existing rules.

        Returns
        -------
        dict | requests.Response | Response

        References
        ----------
        https://developer.twitter.com/en/docs/twitter-api/tweets/filtered-stream/api-reference/post-tweets-search-stream-rules
        """
        pass

    def filter(self, *, threaded=False, **params):
        """filter( \
            *, backfill_minutes=None, expansions=None, media_fields=None, \
            place_fields=None, poll_fields=None, tweet_fields=None, \
            user_fields=None, threaded=False \
        )

        Streams Tweets in real-time based on a specific set of filter rules.

        If you are using the academic research product track, you can connect
        up to two `redundant connections <filter redundant connections_>`_ to
        maximize your streaming up-time.

        The Tweets returned by this endpoint count towards the Project-level
        `Tweet cap`_.

        Parameters
        ----------
        backfill_minutes : int | None
            By passing this parameter, you can request up to five (5) minutes
            worth of streaming data that you might have missed during a
            disconnection to be delivered to you upon reconnection. The
            backfilled Tweets will automatically flow through the reconnected
            stream, with older Tweets generally being delivered before any
            newly matching Tweets. You must include a whole number between 1
            and 5 as the value to this parameter.

            This feature will deliver duplicate Tweets, meaning that if you
            were disconnected for 90 seconds, and you requested two minutes of
            backfill, you will receive 30 seconds worth of duplicate Tweets.
            Due to this, you should make sure your system is tolerant of
            duplicate data.

            This feature is currently only available to the Academic Research
            product track.
        expansions : list[str] | str
            :ref:`expansions_parameter`
        media_fields : list[str] | str
            :ref:`media_fields_parameter`
        place_fields : list[str] | str
            :ref:`place_fields_parameter`
        poll_fields : list[str] | str
            :ref:`poll_fields_parameter`
        tweet_fields : list[str] | str
            :ref:`tweet_fields_parameter`
        user_fields : list[str] | str
            :ref:`user_fields_parameter`
        threaded : bool
            Whether or not to use a thread to run the stream

        Raises
        ------
        TweepyException
            When the stream is already connected

        Returns
        -------
        threading.Thread | None
            The thread if ``threaded`` is set to ``True``, else ``None``

        References
        ----------
        https://developer.twitter.com/en/docs/twitter-api/tweets/filtered-stream/api-reference/get-tweets-search-stream

        .. _filter redundant connections: https://developer.twitter.com/en/docs/twitter-api/tweets/filtered-stream/integrate/recovery-and-redundancy-features
        .. _Tweet cap: https://developer.twitter.com/en/docs/twitter-api/tweet-caps
        """
        pass

    def get_rules(self, **params):
        """get_rules(*, ids)

        Return a list of rules currently active on the streaming endpoint,
        either as a list or individually.

        Parameters
        ----------
        ids : list[str] | str
            Comma-separated list of rule IDs. If omitted, all rules are
            returned.

        Returns
        -------
        dict | requests.Response | Response

        References
        ----------
        https://developer.twitter.com/en/docs/twitter-api/tweets/filtered-stream/api-reference/get-tweets-search-stream-rules
        """
        pass

    def sample(self, *, threaded=False, **params):
        """sample( \
            *, backfill_minutes=None, expansions=None, media_fields=None, \
            place_fields=None, poll_fields=None, tweet_fields=None, \
            user_fields=None, threaded=False \
        )

        Streams about 1% of all Tweets in real-time.

        If you are using the academic research product track, you can connect
        up to two `redundant connections <sample redundant connections_>`_ to
        maximize your streaming up-time.

        Parameters
        ----------
        backfill_minutes : int | None
            By passing this parameter, you can request up to five (5) minutes
            worth of streaming data that you might have missed during a
            disconnection to be delivered to you upon reconnection. The
            backfilled Tweets will automatically flow through the reconnected
            stream, with older Tweets generally being delivered before any
            newly matching Tweets. You must include a whole number between 1
            and 5 as the value to this parameter.

            This feature will deliver duplicate Tweets, meaning that if you
            were disconnected for 90 seconds, and you requested two minutes of
            backfill, you will receive 30 seconds worth of duplicate Tweets.
            Due to this, you should make sure your system is tolerant of
            duplicate data.

            This feature is currently only available to the Academic Research
            product track.
        expansions : list[str] | str
            :ref:`expansions_parameter`
        media_fields : list[str] | str
            :ref:`media_fields_parameter`
        place_fields : list[str] | str
            :ref:`place_fields_parameter`
        poll_fields : list[str] | str
            :ref:`poll_fields_parameter`
        tweet_fields : list[str] | str
            :ref:`tweet_fields_parameter`
        user_fields : list[str] | str
            :ref:`user_fields_parameter`
        threaded : bool
            Whether or not to use a thread to run the stream

        Raises
        ------
        TweepyException
            When the stream is already connected

        Returns
        -------
        threading.Thread | None
            The thread if ``threaded`` is set to ``True``, else ``None``

        References
        ----------
        https://developer.twitter.com/en/docs/twitter-api/tweets/volume-streams/api-reference/get-tweets-sample-stream

        .. _sample redundant connections: https://developer.twitter.com/en/docs/twitter-api/tweets/volume-streams/integrate/recovery-and-redundancy-features
        """
        pass

    def on_data(self, raw_data):
        """This is called when raw data is received from the stream.
        This method handles sending the data to other methods.

        Parameters
        ----------
        raw_data : JSON
            The raw data from the stream

        References
        ----------
        https://developer.twitter.com/en/docs/twitter-api/tweets/filtered-stream/integrate/consuming-streaming-data
        """
        pass

    def on_tweet(self, tweet):
        """This is called when a Tweet is received.

        Parameters
        ----------
        tweet : Tweet
            The Tweet received
        """
        pass

    def on_includes(self, includes):
        """This is called when includes are received.

        Parameters
        ----------
        includes : dict
            The includes received
        """
        pass

    def on_errors(self, errors):
        """This is called when errors are received.

        Parameters
        ----------
        errors : dict
            The errors received
        """
        pass

    def on_matching_rules(self, matching_rules):
        """This is called when matching rules are received.

        Parameters
        ----------
        matching_rules : list[StreamRule]
            The matching rules received
        """
        pass

    def on_response(self, response):
        """This is called when a response is received.

        Parameters
        ----------
        response : StreamResponse
            The response received
        """
        pass


class StreamRule(NamedTuple):
    """Rule for filtered stream

    .. versionadded:: 4.6

    Parameters
    ----------
    value : str | None
        The rule text. If you are using a `Standard Project`_ at the Basic
        `access level`_, you can use the basic set of `operators`_, can submit
        up to 25 concurrent rules, and can submit rules up to 512 characters
        long. If you are using an `Academic Research Project`_ at the Basic
        access level, you can use all available operators, can submit up to
        1,000 concurrent rules, and can submit rules up to 1,024 characters
        long.
    tag : str | None
        The tag label. This is a free-form text you can use to identify the
        rules that matched a specific Tweet in the streaming response. Tags can
        be the same across rules.
    id : str | None
        Unique identifier of this rule. This is returned as a string.


    .. _Standard Project: https://developer.twitter.com/en/docs/projects
    .. _access level: https://developer.twitter.com/en/products/twitter-api/early-access/guide#na_1
    .. _operators: https://developer.twitter.com/en/docs/twitter-api/tweets/search/integrate/build-a-query
    .. _Academic Research Project: https://developer.twitter.com/en/docs/projects
    """
    value: str = None
    tag: str = None
    id: str = None
