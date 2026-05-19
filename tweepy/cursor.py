# Tweepy
# Copyright 2009-2023 Joshua Roesslein
# See LICENSE for details.

from math import inf

from tweepy.errors import TweepyException
from tweepy.parsers import ModelParser, RawParser


class Cursor:
    """:class:`Cursor` can be used to paginate for any :class:`API` methods that
    support pagination

    Parameters
    ----------
    method
        :class:`API` method to paginate for
    args
        Positional arguments to pass to ``method``
    kwargs
        Keyword arguments to pass to ``method``
    """

    def __init__(self, method, *args, **kwargs):
        if hasattr(method, 'pagination_mode'):
            if method.pagination_mode == 'cursor':
                self.iterator = CursorIterator(method, *args, **kwargs)
            elif method.pagination_mode == 'dm_cursor':
                self.iterator = DMCursorIterator(method, *args, **kwargs)
            elif method.pagination_mode == 'id':
                self.iterator = IdIterator(method, *args, **kwargs)
            elif method.pagination_mode == "next":
                self.iterator = NextIterator(method, *args, **kwargs)
            elif method.pagination_mode == 'page':
                self.iterator = PageIterator(method, *args, **kwargs)
            else:
                raise TweepyException('Invalid pagination mode.')
        else:
            raise TweepyException('This method does not perform pagination')

    def pages(self, limit=inf):
        """Retrieve the page for each request

        Parameters
        ----------
        limit
            Maximum number of pages to iterate over

        Returns
        -------
        CursorIterator or DMCursorIterator or IdIterator or NextIterator or \
        PageIterator
            Iterator to iterate through pages
        """
        pass

    def items(self, limit=inf):
        """Retrieve the items in each page/request

        Parameters
        ----------
        limit
            Maximum number of items to iterate over

        Returns
        -------
        ItemIterator
            Iterator to iterate through items
        """
        iterator = ItemIterator(self.iterator)
        iterator.limit = limit
        return iterator


class BaseIterator:

    def __init__(self, method, *args, **kwargs):
        self.method = method
        self.args = args
        self.kwargs = kwargs
        self.limit = inf

    def __next__(self):
        return self.next()

    def next(self):
        raise NotImplementedError

    def prev(self):
        raise NotImplementedError

    def __iter__(self):
        return self


class CursorIterator(BaseIterator):

    def __init__(self, method, *args, **kwargs):
        BaseIterator.__init__(self, method, *args, **kwargs)
        start_cursor = self.kwargs.pop('cursor', None)
        self.next_cursor = start_cursor or -1
        self.prev_cursor = start_cursor or 0
        self.num_tweets = 0




class DMCursorIterator(BaseIterator):

    def __init__(self, method, *args, **kwargs):
        BaseIterator.__init__(self, method, *args, **kwargs)
        self.next_cursor = self.kwargs.pop('cursor', None)
        self.page_count = 0


    def prev(self):
        raise TweepyException('This method does not allow backwards pagination')


class IdIterator(BaseIterator):

    def __init__(self, method, *args, **kwargs):
        BaseIterator.__init__(self, method, *args, **kwargs)
        self.max_id = self.kwargs.pop('max_id', None)
        self.num_tweets = 0
        self.results = []
        self.model_results = []
        self.index = 0

    def next(self):
        """Fetch a set of items with IDs less than current set."""
        pass

    def prev(self):
        """Fetch a set of items with IDs greater than current set."""
        pass


class PageIterator(BaseIterator):

    def __init__(self, method, *args, **kwargs):
        BaseIterator.__init__(self, method, *args, **kwargs)
        self.current_page = 1
        # Keep track of previous page of items to handle Twitter API issue with
        # duplicate pages
        # https://twittercommunity.com/t/odd-pagination-behavior-with-get-users-search/148502
        # https://github.com/tweepy/tweepy/issues/1465
        # https://github.com/tweepy/tweepy/issues/958
        self.previous_items = []




class NextIterator(BaseIterator):

    def __init__(self, method, *args, **kwargs):
        BaseIterator.__init__(self, method, *args, **kwargs)
        self.next_token = self.kwargs.pop('next', None)
        self.page_count = 0


    def prev(self):
        raise TweepyException('This method does not allow backwards pagination')


class ItemIterator(BaseIterator):

    def __init__(self, page_iterator):
        self.page_iterator = page_iterator
        self.limit = inf
        self.current_page = None
        self.page_index = -1
        self.num_tweets = 0


