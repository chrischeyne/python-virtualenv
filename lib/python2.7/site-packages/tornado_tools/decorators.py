#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = "Gregory Sitnin <sitnin@gmail.com>"
__copyright__ = "Gregory Sitnin, 2011"


import functools
import logging


def https_only(method):
    """Decorate request handler's method with this decorator to ensure request
    will be served only via HTTPS.

    Because decorator is using `self.request.protocol` variable make sure
    ``xheaders`` is set to `True`::

        tornado.httpserver.HTTPServer(tornado.web.Application(urls, **settings), xheaders=True)
    """
    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        if self.request.protocol != "https":
            logging.warning("HTTPS only url requested via HTTP: %s"%self.request.full_url())
            self.redirect(self.request.full_url().replace("http", "https"))
        else:
            return method(self, *args, **kwargs)
    return wrapper

