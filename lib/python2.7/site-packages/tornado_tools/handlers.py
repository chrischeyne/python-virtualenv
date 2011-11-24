#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = "Gregory Sitnin <sitnin@gmail.com>"
__copyright__ = "Gregory Sitnin, 2011"


import tornado.web
import logging
from pprint import pformat


class AppStatus(tornado.web.RequestHandler):
    def test(self, reply):
        """Override to implement concrete tests.

        Reply dictionary must consists three keys which values must be lists or tuples:

        ``reply["info"]`` — list containing any usefull information about web application state

        ``reply["warnings"]`` — anything that looks bad but doesn't affect application

        ``reply["errors"]`` — list of errors

        `reply` dictionary is pre-filled, so it's not a good idea to ignore existing values.
        """
        reply["warnings"].append("Webapp status tests are not implemented, yet")

    def get(self, format):
        reply = {"overall": True, "info": list(), "warnings": list(), "errors": list()}

        reply["info"].append({"tornado": tornado.version})

        self.test(reply)

        if format == "json":
            self.set_header("Content-Type", "application/json")
            self.write(json.dumps(reply))
        else:
            self.set_header("Content-Type", "text/plain")
            self.write(pformat(reply))


AppStatusUrl = (r"/status\.(json|txt)", AppStatus)
