#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = "Gregory Sitnin <sitnin@gmail.com>"
__copyright__ = "Gregory Sitnin, 2011"


from uuid import uuid1


SessionException = Exception


class AbstractSession(object):
    def __init__(self, conn, session_id=None, auto=True, collection="sessions"):
        self.data = dict()
        self.conn = conn
        self.collection = collection
        self.session_id = session_id if session_id else unicode(uuid1())
        self.auto = auto
        if self.auto:
            self.load()

    def save(self):
        raise SessionException("Concrete .save() method is not implemented")

    def load(self):
        raise SessionException("Concrete .load() method is not implemented")

    def get(self, key, default=None):
        return self.data[key] if self.data.has_key(key) else default

    def set(self, key, value):
        self.data[key] = value
        if self.auto:
            self.save()
        return value

    def delete(self, key):
        del self.data[key]
        if self.auto:
            self.save()

try:
    import pymongo

    class MongoSession(AbstractSession):
        def save(self):
            self.conn[self.collection].save({"_id": self.session_id}, self.data)

        def load(self):
            self.data = self.conn[self.collection].find_one({"_id": self.session_id})

except ImportError:
    pass
