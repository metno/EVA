import shelve
import hashlib
import os
import os.path
import logging

import eva.exceptions


class Checkpoint(object):
    """
    Store all objects necessary for EVA to restart from previous state.
    Open and close the state db for each method call, to ensure that everything
    is always persisted.
    """

    def __init__(self, filename):
        self.db_filename = filename

        directory = os.path.dirname(filename)
        if not os.path.exists(directory):
            os.makedirs(directory)

    def set(self, object_):
        """
        Serialize any python object.
        """
        self._open()
        key = self.key(object_)
        self.checkpoint[key] = object_
        self._close()

    def key(self, object_):
        """
        Create a unique key for an object by hashing __repr__.
        """
        return hashlib.sha256(unicode(object_)).hexdigest()

    def delete(self, object_):
        """
        Remove serialized python object.
        """
        self._open()
        key = self.key(object_)

        if key not in self.checkpoint.keys():
            raise eva.exceptions.CheckpointKeyDoesntExist("Cannot delete object with key %s as it doesn't exist."
                                                          % key)
        del self.checkpoint[key]
        self._close()

    def load(self):
        """
        Return all serialized python objects.
        """
        self._open()
        objects = []
        for key in self.checkpoint.keys():
            objects.append(self.checkpoint[key])

        self._close()
        return objects

    def _close(self):
        """
        Close checkpoint db. The db file will be synced.
        """
        try:
            self.checkpoint.close()
        except IOError:
            logging.error("Could not close checkpoint file properly. Checkpoint probably not persisted.")
            raise

    def _open(self):
        """
        Open checkpoint db file in read/write mode.
        """
        try:
            self.checkpoint = shelve.open(self.db_filename)
        except IOError:
            logging.critical("Could not open checkpoint file")
            raise
