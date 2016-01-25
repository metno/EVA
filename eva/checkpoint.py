import shelve
import os
import os.path
import logging


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

    def set(self, key, value):
        """
        Serialize any python object.
        """
        logging.info('Saving checkpoint for key \'%s\'', key)
        self._open()
        self.checkpoint[key] = value
        self._close()

    def get(self, key):
        """
        Return all serialized python objects.
        """
        self._open()
        if key in self.checkpoint:
            value = self.checkpoint[key]
        else:
            value = None

        self._close()
        return value

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
