import unittest


class TestImport(unittest.TestCase):
    def test_import_adapter(self):
        import eva.adapter  # NOQA

    def test_import_executor(self):
        import eva.executor  # NOQA

    def test_import_listener(self):
        import eva.listener  # NOQA
