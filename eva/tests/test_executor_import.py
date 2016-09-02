import unittest


class TestImport(unittest.TestCase):
    def test_import_adapter_base(self):
        from eva.adapter import BaseAdapter  # NOQA

    def test_import_adapter_null(self):
        from eva.adapter import NullAdapter  # NOQA

    def test_import_adapter_test_executor(self):
        from eva.adapter import TestExecutorAdapter  # NOQA

    def test_import_adapter_download(self):
        from eva.adapter import DownloadAdapter  # NOQA

    def test_import_adapter_delete(self):
        from eva.adapter import DeleteAdapter  # NOQA

    def test_import_adapter_example(self):
        from eva.adapter import ExampleAdapter  # NOQA

    def test_import_adapter_fimex_grib_to_netcdf(self):
        from eva.adapter import FimexGRIB2NetCDFAdapter  # NOQA

    def test_import_adapter_fimex(self):
        from eva.adapter import FimexAdapter  # NOQA

    def test_import_executor_null(self):
        from eva.executor import NullExecutor  # NOQA

    def test_import_executor_shell(self):
        from eva.executor import ShellExecutor  # NOQA

    def test_import_executor_grid_engine(self):
        from eva.executor import GridEngineExecutor  # NOQA

    def test_import_listener_rpc(self):
        from eva.listener import RPCListener  # NOQA

    def test_import_listener_productstatus(self):
        from eva.listener import ProductstatusListener  # NOQA
