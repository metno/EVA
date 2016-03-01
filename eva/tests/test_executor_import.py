import unittest


class TestImport(unittest.TestCase):
    def test_import_adapter_base(self):
        from eva.adapter import BaseAdapter

    def test_import_adapter_null(self):
        from eva.adapter import NullAdapter

    def test_import_adapter_test_executor(self):
        from eva.adapter import TestExecutorAdapter

    def test_import_adapter_download(self):
        from eva.adapter import DownloadAdapter

    def test_import_adapter_fimex_grib_to_netcdf(self):
        from eva.adapter import FimexGRIB2NetCDFAdapter

    def test_import_executor_null(self):
        from eva.executor import NullExecutor

    def test_import_executor_shell(self):
        from eva.executor import ShellExecutor

    def test_import_executor_grid_engine(self):
        from eva.executor import GridEngineExecutor
