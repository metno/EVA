"""!
Collection of all adapters, for easy access when configuring EVA.

_DO NOT_ import anything from here when subclassing adapters!
"""

from eva.base.adapter import BaseAdapter  # NOQA
from eva.adapter.checksum import ChecksumVerificationAdapter  # NOQA
from eva.adapter.cwf import CWFAdapter  # NOQA
from eva.adapter.delete import DeleteAdapter  # NOQA
from eva.adapter.distribution import DistributionAdapter  # NOQA
from eva.adapter.download import DownloadAdapter  # NOQA
from eva.adapter.example import ExampleAdapter  # NOQA
from eva.adapter.fimex import FimexAdapter  # NOQA
from eva.adapter.fimex_fill_file import FimexFillFileAdapter  # NOQA
from eva.adapter.fimex_grib_to_netcdf import FimexGRIB2NetCDFAdapter  # NOQA
from eva.adapter.gridpp import GridPPAdapter  # NOQA
from eva.adapter.null import NullAdapter  # NOQA
from eva.adapter.test_executor import TestExecutorAdapter  # NOQA
from eva.adapter.thredds import ThreddsAdapter  # NOQA
