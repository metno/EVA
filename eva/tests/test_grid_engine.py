# coding: utf-8

import unittest
import logging

import productstatus.api

import eva.statsd
import eva.executor.grid_engine


BLANK_UUID = '00000000-0000-0000-0000-000000000000'


class TestGridEngineExecutor(unittest.TestCase):
    def setUp(self):
        self.env = {
            'EVA_GRIDENGINE_SSH_KEY_FILE': '/dev/null',
            'EVA_GRIDENGINE_SSH_HOST': 'localhost',
            'EVA_GRIDENGINE_SSH_USER': 'nobody',
        }
        self.productstatus_api = productstatus.api.Api('http://localhost:8000')
        self.logger = logging
        self.zookeeper = None
        self.statsd = eva.statsd.StatsDClient()
        self.executor = eva.executor.grid_engine.GridEngineExecutor(None, self.env, self.logger, self.zookeeper, self.statsd)

    def test_create_job_unique_id(self):
        compare = 'eva.y-h-gr34t--job.%s' % BLANK_UUID
        group_id = u'/yæh/gr34t/~job~/'
        id = eva.executor.grid_engine.create_job_unique_id(group_id, BLANK_UUID)
        self.assertEqual(id, compare)

    def test_parse_qacct_metrics(self):
        raw = """
        ==============================================================
        qname        operational.q
        hostname     c6320-5wszm62.int.met.no
        group        eventadapter
        owner        eventadapter
        project      NONE
        department   onlyroot
        jobname      eva.eva-ecmwf-delete-lustreab.aa129f56-9728-4f18-a141-c65bee7e9ae1
        jobnumber    1000000
        taskid       undefined
        account      sge
        priority     0
        qsub_time    Sat Apr 16 20:40:42 2016
        start_time   Sat Apr 16 20:40:43 2016
        end_time     Sat Apr 16 20:45:43 2016
        granted_pe   NONE
        slots        1
        failed       0
        exit_status  0
        ru_wallclock 0
        ru_utime     0.014
        ru_stime     0.011
        ru_maxrss    2884
        ru_ixrss     0
        ru_ismrss    0
        ru_idrss     0
        ru_isrss     0
        ru_minflt    846
        ru_majflt    0
        ru_nswap     0
        ru_inblock   0
        ru_oublock   24
        ru_msgsnd    0
        ru_msgrcv    0
        ru_nsignals  0
        ru_nvcsw     41
        ru_nivcsw    0
        cpu          0.025
        mem          0.000
        io           0.000
        iow          0.000
        maxvmem      0.000
        arid         undefined
        """
        lines = [x.lstrip() for x in raw.strip().splitlines()]
        metrics = eva.executor.grid_engine.parse_qacct_metrics(lines)
        self.assertDictEqual(metrics, {
            'metrics': {
                'grid_engine_run_time': 300000,
                'grid_engine_ru_utime': 14,
                'grid_engine_ru_stime': 11,
                'grid_engine_qsub_delay': 1000,
            },
            'tags': {
                'grid_engine_hostname': 'c6320-5wszm62.int.met.no',
                'grid_engine_qname': 'operational.q',
            },
        })
