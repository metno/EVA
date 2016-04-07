import eva.base.executor


class NullExecutor(eva.base.executor.BaseExecutor):
    """!
    @brief Pretend to execute tasks, but don't actually do it.
    """

    def execute(self, job):
        job.logger.info("Faking job execution and setting exit code to zero.")
        job.set_status(job.INITIALIZED)
        job.set_status(job.COMPLETE)
        job.exit_code = 0
        job.stdout = []
        job.stderr = []
