import eva.base.executor


class NullExecutor(eva.base.executor.BaseExecutor):
    """
    @brief Pretend to execute tasks, but don't actually do it.
    """

    def execute(self, job):
        self.logger.info("[%s] Faking job execution and setting exit code to zero.", job.id)
        job.exit_code = 0
        job.stdout = []
        job.stderr = []
