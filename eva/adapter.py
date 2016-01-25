import eva.job


class BaseAdapter(object):
    """
    Adapters contain all the information and configuration needed to translate
    a Productstatus event into job execution.
    """

    def __init__(self, id, api, environment_variables):
        """
        @param id an identifier for the adapter; must be constant across program restart
        @param api Productstatus API object
        @param environment_variables Dictionary of EVA_* environment variables
        """
        self.id = id
        self.api = api
        self.env = environment_variables

    def match_event(self, event, resource):
        """
        @brief Check if the event and resource fits this adapter.
        @param event The message sent by the Productstatus server.
        @param resource The Productstatus resource referred to by the event.
        @returns a (possibly empty) list of jobs to be executed in parallel
        """
        raise NotImplementedError()

    def match_timeout(self):
        """
        @brief Called regularly when no events are received.
        @returns a (possibly empty) list of jobs to be executed in parallel
        """
        raise NotImplementedError()

    def finished_jobs(self, jobs):
        """
        @brief Called when one or more jobs have finished.
        @param jobs list of finished jobs
        @returns a (possibly empty) list of jobs to be executed in parallel
        """
        raise NotImplementedError()

    def set_state(self, state):
        """
        @brief Restore state of the adapter from serialization
        @param state state from serialization; might be None when starting up
        """
        raise NotImplementedError()

    def get_state(self):
        """
        @brief Get current state of the adapter for serialization
        @returns state for serialization
        """
        raise NotImplementedError()


class NullAdapter(BaseAdapter):
    """
    An adapter that matches nothing.
    """

    def __init__(self, *args):
        super(NullAdapter, self).__init__("NullAdapter", *args)

    def match_event(self, event, resource):
        return []

    def match_timeout(self):
        return []

    def finished_jobs(self, jobs):
        return []

    def set_state(self, state):
        pass

    def get_state(self):
        return None


class JobQueueAdapter(BaseAdapter):
    """
    An adapter with a job queue.
    """

    def __init__(self, *args):
        super(JobQueueAdapter, self).__init__(*args)
        self.job_queue = []

    def set_state(self, state):
        """
        @brief Restore job queue
        @param state job queue from serialization
        """
        if state is None:
            self.job_queue = []
        else:
            self.job_queue = state

    def get_state(self):
        """
        @brief Serialize job queue
        @returns job queue for serialization
        """
        return self.job_queue

    def enqueue_jobs(self, jobs):
        """
        Add a list of jobs to the job queue.
        @param jobs jobs to enqueue
        @returns jobs if the job queue was empty, else None
        """
        if len(jobs) == 0:
            return []
        self.job_queue.append(jobs)
        if len(self.job_queue) == 1:
            return jobs
        else:
            return []

    def finished_jobs(self, jobs):
        """
        Remove finished jobs from the job queue.
        @param jobs finished jobs
        @returns next queue element if all jobs in the first element have finished
        """
        for job in jobs:
            self.job_queue[0].remove(job)
        if len(self.job_queue[0]) > 0:
            return []
        del self.job_queue[0]
        if len(self.job_queue) == 0:
            return []
        else:
            return self.job_queue[0]


class TestDownloadAdapter(JobQueueAdapter):
    """
    An adapter that downloads any posted DataInstance using wget.
    """

    def __init__(self, *args):
        super(TestDownloadAdapter, self).__init__("TestDownloadAdapter", *args)

    def match_event(self, event, resource):
        if event.resource != 'datainstance':
            return []
        job = eva.job.Job()
        job.command = """#!/bin/bash -x
        echo "Running on host: `hostname`"
        echo "Working directory: `pwd`"
        cd %(destination)s
        echo "Productstatus DataInstance points to %(url)s"
        echo "Now downloading file..."
        wget %(url)s
        echo "Finished."
        """ % {
            'url': resource.url,
            'destination': self.env['EVA_TEST_DOWNLOAD_DESTINATION'],
        }
        return self.enqueue_job(job)

    def match_timeout(self):
        return []
