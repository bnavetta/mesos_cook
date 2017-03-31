import requests
import six

from .model import *

class CookClient(object):
    __slots__ = ['base_uri', 'username', 'password']

    def __init__(self, base_uri, username, password):
        self.base_uri = base_uri
        self.username = username
        self.password = password

    @property
    def _auth(self):
        return (self.username, self.password)

    @property
    def _rawscheduler(self):
        return '{}/rawscheduler'.format(self.base_uri)

    def launch(self, jobs):
        if isinstance(jobs, list):
            jobs_json = [job.to_json() for job in jobs]
        elif isinstance(jobs, Job):
            jobs_json = [jobs.to_json()]
        else:
            raise TypeError("Must be a Job instance or list of Job instances: {}".format(jobs))
        r = requests.post(self._rawscheduler, auth=self._auth, json={ "jobs": jobs_json })
        r.raise_for_status()
        return r.text

    def status(self, jobs):
        if isinstance(jobs, six.string_types):
            jobs = [jobs]
        if not isinstance(jobs, list):
            raise TypeError('Must be a job UUID or list of job UUIDS: {}'.format(jobs))

        r = requests.get(self._rawscheduler, auth=self._auth, params={'job': jobs})
        r.raise_for_status()
        return [JobStatus.from_json(job) for job in r.json()]

    def delete(self, jobs):
        if isinstance(jobs, list):
            jobs_json = [job.to_json() for job in jobs]
        elif isinstance(jobs, Job):
            jobs_json = [jobs.to_json()]
        else:
            raise TypeError("Must be a Job instance or list of Job instances: {}".format(jobs))
        r = requests.delete(self._rawscheduler, auth=self._auth, params={'job': jobs})
        r.raise_for_status()
        return r.json()

    def retry_job(self, job, retries):
        r = requests.post('{}/retry'.format(self.base_uri), auth=self._auth, params={'job': job, 'retries': retries})
        r.raise_for_status()
        return r.json()

    def list_jobs(self, user, states, start_ms=None, end_ms=None, limit=None):
        if isinstance(states, six.string_types):
            state = states
        elif isinstance(states, list):
            state = '+'.join(states)
        else:
            raise TypeError('"states" must be a string or list of strings')
        params = {'user': user, 'state': state}
        if start_ms is not None:
            params['start_ms'] = start_ms
        if end_ms is not None:
            params['end_ms'] = end_ms
        if limit is not None:
            params['limit'] = limit

        r = requests.get('{}/list'.format(self.base_uri), auth=self._auth, params=params)
        r.raise_for_status()
        return [JobStatus.from_json(job) for job in r.json()]
