import numbers
import six
from pystachio import String, Integer, Float, Map, List, Struct, Required

'''
https://github.com/twosigma/Cook/blob/master/scheduler/docs/scheduler-rest-api.asc
'''

class Job(Struct):
    name = Required(String)
    uuid = Required(String)
    priority = Required(Integer)
    command = Required(String)
    max_retries = Required(Integer)
    max_runtime = Required(Integer)
    cpus = Float
    mem = Float
    gpus = Integer
    ports = Integer
    uris = List(FetchURI)

class Job(object):
    __slots__ = ['uuid', 'command', 'name', 'priority', 'max_retries', 'max_runtime', 'cpus', 'mem', 'gpus', 'ports', 'uris', 'env']

    def __init__(self, name, uuid, priority, command, max_retries, **kwargs):
        self.name = name
        self.uuid = uuid
        self.priority = priority
        self.command = command
        self.max_retries = max_retries
        self.max_runtime = kwargs.get('max_runtime', None)
        self.cpus = kwargs.get('cpus', None)
        self.mem = kwargs.get('mem', None)
        self.gpus = kwargs.get('gpus', None)
        self.ports = kwargs.get('ports', None)
        if 'uris' in kwargs:
            uris = kwargs['uris']
            if not isinstance(uris, list):
                raise TypeError('Expected list for "uris", got {}'.format(type(uris)))
            self.uris = list(map(to_uri, uris))
        else:
            self.uris = None
        self.env = kwargs.get('env', None)
        self.ensure_valid()

    def ensure_valid(self):
        check_prop('uuid', self.uuid, six.string_types, True)
        check_prop('command', self.command, six.string_types, True)
        check_prop('name', self.name, six.string_types, True)
        check_prop('priority', self.priority, six.integer_types, True)
        check_prop('max_retries', self.max_retries, six.integer_types, True)
        check_prop('max_runtime', self.max_runtime, six.integer_types)
        check_prop('cpus', self.cpus, numbers.Number)
        check_prop('mem', self.mem, numbers.Number)
        check_prop('gpus', self.gpus, six.integer_types)
        check_prop('ports', self.ports, six.integer_types)
        check_prop('uris', self.uris, list)
        if self.uris is not None:
            for uri in self.uris:
                if not isinstance(uri, FetchURI):
                    raise TypeError('Non-FetchURI in "uris": {}'.format(uri))
        check_prop('env', self.env, dict)
        for key, value in six.iteritems(self.env):
            if not isinstance(key, six.string_types):
                raise TypeError('Non-string key in "env": {}'.format(key))
            if not isinstance(value, six.string_types):
                raise TypeError('Non-string value in "env": {}'.format(value))

    def to_json(self):
        json = {'uuid': self.uuid, 'command': self.command, 'name': self.name, 'priority': self.priority, 'max_retries': self.max_retries}
        if self.max_runtime is not None:
            json['max_runtime'] = self.max_runtime
        if self.cpus is not None:
            json['cpus'] = self.cpus
        if self.mem is not None:
            json['mem'] = self.mem
        if self.gpus is not None:
            json['gpus'] = self.gpus
        if self.ports is not None:
            json['ports'] = self.ports
        if self.uris is not None:
            json['uris'] = list(map(lambda uri: uri.to_json, self.uris))
        if self.env is not None:
            json['env'] = self.env
        return json

    def __repr__(self):
        return 'Job(uuid={0.uuid}, name={0.name}, command={0.command}, priority={0.priority}, max_retries={0.max_retries}, max_runtime={0.max_runtime}, cpus={0.cpus}, gpus={0.gpus}, mem={0.mem}, ports={0.ports}, uris={0.uris}, env={0.env})'.format(self)


class FetchURI(object):
    __slots__ = ['value', 'executable', 'extract', 'cache']

    def __init__(self, value, **kwargs):
        self.value = value
        self.executable = kwargs.get('executable', None)
        self.extract = kwargs.get('extract', None)
        self.cache = kwargs.get('cache', None)
        self.ensure_valid()

    def ensure_valid(self):
        check_prop('value', self.value, six.string_types, True)
        check_prop('executable', self.executable, bool)
        check_prop('extract', self.extract, bool)
        check_prop('cache', self.cache, bool)

    def to_json(self):
        json = {'value': self.value}
        if self.executable is not None:
            json['executable'] = self.executable
        if self.extract is not None:
            json['extract'] = self.extract
        if self.cache is not None:
            json['cache'] = self.cache
        return json

    def __repr__(self):
        return 'FetchURI(value={0.value}, extract={0.extract}, executable={0.executable}, cache={0.cache})'.format(self)

class JobStatus(object):
    __slots__ = ['uuid', 'cpus', 'mem', 'gpus', 'framework_id', 'status', 'instances']

    def __init__(self, uuid, framework_id, status, instances, **kwargs):
        self.uuid = uuid
        self.framework_id = framework_id
        self.status = status
        self.instances = instances
        self.cpus = kwargs.get('cpus', None)
        self.mem = kwargs.get('mem', None)
        self.gpus = kwargs.get('gpus', None)

    def __repr__(self):
        return 'JobStatus(uuid={0.uuid}, cpus={0.cpus}, mem={0.mem}, gpus={0.gpus}, framework_id={0.framework_id}, status={0.status}, instances={0.instances})'.format(self)

    @staticmethod
    def from_json(json):
        return JobStatus(
            uuid=json[u'uuid'],
            framework_id=json[u'framework_id'],
            status=json[u'status'],
            instances=[JobInstance.from_json(i) for i in json[u'instances']],
            cpus=json.get(u'cpus', None),
            mem=json.get(u'mem', None),
            gpus=json.get(u'gpus', None)
        )

class JobInstance(object):
    __slots__ = ['start_time', 'end_time', 'task_id', 'hostname', 'ports', 'slave_id', 'executor_id', 'status', 'output_url']

    def __init__(self, status, **kwargs):
        self.status = status
        self.start_time = kwargs.get('start_time', None)
        self.end_time = kwargs.get('end_time', None)
        self.task_id = kwargs.get('task_id', None)
        self.hostname = kwargs.get('hostname', None)
        self.ports = kwargs.get('ports', None)
        self.slave_id = kwargs.get('slave_id', None)
        self.executor_id = kwargs.get('executor_id', None)
        self.output_url = kwargs.get('output_url', None)

    def __repr__(self):
        return 'JobInstance(start_time={0.start_time}, end_time={0.end_time}, task_id={0.task_id}, hostname={0.hostname}, ports={0.ports}, slave_id={0.slave_id}, executor_id={0.executor_id}, status={0.status}, output_url={0.output_url})'.format(self)

    @staticmethod
    def from_json(json):
        return JobInstance(
            status=json.get(u'status', None),
            start_time=json.get(u'start_time', None),
            end_time=json.get(u'end_time', None),
            task_id=json.get(u'task_id', None),
            hostname=json.get(u'hostname', None),
            ports=json.get(u'ports', None),
            slave_id=json.get(u'slave_id', None),
            executor_id=json.get(u'executor_id', None),
            output_url=json.get(u'output_url', None)
        )

def check_prop(name, value, types, required=False):
    if value is not None:
        if not isinstance(value, types):
            raise TypeError('Expected {} for "{}", got {}'.format(types, name, type(value)))
    elif required:
        raise KeyError('"{}" is required'.format(name))

def to_uri(uri):
    if isinstance(uri, FetchURI):
        return uri
    elif isinstance(uri, dict):
        return FetchURI(**uri)
    else:
        raise TypeError('Expected FetchURI or dict, got {}'.format(type(uri)))
