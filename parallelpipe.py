"""This class provide a transparent way to use multi step map reduce task.

         / map - map2 - reduce
producer - map - map2 /
         \ map /
"""
from multiprocessing import Process, Queue
from time import sleep
import inspect
import collections

class EXIT:
    """This represent a unique value to be sent in the queue to stop the iteration"""
    pass

def iterqueue(queue, expected):
    """Iterate all value from the queue until the ``expected`` number of EXIT elements is
    received"""
    while expected > 0:
        for item in iter(queue.get, EXIT):
            yield item
        expected -= 1

class Task(Process):
    """Represent one single task executing a callable in a subrocess"""
    def __init__(self, callable, args=(), kwargs={}):
        super(Task, self).__init__()
        self._callable = callable
        self._args = args
        self._kwargs = kwargs
        self._que_in = None
        self._que_out = None
        self._que_err = None

    def set_in(self, que_in, num_senders):
        """Set the queue in input and the number of parallel tasks that send inputs"""
        self._que_in = que_in
        self._num_senders = num_senders

    def set_out(self, que_out, num_followers):
        """Set the queue in output and the number of parallel tasks that follow"""
        self._que_out = que_out
        self._num_followers = num_followers

    def set_err(self, que_err):
        """Set the error queue. We push here all the error we get"""
        self._que_err = que_err

    def _consume(self):
        """Return the input iterator to be consumed or None if this is a producer job"""
        if self._que_in:
            return iterqueue(self._que_in, self._num_senders)
        else:
            return None

    def run(self):
        """Execute the task on all the input and send the needed number of EXIT at the end"""
        input = self._consume()
        put_item = self._que_out.put
        
        try:
            if input is None: # producer
                res = self._callable(*self._args, **self._kwargs)
            else:
                res = self._callable(input, *self._args, **self._kwargs)
            if res != None:
                for item in res:
                    put_item(item)

        except Exception as e:
            # we catch an error, we send on the error que, we consume the input and we exit
            # consuming the input queue avoid to keep running processes before exiting with
            # errors
            self._que_err.put((self.name, e))
            if input is not None:
                for i in input:
                    pass
            raise

        finally:
            for i in range(self._num_followers):
                put_item(EXIT)
            self._que_err.put(EXIT)

class Stage(object):
    """Represent a pool of parallel tasks that perform the same type of action on the input."""
    def __init__(self, target, *args, **kwargs):
        if not callable(target):
            raise TypeError("Target is not callable")
        self.qsize = 0 # output queue size
        self.workers = 1
        self._processes = None
        self._target = target
        self._args = args
        self._kwargs = kwargs
        if hasattr(target, '__self__'):
            self.target_name = "%s.%s" % (
                target.__self__.__class__.__name__,
                target.__name__
            )
        else:
            self.target_name = target.__name__

    def setup(self, workers=1, qsize=0):
        """Setup the pool parameters like number of workers and output queue size"""
        if workers <= 0:
            raise ValueError("workers have to be greater then zero")
        if qsize < 0:
            raise ValueError("qsize have to be greater or equal zero")
        self.qsize = qsize # output que size
        self.workers = workers
        return self

    @property
    def processes(self):
        """Initialise and return the list of processes associated with this pool"""
        if self._processes is None:
            self._processes = []
            for p in range(self.workers):
                t = Task(self._target, self._args, self._kwargs)
                t.name = "%s-%d" % (self.target_name, p)
                self._processes.append(t)

        return self._processes

    def set_in(self, que_in, num_senders):
        """Set the queue in input and the number of parallel tasks that send inputs"""
        for p in self.processes:
            p.set_in(que_in, num_senders)

    def set_out(self, que_out, num_followers):
        """Set the queue in output and the number of parallel tasks that follow"""
        for p in self.processes:
            p.set_out(que_out, num_followers)

    def set_err(self, que_err):
        """Set the error queue. We push here all the error we get"""
        for p in self.processes:
            p.set_err(que_err)

    def _start(self):
        """Start all the subprocesses"""
        for p in self.processes:
            p.start()

    def _join(self):
        """Wait for all the subprocesses to finish"""
        for p in self.processes:
            p.join()
        self._processes = None # this ensure we can reuse this taskpool again if needed

    def __str__(self):
        return "%s(x%d)" % (self.target_name, self.workers)

    __repr__ = __str__

    def __or__(self, b):
        return Pipeline([self, b])

    def __ror__(self, b):
        if isinstance(b, collections.Iterable):
            def producer(x):
                return x
            return Stage(producer, b) | self
        raise ValueError("Pipe input have to be iterable")

    def results(self):
        """Create a single pooltask group and return all results on the main thread"""
        return Pipeline([self]).results()

    def execute(self):
        """Create a single pooltask group and return the result on the main thread"""
        return Pipeline([self]).execute()

    def __call__(self, *args, **kwargs):
        self._args = args
        self._kwargs = kwargs
        return self

def stage(workers=1, qsize=0):
    def decorator(f):
        return Stage(f).setup(workers=workers, qsize=qsize)
    return decorator

def map_stage(workers=1, qsize=0, filter_errors=False):
    def decorator(f):
        if filter_errors:
            def map_task(it, *args, **argv):
                for item in it:
                    try:
                        yield f(item, *args, **argv)
                    except Exception as e:
                        pass
        else:
            def map_task(it, *args, **argv):
                for item in it:
                    yield f(item, *args, **argv)
        map_task.__name__ = "pipe_map-%s" % f.__name__
        return Stage(map_task).setup(workers=workers, qsize=qsize)
    return decorator

class TaskException(Exception):
    """Base class of exception propagated from one of the tasks"""
    pass

class Pipeline(list):
    """Represent an ordered list of connected Stages"""
    def __or__(self, b):
        if isinstance(b, Stage):
            self.append(b)
        elif isinstance(b, Pipeline):
            self.extend(b)
        return self

    def results(self):
        """Start all the tasks and return data on an iterator"""
        tt = None
        for i, tf in enumerate(self[:-1]):
            tt = self[i+1]
            q = Queue(tf.qsize)
            tf.set_out(q, tt.workers)
            tt.set_in(q, tf.workers)

        if tt is None: # we have only one pool
            tt = self[0]
        q = Queue(tt.qsize)
        err_q = Queue()
        tt.set_out(q, 1)

        for t in self:
            t.set_err(err_q)
            t._start()

        for item in iterqueue(q, tt.workers):
            yield item

        errors = list(iterqueue(err_q, sum(t.workers for t in self)))

        for t in self:
            t._join()

        if len(errors) > 0:
            task_name, ex = errors[0]
            if len(errors) == 1:
                msg = 'The task "%s" raised %s' % (task_name, repr(ex),)
            else:
                msg = '%d tasks raised an exeption. First error reported on task "%s": %s' % (len(errors), task_name, repr(ex))

            raise TaskException(msg)

    def execute(self):
        """Start all the tasks and return the result"""
        l = None
        for l in self.results():
            pass
        return l
