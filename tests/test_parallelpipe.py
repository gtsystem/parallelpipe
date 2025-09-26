import unittest
from parallelpipe import Stage, TaskException, stage, map_stage
from time import sleep

def t1(x, fail_at=None):
    """Produce values from the given input iterator.
    It will fail at fail_at if fail_at is not None"""
    for t in x:
        yield t
        if fail_at is not None and t == fail_at:
            raise ValueError("failed at %d" % fail_at)

def t2(input, n, fail_at=None):
    """Produce numbers from the given input iterator adding n.
    It will fail at fail_at if fail_at is not None"""
    for i, item in enumerate(input):
        yield item + n
        if fail_at is not None and item == fail_at:
            raise ValueError("failed at %d" % fail_at)

def t3(input, f):
    """Apply the aggregation function f to the input and return it"""
    yield f(input)

class T2(object):
    """Example of class task"""
    def __init__(self, n):
        self.n = n

    def produce(self, input):
        for item in input:
            yield item + self.n

class TestStage(unittest.TestCase):
    def setUp(self):
        pass

    def test_one(self):
        """Only producer configuration"""
        producer = Stage(t1, range(1000)).setup(workers=4, qsize=10)
        res = list(t for t in producer.results())

        self.assertEqual(max(res), 999)
        self.assertEqual(min(res), 0)
        self.assertEqual(len(res), 1000*4) # we are running 4 parallel producers

        # let's reuse a pipe again
        res = list(t for t in producer.results())

        self.assertEqual(max(res), 999)
        self.assertEqual(min(res), 0)
        self.assertEqual(len(res), 1000*4) # we are running 4 parallel producers

        # task with one result
        producer = Stage(t3, range(1000), sum).setup(workers=4, qsize=10)
        res = producer.execute()
        self.assertEqual(res, sum(range(1000)))

    def test_two(self):
        """Producer/Consumer configuration"""
        producer = Stage(t1, range(1000)).setup(workers=4, qsize=10)
        consumer = Stage(t2, 5).setup(workers=4, qsize=1000)
        pipe = producer | consumer
        res = list(t for t in pipe.results())

        self.assertEqual(max(res), 1004)
        self.assertEqual(min(res), 5)
        self.assertEqual(len(res), 1000*4) # we are running 4 parallel producers

        pipe = range(1000) | consumer
        res = list(t for t in pipe.results())
        
        self.assertEqual(max(res), 1004)
        self.assertEqual(min(res), 5)
        self.assertEqual(len(res), 1000) # we are running 4 parallel producers
        

    def test_two_class_instance(self):
        """Producer/Consumer configuration. One of the task is actually a method of a class
        instance"""
        job = T2(5).produce
        producer = Stage(t1, range(1000)).setup(workers=4, qsize=10)
        consumer = Stage(job).setup(workers=4, qsize=1000)
        pipe = producer | consumer
        res = list(t for t in pipe.results())

        self.assertEqual(max(res), 1004)
        self.assertEqual(min(res), 5)
        self.assertEqual(len(res), 1000*4) # we are running 4 parallel producers

    def test_two_reduce(self):
        """Producer/Reducer configuration"""
        producer = Stage(t1, range(1000)).setup(workers=4, qsize=10)
        reducer = Stage(t3, sum).setup(workers=1, qsize=3)
        pipe = producer | reducer
        res = list(t for t in pipe.results())

        expected = sum(range(1000)) * 4

        self.assertEqual(len(res), 1)
        self.assertEqual(res[0], expected)

        # let's try execute here..

        res = pipe.execute()
        self.assertEqual(res, expected)

    def test_three_reduce(self):
        """Producer/Mapper/Reducer configuration"""
        producer = Stage(t1, range(1000)).setup(workers=4, qsize=10)
        mapper = Stage(t2, 5).setup(workers=4, qsize=1000)
        reducer = Stage(t3, sum).setup(workers=2, qsize=3)
        pipe = producer | mapper | reducer
        res = list(t for t in pipe.results())

        expected = sum(range(5, 1005)) * 4

        self.assertEqual(len(res), 2)
        self.assertEqual(sum(res), expected)

    def test_exception_propagation(self):
        """The mapper will fail this time"""
        producer = Stage(t1, range(1000)).setup(workers=2, qsize=10)
        mapper = Stage(t2, 5, 200).setup(workers=6, qsize=1000)
        reducer = Stage(t3, sum).setup(workers=2, qsize=3)
        pipe = producer | mapper | reducer

        with self.assertRaisesRegex(TaskException, "failed at 200"):
            for res in pipe.results():
                pass


        producer = Stage(t1, range(1000), 10).setup(workers=2, qsize=10)
        pipe = producer | mapper | reducer

        with self.assertRaisesRegex(TaskException, "failed at 10"):
            for res in pipe.results():
                pass

    def test_task_decorator(self):
        @stage(workers=4)
        def my_task(it, even=True):
            condition = 0 if even else 1
            for item in it:
                if item % 2 == condition:
                    yield item
        
        @stage(workers=1)
        def consume(it):
            yield max(it)
        
        res = (range(1000) | my_task | consume).execute()
        self.assertEqual(res, 998)
        
        res = (range(1000) | my_task(even=False) | consume).execute()
        self.assertEqual(res, 999)

    def test_map_decorator(self):
        @map_stage(workers=4)
        def my_task(item, x):
            return item + x
        
        def my_task_fail(item, x):
            if item != x:
                raise Exception("failure")
            
            return item * 2
        
        @stage()
        def consume(n):
            yield sum(n)
        
        fail1 = map_stage(workers=4)(my_task_fail)
        fail2 = map_stage(workers=4, filter_errors=True)(my_task_fail)
        
        res = (range(1000) | my_task(5) | consume).execute()
        self.assertEqual(res, 504500)
        
        res = (range(1000) | fail2(5) | consume).execute()
        self.assertEqual(res, 10)
        
        with self.assertRaisesRegex(TaskException, "failure"):
            (range(1000) | fail1(5) | consume).execute()

    def test_slow_second_stage(self):
        
        @stage(workers=2)
        def mapit(it):
            for item in it:
                yield item + 1
        
        @stage(workers=1)
        def reduce(it):
            sleep(3)  # simulate a long startup time
            tot = 0
            for item in it:
                tot += item
                sleep(2)
                yield 5
            yield tot

        @stage(workers=2)
        def write(it):
            for item in it:
                yield item

        res = list(([1] | mapit | reduce | write).results())
        self.assertEqual(res, [5, 2])


if __name__ == '__main__':
    import multiprocessing
    multiprocessing.set_start_method('fork', force=False)
    unittest.main()

