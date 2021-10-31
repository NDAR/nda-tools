from queue import Queue
from threading import Thread

from NDATools.utils.Utils import get_traceback


class Worker(Thread):
    """ Thread executing tasks from a given tasks queue """

    def __init__(self, tasks):
        Thread.__init__(self)
        self.tasks = tasks
        self.daemon = True
        self.start()

    def run(self):
        while True:
            func, args, kargs = self.tasks.get()
            try:
                func(*args, **kargs)
            except Exception as e:
                # An exception happened in this thread
                print(str(e))
                print(get_traceback())

            finally:
                # Mark this task as done, whether an exception happened or not
                self.tasks.task_done()

class ThreadPool:
    """ Pool of threads consuming tasks from a queue """

    def __init__(self, num_threads, queue_size=None):
        queue_size = queue_size or num_threads * 100
        self.tasks = Queue(queue_size)
        for _ in range(num_threads):
            Worker(self.tasks)

    def add_task(self, func, *args, **kargs):
        """ Add a task to the queue """
        self.tasks.put((func, args, kargs))

    def map(self, func, args_list):
        """ Add a list of tasks to the queue """
        for args in args_list:
            self.add_task(func, args)

    def wait_completion(self):
        """ Wait for completion of all the tasks in the queue """
        self.tasks.join()