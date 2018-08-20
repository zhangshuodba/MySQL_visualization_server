from concurrent import futures


class BaseWorker(object):

    _thread_pool = futures.ThreadPoolExecutor(10)

    def job(self):
        raise NotImplemented("base worker job method should be implemented")

    def __call__(self, *args, **kwargs):
        self.run()

    def run(self):
        self._thread_pool.submit(self.job)
