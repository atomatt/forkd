import errno
import logging
import os
import select
import signal
import sys
import time


# Signals trapped and their single-byte identifier when sent through pipe.
SIGNAL_IDS = {
    'SIGCHLD': 'C',
    'SIGINT':  'I',
    'SIGUSR1': '1',
    'SIGUSR2': '2',
    'SIGTERM': 'T',
}
SIGNAL_IDS_REV = dict((v, k) for (k, v) in SIGNAL_IDS.iteritems())


class Forkd(object):

    def __init__(self, worker_func, num_workers=1):
        self.worker_func = worker_func
        self.num_workers = num_workers
        self._status = None
        self._pipe = None
        self._workers = {}
        self._log = logging.getLogger('forkd')

    def run(self):
        self._status = 'starting'
        self.setup()
        self._spawn_workers()
        self._status = 'running'
        self.loop()
        self._status = 'ended'

    def shutdown(self):
        if self._status == 'shutdown':
            return
        self._log.info('[%s] shutting down', os.getpid())
        self._status = 'shutdown'
        self.num_workers = 0

    def loop(self):
        while self._workers:
            try:
                signal_id = os.read(self._pipe[0], 1)
                if not signal_id:
                    break
                # Call signal handler.
                handler = getattr(self, '_' + SIGNAL_IDS_REV[signal_id])
                handler()
            except OSError, e:
                if e.errno != errno.EINTR:
                    self._log.info('OSError %x: %s', e.errno, unicode(e))
                    raise

    def setup(self):
        self._pipe = os.pipe()
        for name in SIGNAL_IDS:
            self._signal(name)

    def _spawn_workers(self):
        for i in range(max(self.num_workers - len(self._workers), 0)):
            pid = self._spawn_worker()
            self._workers[pid] = {}
            self._log.info('[%s] start worker %s', os.getpid(), pid)

    def _spawn_worker(self):

        pid = os.fork()
        if pid:
            return pid

        pid = os.getpid()
        self._log.debug('[%s] worker running', pid)
        worker = self.worker_func()
        while True:
            try:
                worker.next()
            except StopIteration:
                break
        self._log.debug('[%s] worker ending', pid)
        sys.exit(0)

    def _signal(self, signame):
        """Install signal handler that routes the signal event to the pipe.
        """
        signal_id = SIGNAL_IDS[signame]
        def handler(signo, frame):
            os.write(self._pipe[1], signal_id)
        signal.signal(getattr(signal, signame), handler)

    def _SIGCHLD(self):
        """Handle child termination.
        """
        self._log.debug('[%s] SIGCHLD', os.getpid())
        while self._workers:
            pid, status = os.waitpid(-1, os.WNOHANG)
            if not pid:
                break
            self._log.info('[%s] worker %s ended with status: %s', os.getpid(), pid, status)
            del self._workers[pid]
        self._spawn_workers()

    def _SIGINT(self):
        """Handle terminal interrupt.
        """
        self._log.debug('[%s] SIGINT', os.getpid())
        self.shutdown()

    def _SIGTERM(self):
        """Handle termination request.
        """
        self._log.debug('[%s] SIGTERM', os.getpid())
        self.shutdown()

    def _SIGUSR1(self):
        self.num_workers += 1
        self._log.info('[%s] adding worker, num_workers=%d', os.getpid(), self.num_workers)
        self._spawn_workers()

    def _SIGUSR2(self):
        self.num_workers -= 1
        self._log.info('[%s] removing worker, num_workers=%d', os.getpid(), self.num_workers)
        self._spawn_workers()


def main():
    def test():
        from datetime import datetime
        import random
        log = logging.getLogger('test')
        for i in range(3):
            time.sleep(random.random()*2)
            log.info('[%s] %s', os.getpid(), datetime.utcnow())
            yield
    manager = Forkd(test, num_workers=3)
    manager.run()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
