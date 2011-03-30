"""
Pre-forking process manager.
"""


import errno
import fcntl
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


# Worker messages.
WORKER_QUIT = 'Q'


class Forkd(object):
    """Pre-forking process manager.
    """

    def __init__(self, worker_func, num_workers=1):
        self.worker_func = worker_func
        self.num_workers = num_workers
        self._status = None
        self._signal_pipe = None
        self._workers = {}
        self._log = logging.getLogger('forkd')

    def run(self):
        """Run workers and block until no workers remain.
        """
        self._status = 'starting'
        self._setup()
        self._spawn_workers()
        self._status = 'running'
        self._loop()
        self._status = 'ended'

    def _shutdown(self):
        """Shutdown workers cleanly.
        """
        # Ignore if already shutting down.
        if self._status == 'shutdown':
            return
        self._log.info('[%s] shutting down', os.getpid())
        self._status = 'shutdown'
        # Set num_workers to 0 to avoid spawning any more children.
        self.num_workers = 0
        # Sent QUIT to all workers.
        for pid, worker in self._workers.iteritems():
            os.write(worker['pipe'][1], WORKER_QUIT)

    def _loop(self):
        """Loop, handling signals, until no workers exist.
        """
        while self._workers:
            try:
                signal_id = os.read(self._signal_pipe[0], 1)
                if not signal_id:
                    break
                # Call signal handler.
                handler = getattr(self, '_' + SIGNAL_IDS_REV[signal_id])
                handler()
            except OSError, e:
                if e.errno != errno.EINTR:
                    self._log.info('OSError %x: %s', e.errno, unicode(e))
                    raise

    def _setup(self):
        """Setup signals and master control pipe.
        """
        self._signal_pipe = os.pipe()
        for name in SIGNAL_IDS:
            self._signal(name)

    def _spawn_workers(self):
        """Spawn required number of worker processes.
        """
        for i in range(max(self.num_workers - len(self._workers), 0)):
            pid, pipe = self._spawn_worker()
            self._workers[pid] = {'pipe': pipe}
            self._log.info('[%s] started worker %s', os.getpid(), pid)

    def _spawn_worker(self):
        """Spawn a single worker process.
        """

        # Create worker control pipe. Read end is non-blocking so we can "peek" at it.
        worker_pipe = os.pipe()
        fcntl.fcntl(worker_pipe[0], fcntl.F_SETFL, fcntl.fcntl(worker_pipe[0], fcntl.F_GETFL) | os.O_NONBLOCK)

        # Fork process and return immediately if not new worker.
        pid = os.fork()
        if pid:
            return pid, worker_pipe

        # Get worker pid.
        pid = os.getpid()
        self._log.debug('[%s] worker running', pid)

        # Create worker.
        worker = self.worker_func()

        # Loop until either the worker ends or is shutdown.
        while True:
            # Read byte from worker pipe, if available.
            try:
                ch = os.read(worker_pipe[0], 1)
            except OSError, e:
                if e.errno != errno.EAGAIN:
                    raise
            else:
                if ch == WORKER_QUIT:
                    self._log.debug('[%s] received QUIT', pid)
                    break
            # Run worker.
            try:
                worker.next()
            except StopIteration:
                break
            except Exception, e:
                self._log.exception('[%s] exception in worker', pid)
                sys.exit(-1)

        # Exit worker process.
        self._log.debug('[%s] worker ending', pid)
        sys.exit(0)

    def _add_worker(self):
        """Add a new worker process.
        """
        self.num_workers += 1
        self._log.info('[%s] adding worker, num_workers=%d', os.getpid(), self.num_workers)
        self._spawn_workers()

    def _remove_worker(self):
        """Remove a worker process.
        """
        if self.num_workers == 0:
            return
        self.num_workers -= 1
        self._log.info('[%s] removing worker, num_workers=%d', os.getpid(), self.num_workers)

    def _signal(self, signame):
        """Install signal handler that routes the signal event to the pipe.
        """
        signal_id = SIGNAL_IDS[signame]
        def handler(signo, frame):
            os.write(self._signal_pipe[1], signal_id)
        signal.signal(getattr(signal, signame), handler)

    def _SIGCHLD(self):
        """Handle child termination.
        """
        self._log.debug('[%s] SIGCHLD', os.getpid())
        while self._workers:
            pid, status = os.waitpid(-1, os.WNOHANG)
            if not pid:
                break
            status = status >> 8
            self._log.info('[%s] worker %s ended with status: %s', os.getpid(), pid, status)
            worker = self._workers.pop(pid)
            os.close(worker['pipe'][0])
            os.close(worker['pipe'][1])
        self._spawn_workers()

    def _SIGINT(self):
        """Handle terminal interrupt.
        """
        self._log.debug('[%s] SIGINT', os.getpid())
        self._shutdown()

    def _SIGTERM(self):
        """Handle termination request.
        """
        self._log.debug('[%s] SIGTERM', os.getpid())
        self._shutdown()

    def _SIGUSR1(self):
        """Handle usr1 (add worker) request.
        """
        self._log.debug('[%s] SIGUSR1', os.getpid())
        self._add_worker()

    def _SIGUSR2(self):
        """Handle usr2 (remove worker) request.
        """
        self._log.debug('[%s] SIGUSR2', os.getpid())
        self._remove_worker()


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
