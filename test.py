from datetime import datetime
import logging
import os
import random
import sys
import time

from forkd import Forkd


def worker():
    count = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    log = logging.getLogger('worker')
    log.debug('[%s] getting going', os.getpid())
    try:
        for i in range(count):
            log.info('[%s] %s', os.getpid(), datetime.utcnow())
            yield
            time.sleep(random.SystemRandom().random()*2)
    finally:
        log.debug('[%s] cleaning up', os.getpid())


def main():
    logging.basicConfig(level=logging.INFO)
    forkd = Forkd(worker, num_workers=1)
    forkd.run()


if __name__ == '__main__':
    main()
