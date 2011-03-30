from datetime import datetime
import logging
import os
import random
import time

from forkd import Forkd


def worker():
    log = logging.getLogger('worker')
    log.info('[%s] getting going', os.getpid())
    try:
        for i in range(5):
            time.sleep(random.random()*2)
            time.sleep(1)
            log.info('[%s] %s', os.getpid(), datetime.utcnow())
            yield
    finally:
        log.info('[%s] cleaning up', os.getpid())


logging.basicConfig(level=logging.INFO)
manager = Forkd(worker, num_workers=1)
manager.run()
