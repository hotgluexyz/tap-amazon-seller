import signal
import time
from contextlib import contextmanager



class Timeout(Exception):
    def __init__(self, value="Timed Out"):
        self.value = value

    def __str__(self):
        return repr(self.value)


class InvalidResponse(Exception):
    pass

class RetriableError(Exception):
    pass


def timeout(seconds_before_timeout):
    def decorate(f):
        def handler(signum, frame):
            raise Timeout()

        def new_f(*args, **kwargs):
            old = signal.signal(signal.SIGALRM, handler)
            old_time_left = signal.alarm(seconds_before_timeout)
            if 0 < old_time_left < seconds_before_timeout:
                signal.alarm(old_time_left)
            start_time = time.time()
            try:
                result = f(*args, **kwargs)
            finally:
                if old_time_left > 0:
                    old_time_left -= time.time() - start_time
                signal.signal(signal.SIGALRM, old)
                signal.alarm(old_time_left)
            return result

        new_f.__name__ = f.__name__
        return new_f

    return decorate
