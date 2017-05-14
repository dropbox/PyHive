# This file taken from https://github.com/cloudera/impyla version 0.13.8

from puresasl.client import SASLClient, SASLError
from contextlib import contextmanager

@contextmanager
def error_catcher(self, Exc = Exception):
    try:
        self.error = None
        yield
    except Exc as e:
        self.error = e.message


class PureSASLClient(SASLClient):
    def __init__(self, *args, **kwargs):
        self.error = None
        super(PureSASLClient, self).__init__(*args, **kwargs)

    def start(self, mechanism):
        with error_catcher(self, SASLError):
            if isinstance(mechanism, list):
                self.choose_mechanism(mechanism)
            else:
                self.choose_mechanism([mechanism])
            return True, self.mechanism, self.process()
        # else
        return False, mechanism, None

    def encode(self, incoming):
        with error_catcher(self):
            return True, self.unwrap(incoming)
        # else
        return False, None

    def decode(self, outgoing):
        with error_catcher(self):
            return True, self.wrap(outgoing)
        # else
        return False, None

    def step(self, challenge):
        with error_catcher(self):
            return True, self.process(challenge)
        # else
        return False, None

    def getError(self):
        return self.error
