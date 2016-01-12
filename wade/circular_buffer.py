import threading
import unittest
import copy


class CircularBufferError(Exception):
    pass


class CircularBuffer(object):
    """Unlike traditional circular buffers, this allows reading and writing
    multiple values at a time. Additionally, this object provides peeking and
    commiting reads. See test_circular_buffer.py for examples.

    The interface is almost identical to the following code but unlike it our
    implementation efficiently frees up space when commiting reads:
    http://c.learncodethehardway.org/book/ex44.html
    """
    def __init__(self, capacity=10):
        self.capacity = capacity
        self.length = self.capacity + 1
        self.data = [None] * self.length
        self.start = 0
        self.end = 0

    def __len__(self):
        return self.available_data()

    def __repr__(self):
        return 'CircularBuffer(%s, length=%d, free=%d, capacity=%d)' % \
                (self.peek_all(),
                 self.available_data(),
                 self.available_space(),
                 self.capacity)

    def _read(self, amount, commit):
        """Read up to amount and return a list. May return less data than
        requested when amount > available_data(). It is the caller's
        responsibility to check the length of the returned result list.

        @return: list(object)
        """
        if amount <= 0:
            raise CircularBufferError('Must request a positive amount of data')
        if not self.available_data():
            return []
        
        amount = min(amount, self.available_data())
        read_end = self.start + amount

        if read_end < self.length:
            ret = self.data[self.start:read_end]
        else:
            ret = self.data[self.start:] + self.data[:(read_end - self.length)]
        
        if commit:
            self.commit_read(amount)

        return ret

    def available_data(self):
        if self.start <= self.end:
            return self.end - self.start

        return self.length - (self.start - self.end)

    def available_space(self):
        return self.capacity - self.available_data()

    def commit_read(self, amount):
        self.start = (self.start + amount) % self.length

    def commit_write(self, amount):
        self.end = (self.end + amount) % self.length

    def read(self, amount):
        return self._read(amount, commit=True)

    def read_all(self):
        if not self.available_data():
            return []
        return self.read(self.available_data())

    def peek(self, amount):
        return self._read(amount, commit=False)

    def peek_all(self):
        if not self.available_data():
            return []
        return self.peek(self.available_data())

    def write(self, data):
        """Writes a list of objects into the buffer if it fits.

        @param data, list(object)
        """
        amount = len(data)
        if amount > self.available_space():
            raise CircularBufferError(
                'Not enough space: %d requested, %d available' % \
                (amount, self.available_space()))

        write_end = self.end + amount

        if write_end < self.length:  # if no wrap around
            self.data[self.end:write_end] = data
        else: # if wrap around
            partition = self.length - self.end
            end_block = data[:partition]
            start_block = data[partition:]
            self.data[self.end:] = end_block  # write at end of buffer
            self.data[:len(start_block)] = start_block  # write leftover at beginning

        self.commit_write(amount)
