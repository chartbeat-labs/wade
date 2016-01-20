

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
    def __init__(self, capacity):
        self._capacity = capacity
        self._length = self._capacity + 1
        self._data = bytearray([0] * self._length)
        self._start = 0
        self._end = 0

    def __len__(self):
        return self.available_data()

    def __repr__(self):
        return 'CircularBuffer(%s, length=%d, free=%d, capacity=%d)' % \
                (self.peek_all(),
                 self.available_data(),
                 self.available_space(),
                 self._capacity)

    def _read(self, amount, commit):
        """Read up to amount and return a list. May return less data than
        requested when amount > available_data(). It is the caller's
        responsibility to check the length of the returned result list.

        @return: bytearray
        """
        if amount <= 0:
            raise CircularBufferError('Must request a positive amount of data')
        if not self.available_data():
            return bytearray()
        
        amount = min(amount, self.available_data())
        read_end = self._start + amount

        if read_end < self._length:
            ret = self._data[self._start:read_end]
        else:
            ret = self._data[self._start:] + self._data[:(read_end - self._length)]
        
        if commit:
            self.commit_read(amount)

        return ret

    def available_data(self):
        return (self._end - self._start) % self._length

    def available_space(self):
        return self._capacity - self.available_data()

    def commit_read(self, amount):
        self._start = (self._start + amount) % self._length

    def commit_write(self, amount):
        self._end = (self._end + amount) % self._length

    def read(self, amount):
        return self._read(amount, commit=True)

    def read_all(self):
        if not self.available_data():
            return bytearray()
        return self.read(self.available_data())

    def peek(self, amount):
        return self._read(amount, commit=False)

    def peek_all(self):
        if not self.available_data():
            return bytearray()
        return self.peek(self.available_data())

    def write(self, data):
        """Writes a string or bytes into the buffer if it fits.

        @param data, str or byte
        """
        amount = len(data)
        if amount > self.available_space():
            raise CircularBufferError(
                'Not enough space: %d requested, %d available' % \
                (amount, self.available_space()))

        write_end = self._end + amount

        if write_end < self._length:  # if no wrap around
            self._data[self._end:write_end] = data
        else: # if wrap around
            partition = self._length - self._end
            end_block = data[:partition]
            start_block = data[partition:]
            self._data[self._end:] = end_block  # write at end of buffer
            self._data[:len(start_block)] = start_block  # write leftover at beginning

        self.commit_write(amount)
