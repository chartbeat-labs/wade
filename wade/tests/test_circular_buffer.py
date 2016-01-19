#!/usr/bin/env python

import unittest

from wade.circular_buffer import CircularBuffer

class CircularBufferTest(unittest.TestCase):
    def test_fill_and_empty(self):
        """This test fills up the buffer with data, commits all of it, and
        ensures that the buffer empties
        """

        n = 256
        cb = CircularBuffer(n)
        vals = range(n)

        # Check starting state.
        self.assertEqual(cb.available_data(), 0)
        self.assertEqual(cb.available_space(), n)

        # Write n values to buffer to fill it up.
        for i, val in enumerate(vals):
            cb.write([val]) # write expects iterable.
            self.assertEqual(cb.available_data(), i+1)
            self.assertEqual(cb.available_space(), n-i-1)

        # Check that the buffer is full.
        self.assertEqual(cb.available_data(), n)
        self.assertEqual(cb.available_space(), 0)
        self.assertEqual(cb.peek_all(), bytearray(vals))

        # Commit all elements except for the last one.
        for i in xrange(len(vals)-1):
            cb.commit_read(1)
            self.assertEqual(cb.available_data(), n-i-1)
            self.assertEqual(cb.available_space(), i+1)

        # Commit the last element, check that we've now cleared the buffer and
        # returned to the start state.
        cb.commit_read(1)
        self.assertEqual(cb.available_data(), 0)
        self.assertEqual(cb.available_space(), n)
        self.assertEqual(cb.peek_all(), bytearray())

    def test_half_empty_and_refill(self):
        """Fills up the buffer, commits half of it, verifies state, fills buffer
        and verifies again"""
        n = 256
        cb = CircularBuffer(n)
        vals = range(n)
        reverse_vals = vals[::-1]

        # Fill buffer and verify
        cb.write(vals)
        self.assertEqual(cb.available_space(), 0)
        self.assertEqual(cb.available_data(), n)
        self.assertEqual(cb.peek_all(), bytearray(vals))

        # commit half of the data
        half = n/2
        cb.commit_read(half)

        # verify empty space
        self.assertEqual(cb.available_space(), half)
        self.assertEqual(cb.available_data(), half)
        self.assertEqual(cb.peek_all(), bytearray(vals[half:]))

        # now fill buffer up again and verify data.
        cb.write(reverse_vals[:half])
        self.assertEqual(cb.peek_all(), bytearray(vals[half:] + reverse_vals[:half]))

        # verify it's full
        self.assertEqual(cb.available_space(), 0)
        self.assertEqual(cb.available_data(), n)

        # drain again and verify empty
        cb.commit_read(n)
        self.assertEqual(cb.available_space(), n)
        self.assertEqual(cb.available_data(), 0)
