from __future__ import division
import xdrlib
import socket
from uuid import UUID
from datetime import datetime, tzinfo, timedelta


class UTC(tzinfo):
    ZERO = timedelta(0)

    def utcoffset(self, dt):
        return UTC.ZERO

    def tzname(self, dt):
        return "UTC"

    def dst(self, dt):
        return UTC.ZERO

_utc = UTC()


class XDRSock(object):
    def __init__(self, sock):
        self.__s = sock
        self.__p = xdrlib.Packer()
        self.__u = xdrlib.Unpacker("")

    def receive(self, num_bytes):
        b = b""
        while len(b) < num_bytes:
            # Python 2.7 socket recv raises OverflowError for lengths >= 2 GiB.
            t = self.__s.recv(min(num_bytes - len(b), (1024 ** 3 * 2) - 1))
            if len(t) == 0:
                raise EOFError
            b += t
        return b

    def send_u32(self, integer):
        self.__p.reset()
        self.__p.pack_uint(integer)
        self.__s.sendall(self.__p.get_buffer())

    def receive_u32(self):
        """
        Return a 32-bit integer

        Note that 8 and 16 bit integers are padded to 32 bits, so they are also
        received with this function.
        """
        b = self.receive(4)
        self.__u.reset(b)
        return self.__u.unpack_uint()

    def send_u64(self, integer):
        self.__p.reset()
        self.__p.pack_uhyper(integer)
        self.__s.sendall(self.__p.get_buffer())

    def receive_u64(self):
        return (self.receive_u32() << 32) | self.receive_u32()

    def send_fixed_string(self, length, string):
        self.__p.reset()
        self.__p.pack_fstring(length, string)
        self.__s.sendall(self.__p.get_buffer())

    def receive_fixed_string(self, length):
        padded_length = length
        # Round up to multiple of 4 to include padding.
        if length % 4:
            padded_length += 4 - (length % 4)
        b = self.receive(padded_length)
        self.__u.reset(b)
        return self.__u.unpack_fstring(length)

    def _receive_fixed_bytes(self, num_bytes, chunk_size=None):
        if not chunk_size:
            chunk_size = self.__get_receive_buffer_size()

        if num_bytes % 4:
            padding_length = 4 - (num_bytes % 4)
        else:
            padding_length = 0

        for _ in range(num_bytes // chunk_size):
            self.receive(chunk_size)

        remaining = num_bytes % chunk_size
        if remaining:
            self._receive_fixed_bytes(remaining, chunk_size=remaining)
        elif padding_length:
            self.receive(padding_length)

        return num_bytes

    def send_string(self, string):
        self.__p.reset()
        self.__p.pack_string(string)
        self.__s.sendall(self.__p.get_buffer())

    def receive_string(self):
        return self.receive_fixed_string(self.receive_u32())

    def _send_fixed_bytes(self, num_bytes, chunk_size=None, byte_value=255):
        if not chunk_size:
            chunk_size = self.__get_send_buffer_size()

        self.__p.reset()
        self.__p.pack_fstring(chunk_size, bytearray([byte_value] * chunk_size))

        buffer = self.__p.get_buffer()
        for _ in range(num_bytes // chunk_size):
            self.__s.sendall(buffer)

        remaining = num_bytes % chunk_size
        if remaining:
            self._send_fixed_bytes(remaining, chunk_size=remaining,
                                   byte_value=byte_value)

    def send_uuid(self, uuid):
        self.send_fixed_string(16, uuid.bytes)

    def receive_uuid(self):
        return UUID(bytes=self.receive_fixed_string(16))

    def send_bool(self, value):
        self.send_u32(value and 1 or 0)

    def receive_bool(self):
        return self.receive_u32() and True or False

    def send_array(self, array, send_func):
        self.send_u32(len(array))
        for element in array:
            send_func(element)

    @staticmethod
    def receive_array_n(n, receive_func):
        return [receive_func() for _ in range(n)]

    def receive_array(self, receive_func):
        return self.receive_array_n(self.receive_u32(), receive_func)

    def receive_timestamp(self):
        # UTC POSIX timestamp in nanoseconds.
        return datetime.fromtimestamp(self.receive_u64() / 1000000000, _utc)

    def __get_send_buffer_size(self):
        socket_type = self.__s.getsockopt(socket.SOL_SOCKET, socket.SO_TYPE)
        return self.__s.getsockopt(socket_type, socket.SO_SNDBUF)

    def __get_receive_buffer_size(self):
        socket_type = self.__s.getsockopt(socket.SOL_SOCKET, socket.SO_TYPE)
        return self.__s.getsockopt(socket_type, socket.SO_RCVBUF)
