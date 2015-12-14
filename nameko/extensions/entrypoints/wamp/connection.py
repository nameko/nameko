"""
websocket - WebSocket client library for Python

Copyright (C) 2010 Hiroki Ohtani(liris)

    This library is free software; you can redistribute it and/or
    modify it under the terms of the GNU Lesser General Public
    License as published by the Free Software Foundation; either
    version 2.1 of the License, or (at your option) any later version.

    This library is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
    Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General Public
    License along with this library; if not, write to the Free Software
    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

"""
from __future__ import print_function


import six
import socket

try:
    import ssl
    from ssl import SSLError
    if hasattr(ssl, "match_hostname"):
        from ssl import match_hostname
    else:
        from backports.ssl_match_hostname import match_hostname

    HAVE_SSL = True
except ImportError:
    # dummy class of SSLError for ssl none-support environment.
    class SSLError(Exception):
        pass

    HAVE_SSL = False

from six.moves.urllib.parse import urlparse
if six.PY3:
    from base64 import encodebytes as base64encode
else:
    from base64 import encodestring as base64encode

import array
import os
import errno
import struct
import uuid
import hashlib
import threading
import logging

# websocket modules


"""
websocket python client.
=========================

This version support only hybi-13.
Please see http://tools.ietf.org/html/rfc6455 for protocol.
"""

# closing frame status codes.
STATUS_NORMAL = 1000
STATUS_GOING_AWAY = 1001
STATUS_PROTOCOL_ERROR = 1002
STATUS_UNSUPPORTED_DATA_TYPE = 1003
STATUS_STATUS_NOT_AVAILABLE = 1005
STATUS_ABNORMAL_CLOSED = 1006
STATUS_INVALID_PAYLOAD = 1007
STATUS_POLICY_VIOLATION = 1008
STATUS_MESSAGE_TOO_BIG = 1009
STATUS_INVALID_EXTENSION = 1010
STATUS_UNEXPECTED_CONDITION = 1011
STATUS_TLS_HANDSHAKE_ERROR = 1015

VALID_CLOSE_STATUS = (
    STATUS_NORMAL,
    STATUS_GOING_AWAY,
    STATUS_PROTOCOL_ERROR,
    STATUS_UNSUPPORTED_DATA_TYPE,
    STATUS_INVALID_PAYLOAD,
    STATUS_POLICY_VIOLATION,
    STATUS_MESSAGE_TOO_BIG,
    STATUS_INVALID_EXTENSION,
    STATUS_UNEXPECTED_CONDITION,
    )

class ABNF(object):
    """
    ABNF frame class.
    see http://tools.ietf.org/html/rfc5234
    and http://tools.ietf.org/html/rfc6455#section-5.2
    """

    # operation code values.
    OPCODE_CONT   = 0x0
    OPCODE_TEXT   = 0x1
    OPCODE_BINARY = 0x2
    OPCODE_CLOSE  = 0x8
    OPCODE_PING   = 0x9
    OPCODE_PONG   = 0xa

    # available operation code value tuple
    OPCODES = (OPCODE_CONT, OPCODE_TEXT, OPCODE_BINARY, OPCODE_CLOSE,
                OPCODE_PING, OPCODE_PONG)

    # opcode human readable string
    OPCODE_MAP = {
        OPCODE_CONT: "cont",
        OPCODE_TEXT: "text",
        OPCODE_BINARY: "binary",
        OPCODE_CLOSE: "close",
        OPCODE_PING: "ping",
        OPCODE_PONG: "pong"
        }

    # data length threashold.
    LENGTH_7  = 0x7e
    LENGTH_16 = 1 << 16
    LENGTH_63 = 1 << 63

    def __init__(self, fin=0, rsv1=0, rsv2=0, rsv3=0,
                 opcode=OPCODE_TEXT, mask=1, data=""):
        """
        Constructor for ABNF.
        please check RFC for arguments.
        """
        self.fin = fin
        self.rsv1 = rsv1
        self.rsv2 = rsv2
        self.rsv3 = rsv3
        self.opcode = opcode
        self.mask = mask
        self.data = data
        self.get_mask_key = os.urandom

    def validate(self):
        """
        validate the ABNF frame.
        """
        if self.rsv1 or self.rsv2 or self.rsv3:
            raise WebSocketProtocolException("rsv is not implemented, yet")

        if self.opcode not in ABNF.OPCODES:
            raise WebSocketProtocolException("Invalid opcode " + self.opcode)

        if self.opcode == ABNF.OPCODE_PING and not self.fin:
            raise WebSocketProtocolException("Invalid ping frame.")

        if self.opcode == ABNF.OPCODE_CLOSE:
            l = len(self.data)
            if not l:
                return
            if l == 1 or l >= 126:
                raise WebSocketProtocolException("Invalid close frame.")
            if l > 2 and not validate_utf8(self.data[2:]):
                raise WebSocketProtocolException("Invalid close frame.")

            code = 256*six.byte2int(self.data[0:1]) + six.byte2int(self.data[1:2])
            if not self._is_valid_close_status(code):
                raise WebSocketProtocolException("Invalid close opcode.")

    def _is_valid_close_status(self, code):
        return code in VALID_CLOSE_STATUS or (3000 <= code <5000)

    def __str__(self):
        return "fin=" + str(self.fin) \
                + " opcode=" + str(self.opcode) \
                + " data=" + str(self.data)

    @staticmethod
    def _create_frame(data, opcode, fin=1):
        """
        create frame to send text, binary and other data.

        data: data to send. This is string value(byte array).
            if opcode is OPCODE_TEXT and this value is uniocde,
            data value is conveted into unicode string, automatically.

        opcode: operation code. please see OPCODE_XXX.

        fin: fin flag. if set to 0, create continue fragmentation.
        """

        # should be more like....
        # https://github.com/crossbario/autobahn-python/blob/a771b54bc93f4d0cc273162dc737f2c528a92fdd/autobahn/websocket/protocol.py#L1749
        raise

        if opcode == ABNF.OPCODE_TEXT and isinstance(data, six.text_type):
            data = data.encode("utf-8")
        # mask must be set if send data from client 

        return ABNF(fin, 0, 0, 0, opcode, 1, data)

    @staticmethod
    def create_frame(data,
                  opcode,
                  payload=b'',
                  fin=True,
                  rsv=0,
                  mask=None,
                  payload_len=None,
                  chopsize=None,
                  sync=False):
        """
        Send out frame. Normally only used internally via sendMessage(),
        sendPing(), sendPong() and sendClose().
        This method deliberately allows to send invalid frames (that is frames
        invalid per-se, or frames invalid because of protocol state). Other
        than in fuzzing servers, calling methods will ensure that no invalid
        frames are sent.
        In addition, this method supports explicit specification of payload
        length. When payload_len is given, it will always write that many
        octets to the stream. It'll wrap within payload, resending parts of
        that when more octets were requested The use case is again for fuzzing
        server which want to sent increasing amounts of payload data to peers
        without having to construct potentially large messages themselves.
        """
        print('create frame')
        if payload_len is not None:
            if len(payload) < 1:
                raise Exception("cannot construct repeated payload with length %d from payload of length %d" % (payload_len, len(payload)))
            l = payload_len
            pl = b''.join([payload for _ in range(payload_len / len(payload))]) + payload[:payload_len % len(payload)]
        else:
            l = len(payload)
            pl = payload

        # first byte
        #
        b0 = 0
        if fin:
            b0 |= (1 << 7)
        b0 |= (rsv % 8) << 4
        b0 |= opcode % 128

        # second byte, payload len bytes and mask
        #
        b1 = 0
        mv = b''
        plm = pl

        el = b''
        if l <= 125:
            b1 |= l
        elif l <= 0xFFFF:
            b1 |= 126
            el = struct.pack("!H", l)
        elif l <= 0x7FFFFFFFFFFFFFFF:
            b1 |= 127
            el = struct.pack("!Q", l)
        else:
            raise Exception("invalid payload length")

        if six.PY3:
            raw = b''.join([b0.to_bytes(1, 'big'), b1.to_bytes(1, 'big'), el, mv, plm])
        else:
            raw = b''.join([chr(b0), chr(b1), el, mv, plm])

        #return raw
        print(raw)
        return ABNF(fin, 0, 0, 0, opcode, 1, raw)


    def format(self):
        """
        format this object to string(byte array) to send data to server.
        """
        if any(x not in (0, 1) for x in [self.fin, self.rsv1, self.rsv2, self.rsv3]):
            raise ValueError("not 0 or 1")
        if self.opcode not in ABNF.OPCODES:
            raise ValueError("Invalid OPCODE")
        length = len(self.data)
        if length >= ABNF.LENGTH_63:
            raise ValueError("data is too long")

        frame_header = chr(self.fin << 7
                           | self.rsv1 << 6 | self.rsv2 << 5 | self.rsv3 << 4
                           | self.opcode)
        if length < ABNF.LENGTH_7:
            frame_header += chr(self.mask << 7 | length)
            frame_header = six.b(frame_header)
        elif length < ABNF.LENGTH_16:
            frame_header += chr(self.mask << 7 | 0x7e)
            frame_header = six.b(frame_header)
            frame_header += struct.pack("!H", length)
        else:
            frame_header += chr(self.mask << 7 | 0x7f)
            frame_header = six.b(frame_header)
            frame_header += struct.pack("!Q", length)

        if not self.mask:
            return frame_header + self.data
        else:
            mask_key = self.get_mask_key(4)
            return frame_header + self._get_masked(mask_key)

    def _get_masked(self, mask_key):
        s = ABNF.mask(mask_key, self.data)

        if isinstance(mask_key, six.text_type):
            mask_key = mask_key.encode('utf-8')

        return mask_key + s

    @staticmethod
    def mask(mask_key, data):
        """
        mask or unmask data. Just do xor for each byte

        mask_key: 4 byte string(byte).

        data: data to mask/unmask.
        """

        if isinstance(mask_key, six.text_type):
            mask_key = six.b(mask_key)

        if isinstance(data, six.text_type):
            data = six.b(data)

        _m = array.array("B", mask_key)
        _d = array.array("B", data)
        for i in range(len(_d)):
            _d[i] ^= _m[i % 4]

        if six.PY3:
            return _d.tobytes()
        else:
            return _d.tostring()


class WebSocketException(Exception):
    """
    websocket exeception class.
    """
    pass

class WebSocketProtocolException(WebSocketException):
    """
    If the webscoket protocol is invalid, this exception will be raised.
    """
    pass

class WebSocketPayloadException(WebSocketException):
    """
    If the webscoket payload is invalid, this exception will be raised.
    """
    pass

class WebSocketConnectionClosedException(WebSocketException):
    """
    If remote host closed the connection or some network error happened,
    this exception will be raised.
    """
    pass

class WebSocketTimeoutException(WebSocketException):
    """
    WebSocketTimeoutException will be raised at socket timeout during read/write data.
    """
    pass




class NoLock(object):
    def __enter__(self):
        pass

    def __exit__(self,type, value, traceback):
        pass


# websocket supported version.
VERSION = 13


DEFAULT_SOCKET_OPTION = [(socket.SOL_TCP, socket.TCP_NODELAY, 1),]
if hasattr(socket, "SO_KEEPALIVE"):
    DEFAULT_SOCKET_OPTION.append((socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1))
if hasattr(socket, "TCP_KEEPIDLE"):
    DEFAULT_SOCKET_OPTION.append((socket.SOL_TCP, socket.TCP_KEEPIDLE, 30))
if hasattr(socket, "TCP_KEEPINTVL"):
    DEFAULT_SOCKET_OPTION.append((socket.SOL_TCP, socket.TCP_KEEPINTVL, 10))
if hasattr(socket, "TCP_KEEPCNT"):
    DEFAULT_SOCKET_OPTION.append((socket.SOL_TCP, socket.TCP_KEEPCNT, 3))

logger = logging.getLogger()



default_timeout = None
traceEnabled = False


def validate_utf8(utfbytes):
    """
    validate utf8 byte string.
    utfbytes: utf byte string to check.
    return value: if valid utf8 string, return true. Otherwise, return false.
    """
    state = UTF8_ACCEPT
    codep = 0
    for i in utfbytes:
        if six.PY2:
            i = ord(i)
        state, codep = _decode(state, codep, i)
        if state == UTF8_REJECT:
            return False

    return True


def enableTrace(tracable):
    """
    turn on/off the tracability.

    tracable: boolean value. if set True, tracability is enabled.
    """
    global traceEnabled
    traceEnabled = tracable
    if tracable:
        if not logger.handlers:
            logger.addHandler(logging.StreamHandler())
        logger.setLevel(logging.DEBUG)

def _dump(title, message):
    if traceEnabled:
        logger.debug("--- " + title + " ---")
        logger.debug(message)
        logger.debug("-----------------------")

def setdefaulttimeout(timeout):
    """
    Set the global timeout setting to connect.

    timeout: default socket timeout time. This value is second.
    """
    global default_timeout
    default_timeout = timeout


def getdefaulttimeout():
    """
    Return the global timeout setting(second) to connect.
    """
    return default_timeout


def _parse_url(url):
    """
    parse url and the result is tuple of
    (hostname, port, resource path and the flag of secure mode)

    url: url string.
    """
    if ":" not in url:
        raise ValueError("url is invalid")

    scheme, url = url.split(":", 1)

    parsed = urlparse(url, scheme="ws")
    if parsed.hostname:
        hostname = parsed.hostname
    else:
        raise ValueError("hostname is invalid")
    port = 0
    if parsed.port:
        port = parsed.port

    is_secure = False
    if scheme == "ws":
        if not port:
            port = 80
    elif scheme == "wss":
        is_secure = True
        if not port:
            port = 443
    else:
        raise ValueError("scheme %s is invalid" % scheme)

    if parsed.path:
        resource = parsed.path
    else:
        resource = "/"

    if parsed.query:
        resource += "?" + parsed.query

    return (hostname, port, resource, is_secure)


DEFAULT_NO_PROXY_HOST = ["localhost", "127.0.0.1"]

def _is_no_proxy_host(hostname, no_proxy):
    if not no_proxy:
        v = os.environ.get("no_proxy", "").replace(" ", "")
        no_proxy = v.split(",")
    if not no_proxy:
        no_proxy = DEFAULT_NO_PROXY_HOST

    return hostname in no_proxy

def _get_proxy_info(hostname, is_secure, **options):
    """
    try to retrieve proxy host and port from environment if not provided in options.
    result is (proxy_host, proxy_port, proxy_auth).
    proxy_auth is tuple of username and password of proxy authentication information.
    
    hostname: websocket server name.

    is_secure:  is the connection secure? (wss)
                looks for "https_proxy" in env before falling back to "http_proxy"

    options:    "http_proxy_host" - http proxy host name.
                "http_proxy_port" - http proxy port.
                "http_no_proxy"   - host names, which doesn't use proxy.
                "http_proxy_auth" - http proxy auth infomation. tuple of username and password.
                                    defualt is None
    """
    if _is_no_proxy_host(hostname, options.get("http_no_proxy", None)):
        return None, 0, None

    http_proxy_host = options.get("http_proxy_host", None)
    if http_proxy_host:
        return http_proxy_host, options.get("http_proxy_port", 0), options.get("http_proxy_auth", None)

    env_keys = ["http_proxy"]
    if is_secure:
        env_keys.insert(0, "https_proxy")

    for key in env_keys:
        value = os.environ.get(key, None)
        if value:
            proxy = urlparse(value)
            auth = (proxy.username, proxy.password) if proxy.username else None
            return proxy.hostname, proxy.port, auth

    return None, 0, None

def _extract_err_message(exception):
        message = getattr(exception, 'strerror', '')
        if not message:
            message = getattr(exception, 'message', '')

        return message


def create_connection(url, timeout=None, **options):
    """
    connect to url and return websocket object.

    Connect to url and return the WebSocket object.
    Passing optional timeout parameter will set the timeout on the socket.
    If no timeout is supplied, the global default timeout setting returned by getdefauttimeout() is used.
    You can customize using 'options'.
    If you set "header" list object, you can set your own custom header.

    >>> conn = create_connection("ws://echo.websocket.org/",
         ...     header=["User-Agent: MyProgram",
         ...             "x-custom: header"])


    timeout: socket timeout time. This value is integer.
             if you set None for this value, it means "use default_timeout value"


    options: "header" -> custom http header list.
             "cookie" -> cookie value.
             "http_proxy_host" - http proxy host name.
             "http_proxy_port" - http proxy port. If not set, set to 80.
             "http_no_proxy"   - host names, which doesn't use proxy.
             "http_proxy_auth" - http proxy auth infomation. tuple of username and password.
                                    defualt is None
             "enable_multithread" -> enable lock for multithread.
             "sockopt" -> socket options
             "sslopt" -> ssl option
             "subprotocols" - array of available sub protocols. default is None.
    """
    sockopt = options.get("sockopt", [])
    sslopt = options.get("sslopt", {})
    fire_cont_frame = options.get("fire_cont_frame", False)
    enable_multithread = options.get("enable_multithread", False)
    websock = WebSocket(sockopt=sockopt, sslopt=sslopt,
        fire_cont_frame = fire_cont_frame, enable_multithread=enable_multithread)
    websock.settimeout(timeout if timeout is not None else default_timeout)
    websock.connect(url, **options)
    return websock

_MAX_INTEGER = (1 << 32) -1
_AVAILABLE_KEY_CHARS = list(range(0x21, 0x2f + 1)) + list(range(0x3a, 0x7e + 1))
_MAX_CHAR_BYTE = (1<<8) -1

# ref. Websocket gets an update, and it breaks stuff.
# http://axod.blogspot.com/2010/06/websocket-gets-update-and-it-breaks.html


def _create_sec_websocket_key():
    uid = uuid.uuid4()
    return base64encode(uid.bytes).decode('utf-8').strip()


_HEADERS_TO_CHECK = {
    "upgrade": "websocket",
    "connection": "upgrade",
    }


class _FrameBuffer(object):
    _HEADER_MASK_INDEX = 5
    _HEADER_LENGHT_INDEX = 6

    def __init__(self):
        self.clear()

    def clear(self):
        self.header = None
        self.length = None
        self.mask = None

    def has_received_header(self):
        return  self.header is None

    def recv_header(self, recv_fn):
        header = recv_fn(2)
        b1 = header[0]

        if six.PY2:
            b1 = ord(b1)

        fin = b1 >> 7 & 1
        rsv1 = b1 >> 6 & 1
        rsv2 = b1 >> 5 & 1
        rsv3 = b1 >> 4 & 1
        opcode = b1 & 0xf
        b2 = header[1]

        if six.PY2:
            b2 = ord(b2)

        has_mask = b2 >> 7 & 1
        length_bits = b2 & 0x7f

        self.header = (fin, rsv1, rsv2, rsv3, opcode, has_mask, length_bits)

    def has_mask(self):
        if not self.header:
            return False
        return self.header[_FrameBuffer._HEADER_MASK_INDEX]


    def has_received_length(self):
        return self.length is None

    def recv_length(self, recv_fn):
        bits = self.header[_FrameBuffer._HEADER_LENGHT_INDEX]
        length_bits = bits & 0x7f
        if length_bits == 0x7e:
            v = recv_fn(2)
            self.length = struct.unpack("!H", v)[0]
        elif length_bits == 0x7f:
            v = recv_fn(8)
            self.length = struct.unpack("!Q", v)[0]
        else:
            self.length = length_bits

    def has_received_mask(self):
        return self.mask is None

    def recv_mask(self, recv_fn):
        self.mask = recv_fn(4) if self.has_mask() else ""


class WampWebSocket(object):
    """
    Low level WebSocket interface.
    This class is based on
      The WebSocket protocol draft-hixie-thewebsocketprotocol-76
      http://tools.ietf.org/html/draft-hixie-thewebsocketprotocol-76

    We can connect to the websocket server and send/recieve data.
    The following example is a echo client.

    >>> import websocket
    >>> ws = websocket.WebSocket()
    >>> ws.connect("ws://echo.websocket.org")
    >>> ws.send("Hello, Server")
    >>> ws.recv()
    'Hello, Server'
    >>> ws.close()

    get_mask_key: a callable to produce new mask keys, see the set_mask_key
      function's docstring for more details
    sockopt: values for socket.setsockopt.
        sockopt must be tuple and each element is argument of sock.setscokopt.
    sslopt: dict object for ssl socket option.
    fire_cont_frame: fire recv event for each cont frame. default is False
    enable_multithread: if set to True, lock send method.
    """

    def __init__(self, get_mask_key=None, sockopt=None, sslopt=None,
        fire_cont_frame=False, enable_multithread=False):
        """
        Initalize WebSocket object.
        """
        if sockopt is None:
            sockopt = []
        if sslopt is None:
            sslopt = {}
        self.connected = False
        self.sock = None
        self._timeout = None
        self.sockopt = sockopt
        self.sslopt = sslopt
        self.get_mask_key = get_mask_key
        self.fire_cont_frame = fire_cont_frame
        # Buffers over the packets from the layer beneath until desired amount
        # bytes of bytes are received.
        self._recv_buffer = []
        # These buffer over the build-up of a single frame.
        self._frame_buffer = _FrameBuffer()
        self._cont_data = None
        self._recving_frames = None
        if enable_multithread:
            self.lock = threading.Lock()
        else:
            self.lock = NoLock()

        self.subprotocol = None

    def fileno(self):
        return self.sock.fileno()

    def set_mask_key(self, func):
        """
        set function to create musk key. You can custumize mask key generator.
        Mainly, this is for testing purpose.

        func: callable object. the fuct must 1 argument as integer.
              The argument means length of mask key.
              This func must be return string(byte array),
              which length is argument specified.
        """
        self.get_mask_key = func

    def gettimeout(self):
        """
        Get the websocket timeout(second).
        """
        return self._timeout

    def settimeout(self, timeout):
        """
        Set the timeout to the websocket.

        timeout: timeout time(second).
        """
        self._timeout = timeout
        if self.sock:
            self.sock.settimeout(timeout)

    timeout = property(gettimeout, settimeout)

    def connect(self, url, **options):
        """
        Connect to url. url is websocket url scheme. ie. ws://host:port/resource
        You can customize using 'options'.
        If you set "header" list object, you can set your own custom header.

        >>> ws = WebSocket()
        >>> ws.connect("ws://echo.websocket.org/",
                ...     header=["User-Agent: MyProgram",
                ...             "x-custom: header"])

        timeout: socket timeout time. This value is integer.
                 if you set None for this value,
                 it means "use default_timeout value"

        options: "header" -> custom http header list.
                 "cookie" -> cookie value.
                 "http_proxy_host" - http proxy host name.
                 "http_proxy_port" - http proxy port. If not set, set to 80.
                 "http_no_proxy"   - host names, which doesn't use proxy.
                 "http_proxy_auth" - http proxy auth infomation. tuple of username and password.
                                    defualt is None
                 "subprotocols" - array of available sub protocols. default is None.

        """

        hostname, port, resource, is_secure = _parse_url(url)
        proxy_host, proxy_port, proxy_auth = _get_proxy_info(hostname, is_secure, **options)
        if not proxy_host:
            addrinfo_list = socket.getaddrinfo(hostname, port, 0, 0, socket.SOL_TCP)
        else:
            proxy_port = proxy_port and proxy_port or 80
            addrinfo_list = socket.getaddrinfo(proxy_host, proxy_port, 0, 0, socket.SOL_TCP)

        if not addrinfo_list:
            raise WebSocketException("Host not found.: " + hostname + ":" + str(port))

        err = None
        for addrinfo in addrinfo_list:
            family = addrinfo[0]
            self.sock = socket.socket(family)
            self.sock.settimeout(self.timeout)
            for opts in DEFAULT_SOCKET_OPTION:
                self.sock.setsockopt(*opts)
            for opts in self.sockopt:
                self.sock.setsockopt(*opts)
            
            address = addrinfo[4]
            try:
                self.sock.connect(address)
            except socket.error as error:
                error.remote_ip = str(address[0])
                if error.errno in (errno.ECONNREFUSED, ):
                    err = error
                    continue
                else:
                    raise
            else:
                break
        else:
            raise err

        if proxy_host:
            self._tunnel(hostname, port, proxy_auth)

        if is_secure:
            if HAVE_SSL:
                sslopt = dict(cert_reqs=ssl.CERT_REQUIRED)
                certPath = os.path.join(
                    os.path.dirname(__file__), "cacert.pem")
                if os.path.isfile(certPath):
                    sslopt['ca_certs'] = certPath
                sslopt.update(self.sslopt)
                check_hostname = sslopt.pop('check_hostname', True)
                self.sock = ssl.wrap_socket(self.sock, **sslopt)
                if (sslopt["cert_reqs"] != ssl.CERT_NONE
                        and check_hostname):
                    match_hostname(self.sock.getpeercert(), hostname)
            else:
                raise WebSocketException("SSL not available.")

        self._handshake(hostname, port, resource, **options)

    def _tunnel(self, host, port, auth):
        logger.debug("Connecting proxy...")
        connect_header = "CONNECT %s:%d HTTP/1.0\r\n" % (host, port)
        # TODO: support digest auth.
        if auth and auth[0]:
            auth_str = auth[0]
            if auth[1]:
                auth_str += ":" + auth[1]
            encoded_str = base64encode(auth_str.encode()).strip().decode()
            connect_header += "Proxy-Authorization: Basic %s\r\n" % encoded_str
        connect_header += "\r\n"
        _dump("request header", connect_header)

        self._send(connect_header)

        status, resp_headers = self._read_headers()
        if status != 200:
            raise WebSocketException("failed CONNECT via proxy")

    def _get_resp_headers(self, success_status = 101):
        status, resp_headers = self._read_headers()
        if status != success_status:
            self.close()
            raise WebSocketException("Handshake status %d" % status)
        return resp_headers

    def _get_handshake_headers(self, resource, host, port, options):
        headers = []
        headers.append("GET %s HTTP/1.1" % resource)
        headers.append("Upgrade: websocket")
        headers.append("Connection: Upgrade")
        if port == 80:
            hostport = host
        else:
            hostport = "%s:%d" % (host, port)
        headers.append("Host: %s" % hostport)

        if "origin" in options:
            headers.append("Origin: %s" % options["origin"])
        else:
            headers.append("Origin: http://%s" % hostport)

        key = _create_sec_websocket_key()
        headers.append("Sec-WebSocket-Key: %s" % key)
        headers.append("Sec-WebSocket-Version: %s" % VERSION)

        subprotocols = options.get("subprotocols")
        if subprotocols:
            headers.append("Sec-WebSocket-Protocol: %s" % ",".join(subprotocols))

        if "header" in options:
            headers.extend(options["header"])

        cookie = options.get("cookie", None)

        if cookie:
          headers.append("Cookie: %s" % cookie)

        # WAMP protocol
        headers.append("Sec-WebSocket-Protocol: wamp.2.json.batched,wamp.2.json")

        headers.append("")
        headers.append("")

        return headers, key

    def _handshake(self, host, port, resource, **options):
        headers, key = self._get_handshake_headers(resource, host, port, options)

        header_str = "\r\n".join(headers)
        self._send(header_str)
        _dump("request header", header_str)

        resp_headers = self._get_resp_headers()
        success = self._validate_header(resp_headers, key, options.get("subprotocols"))
        if not success:
            self.close()
            raise WebSocketException("Invalid WebSocket Header")

        self.connected = True

    def _validate_header(self, headers, key, subprotocols):
        for k, v in _HEADERS_TO_CHECK.items():
            r = headers.get(k, None)
            if not r:
                return False
            r = r.lower()
            if v != r:
                return False
        
        if subprotocols:
            subproto = headers.get("sec-websocket-protocol", None)
            if not subproto or subproto not in subprotocols:
                logger.error("Invalid subprotocol: " + str(subprotocols))
                return False
            self.subprotocol = subproto


        result = headers.get("sec-websocket-accept", None)
        if not result:
            return False
        result = result.lower()

        if isinstance(result, six.text_type):
            result = result.encode('utf-8')

        value = (key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode('utf-8')
        hashed = base64encode(hashlib.sha1(value).digest()).strip().lower()
        return hashed == result

    def _read_headers(self):
        status = None
        headers = {}
        if traceEnabled:
            logger.debug("--- response header ---")

        while True:
            line = self._recv_line()
            line = line.decode('utf-8')
            if line == "\r\n" or line == "\n":
                break
            line = line.strip()
            if traceEnabled:
                logger.debug(line)
            if not status:
                status_info = line.split(" ", 2)
                status = int(status_info[1])
            else:
                kv = line.split(":", 1)
                if len(kv) == 2:
                    key, value = kv
                    headers[key.lower()] = value.strip().lower()
                else:
                    raise WebSocketException("Invalid header")

        if traceEnabled:
            logger.debug("-----------------------")

        return status, headers

    def send(self, payload, opcode=ABNF.OPCODE_TEXT):
        """
        Send the data as string.

        payload: Payload must be utf-8 string or unicode,
                  if the opcode is OPCODE_TEXT.
                  Otherwise, it must be string(byte array)

        opcode: operation code to send. Please see OPCODE_XXX.
        """
        print('send message to broker')
        frame = ABNF.create_frame(payload, opcode)  
        return self.send_frame(frame)

    def send_frame(self, frame):
        """
        Send the data frame.

        frame: frame data created  by ABNF.create_frame

        >>> ws = create_connection("ws://echo.websocket.org/")
        >>> frame = ABNF.create_frame("Hello", ABNF.OPCODE_TEXT)
        >>> ws.send_frame(frame)
        >>> cont_frame = ABNF.create_frame("My name is ", ABNF.OPCODE_CONT, 0)
        >>> ws.send_frame(frame)
        >>> cont_frame = ABNF.create_frame("Foo Bar", ABNF.OPCODE_CONT, 1)
        >>> ws.send_frame(frame)

        """
        if self.get_mask_key:
            frame.get_mask_key = self.get_mask_key

        data = frame.format()
        length = len(data)
        if traceEnabled:
            logger.debug("send: " + repr(data))

        with self.lock:
            while data:
                l = self._send(data)
                data = data[l:]

        return length

    def send_binary(self, payload):
        return self.send(payload, ABNF.OPCODE_BINARY)

    def ping(self, payload=""):
        """
        send ping data.

        payload: data payload to send server.
        """
        if isinstance(payload, six.text_type):
            payload = payload.encode("utf-8")
        self.send(payload, ABNF.OPCODE_PING)

    def pong(self, payload):
        """
        send pong data.

        payload: data payload to send server.
        """
        if isinstance(payload, six.text_type):
            payload = payload.encode("utf-8")
        self.send(payload, ABNF.OPCODE_PONG)

    def recv(self):
        """
        Receive string data(byte array) from the server.

        return value: string(byte array) value.
        """
        opcode, data = self.recv_data()
        if six.PY3 and opcode == ABNF.OPCODE_TEXT:
            return data.decode("utf-8")
        elif opcode == ABNF.OPCODE_TEXT or opcode == ABNF.OPCODE_BINARY:
            return data
        else:
            return ''

    def recv_data(self, control_frame=False):
        """
        Recieve data with operation code.

        control_frame: a boolean flag indicating whether to return control frame
        data, defaults to False

        return  value: tuple of operation code and string(byte array) value.
        """
        opcode, frame = self.recv_data_frame(control_frame)
        return opcode, frame.data

    def recv_data_frame(self, control_frame=False):
        """
        Recieve data with operation code.

        control_frame: a boolean flag indicating whether to return control frame
        data, defaults to False

        return  value: tuple of operation code and string(byte array) value.
        """
        while True:
            frame = self.recv_frame()
            if not frame:
                # handle error:
                # 'NoneType' object has no attribute 'opcode'
                raise WebSocketProtocolException("Not a valid frame %s" % frame)
            elif frame.opcode in (ABNF.OPCODE_TEXT, ABNF.OPCODE_BINARY, ABNF.OPCODE_CONT):
                if not self._recving_frames and frame.opcode == ABNF.OPCODE_CONT:
                    raise WebSocketProtocolException("Illegal frame")
                if self._recving_frames and frame.opcode in (ABNF.OPCODE_TEXT, ABNF.OPCODE_BINARY):
                    raise WebSocketProtocolException("Illegal frame")

                if self._cont_data:
                    self._cont_data[1] += frame.data
                else:
                    if frame.opcode in (ABNF.OPCODE_TEXT, ABNF.OPCODE_BINARY):
                        self._recving_frames = frame.opcode
                    self._cont_data = [frame.opcode, frame.data]

                if frame.fin:
                    self._recving_frames = None
                
                if frame.fin or self.fire_cont_frame:
                    data = self._cont_data
                    self._cont_data = None
                    frame.data = data[1]
                    if not self.fire_cont_frame and data[0] == ABNF.OPCODE_TEXT and not validate_utf8(frame.data):
                        raise WebSocketPayloadException("cannot decode: " + repr(frame.data))
                    return [data[0], frame]

            elif frame.opcode == ABNF.OPCODE_CLOSE:
                self.send_close()
                return (frame.opcode, frame)
            elif frame.opcode == ABNF.OPCODE_PING:
                if len(frame.data) < 126:
                    self.pong(frame.data)
                else:
                    raise WebSocketProtocolException("Ping message is too long")
                if control_frame:
                    return (frame.opcode, frame)
            elif frame.opcode == ABNF.OPCODE_PONG:
                if control_frame:
                    return (frame.opcode, frame)

    def recv_frame(self):
        """
        recieve data as frame from server.

        return value: ABNF frame object.
        """
        frame_buffer = self._frame_buffer
        # Header
        if frame_buffer.has_received_header():
            frame_buffer.recv_header(self._recv_strict)
        (fin, rsv1, rsv2, rsv3, opcode, has_mask, _) = frame_buffer.header

        # Frame length
        if frame_buffer.has_received_length():
            frame_buffer.recv_length(self._recv_strict)
        length = frame_buffer.length

        # Mask
        if frame_buffer.has_received_mask():
            frame_buffer.recv_mask(self._recv_strict)
        mask = frame_buffer.mask

        # Payload
        payload = self._recv_strict(length)
        if has_mask:
            payload = ABNF.mask(mask, payload)

        # Reset for next frame
        frame_buffer.clear()

        frame = ABNF(fin, rsv1, rsv2, rsv3, opcode, has_mask, payload)
        frame.validate()

        return frame


    def send_close(self, status=STATUS_NORMAL, reason=six.b("")):
        """
        send close data to the server.

        status: status code to send. see STATUS_XXX.

        reason: the reason to close. This must be string or bytes.
        """
        if status < 0 or status >= ABNF.LENGTH_16:
            raise ValueError("code is invalid range")
        self.connected = False
        self.send(struct.pack('!H', status) + reason, ABNF.OPCODE_CLOSE)

    def close(self, status=STATUS_NORMAL, reason=six.b("")):
        """
        Close Websocket object

        status: status code to send. see STATUS_XXX.

        reason: the reason to close. This must be string.
        """
        if self.connected:
            if status < 0 or status >= ABNF.LENGTH_16:
                raise ValueError("code is invalid range")

            try:
                self.connected = False
                self.send(struct.pack('!H', status) + reason, ABNF.OPCODE_CLOSE)
                timeout = self.sock.gettimeout()
                self.sock.settimeout(3)
                try:
                    frame = self.recv_frame()
                    if logger.isEnabledFor(logging.ERROR):
                        recv_status = struct.unpack("!H", frame.data)[0]
                        if recv_status != STATUS_NORMAL:
                            logger.error("close status: " + repr(recv_status))
                except:
                    pass
                self.sock.settimeout(timeout)
                self.sock.shutdown(socket.SHUT_RDWR)
            except:
                pass

        self.shutdown()

    def abort(self):
        """
        Low-level asynchonous abort, wakes up other threads that are waiting in recv_*
        """
        if self.connected:
            self.sock.shutdown(socket.SHUT_RDWR)

    def shutdown(self):
        "close socket, immediately."
        if self.sock:
            self.sock.close()
            self.sock = None
            self.connected = False

    def _send(self, data):
        if isinstance(data, six.text_type):
            data = data.encode('utf-8')

        if not self.sock:
            raise WebSocketConnectionClosedException("socket is already closed.")

        try:
            return self.sock.send(data)
        except socket.timeout as e:
            message = _extract_err_message(e)
            raise WebSocketTimeoutException(message)
        except Exception as e:
            message = _extract_err_message(e)
            if message and "timed out" in message:
                raise WebSocketTimeoutException(message)
            else:
                raise

    def _recv(self, bufsize):
        if not self.sock:
            raise WebSocketConnectionClosedException("socket is already closed.")

        try:
            bytes = self.sock.recv(bufsize)
        except socket.timeout as e:
            message = _extract_err_message(e)
            raise WebSocketTimeoutException(message)
        except SSLError as e:
            message = _extract_err_message(e)
            if message == "The read operation timed out":
                raise WebSocketTimeoutException(message)
            else:
                raise

        if not bytes:
            self.sock.close()
            self.sock = None
            self.connected = False
            raise WebSocketConnectionClosedException()
        return bytes

    def _recv_strict(self, bufsize):
        shortage = bufsize - sum(len(x) for x in self._recv_buffer)
        while shortage > 0:
            bytes = self._recv(shortage)
            self._recv_buffer.append(bytes)
            shortage -= len(bytes)

        unified = six.b("").join(self._recv_buffer)

        if shortage == 0:
            self._recv_buffer = []
            return unified
        else:
            self._recv_buffer = [unified[bufsize:]]
            return unified[:bufsize]


    def _recv_line(self):
        line = []
        while True:
            c = self._recv(1)
            line.append(c)
            if c == six.b("\n"):
                break
        return six.b("").join(line)
