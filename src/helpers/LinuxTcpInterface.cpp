#ifdef ARDULINUX_PLATFORM

#include "LinuxTcpInterface.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <Arduino.h>  // millis()

LinuxTcpInterface::LinuxTcpInterface()
    : _server_fd(-1), _client_fd(-1), _enabled(false),
      _device_connected(false), _port(0), _last_write(0),
      _send_queue_len(0), _rx_len(0) {
  _received_frame_header.type = 0;
  _received_frame_header.length = 0;
}

LinuxTcpInterface::~LinuxTcpInterface() {
  closeClient();
  if (_server_fd >= 0) ::close(_server_fd);
}

bool LinuxTcpInterface::begin(uint16_t port, const char* bind_addr) {
  if (port == 0) return false;

  _server_fd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
  if (_server_fd < 0) {
    fprintf(stderr, "LinuxTcpInterface: socket() failed: %s\n", strerror(errno));
    return false;
  }

  int one = 1;
  (void)::setsockopt(_server_fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

  struct sockaddr_in addr;
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  if (::inet_pton(AF_INET, bind_addr, &addr.sin_addr) != 1) {
    fprintf(stderr, "LinuxTcpInterface: inet_pton(%s) failed (not a valid IPv4 address)\n", bind_addr);
    ::close(_server_fd);
    _server_fd = -1;
    return false;
  }

  if (::bind(_server_fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
    fprintf(stderr, "LinuxTcpInterface: bind(%s:%u) failed: %s\n", bind_addr, port, strerror(errno));
    ::close(_server_fd);
    _server_fd = -1;
    return false;
  }

  if (::listen(_server_fd, 1) < 0) {
    fprintf(stderr, "LinuxTcpInterface: listen() failed: %s\n", strerror(errno));
    ::close(_server_fd);
    _server_fd = -1;
    return false;
  }

  _port = port;
  return true;
}

void LinuxTcpInterface::enable() {
  if (_enabled) return;
  _enabled = true;
  _send_queue_len = 0;
  _rx_len = 0;
  resetReceivedFrameHeader();
}

void LinuxTcpInterface::disable() {
  _enabled = false;
  closeClient();
}

void LinuxTcpInterface::closeClient() {
  if (_client_fd >= 0) {
    ::close(_client_fd);
    _client_fd = -1;
  }
  _device_connected = false;
  _rx_len = 0;
  resetReceivedFrameHeader();
}

void LinuxTcpInterface::acceptIfNeeded() {
  if (_server_fd < 0) return;

  int new_fd = ::accept4(_server_fd, NULL, NULL, SOCK_NONBLOCK | SOCK_CLOEXEC);
  if (new_fd < 0) return;  // EAGAIN/EWOULDBLOCK = no incoming connection

  // Single-client semantics: a new connection kicks the previous one,
  // mirroring SerialWifiInterface::checkRecvFrame()'s replace-on-new behavior.
  closeClient();
  _client_fd = new_fd;
}

bool LinuxTcpInterface::hasReceivedFrameHeader() const {
  return _received_frame_header.type != 0 && _received_frame_header.length != 0;
}

void LinuxTcpInterface::resetReceivedFrameHeader() {
  _received_frame_header.type = 0;
  _received_frame_header.length = 0;
}

size_t LinuxTcpInterface::writeFrame(const uint8_t src[], size_t len) {
  if (len == 0 || len > MAX_FRAME_SIZE) return 0;
  if (!_device_connected) return 0;
  if (_send_queue_len >= FRAME_QUEUE_SIZE) return 0;

  _send_queue[_send_queue_len].len = (uint8_t)len;
  memcpy(_send_queue[_send_queue_len].buf, src, len);
  _send_queue_len++;
  return len;
}

void LinuxTcpInterface::drainSendQueue() {
  while (_send_queue_len > 0 && _device_connected) {
    int len = _send_queue[0].len;
    uint8_t pkt[3 + MAX_FRAME_SIZE];
    pkt[0] = '>';  // same framing as serial / SerialWifiInterface so meshcli can delimit
    pkt[1] = (uint8_t)(len & 0xFF);
    pkt[2] = (uint8_t)((len >> 8) & 0xFF);
    memcpy(&pkt[3], _send_queue[0].buf, len);

    ssize_t n = ::send(_client_fd, pkt, 3 + len, MSG_NOSIGNAL | MSG_DONTWAIT);
    if (n < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) return;  // try next tick
      closeClient();
      return;
    }
    // Partial writes are rare on local TCP with small frames; on the unlikely
    // partial we drop the frame to keep the protocol aligned rather than try
    // to track partial-write state across ticks.
    _last_write = millis();
    _send_queue_len--;
    for (int i = 0; i < _send_queue_len; i++) {
      _send_queue[i] = _send_queue[i + 1];
    }
  }
}

size_t LinuxTcpInterface::readFrameFromBuffer(uint8_t dest[]) {
  // Parse header if we don't have one yet
  if (!hasReceivedFrameHeader()) {
    const size_t HDR_LEN = 3;
    if (_rx_len < HDR_LEN) return 0;

    _received_frame_header.type = _rx_buf[0];
    _received_frame_header.length = (uint16_t)_rx_buf[1] | ((uint16_t)_rx_buf[2] << 8);

    memmove(_rx_buf, _rx_buf + HDR_LEN, _rx_len - HDR_LEN);
    _rx_len -= HDR_LEN;
  }

  uint16_t frame_length = _received_frame_header.length;
  uint8_t frame_type = _received_frame_header.type;

  // '<' (0x3c) is the only valid app→radio frame type
  if (frame_type != '<') {
    fprintf(stderr, "LinuxTcpInterface: bad frame type 0x%02x, resyncing\n", frame_type);
    _rx_len = 0;
    resetReceivedFrameHeader();
    return 0;
  }

  if (frame_length > MAX_FRAME_SIZE) {
    fprintf(stderr, "LinuxTcpInterface: frame length %u > MAX_FRAME_SIZE %d, resyncing\n",
            frame_length, MAX_FRAME_SIZE);
    _rx_len = 0;
    resetReceivedFrameHeader();
    return 0;
  }

  if (_rx_len < frame_length) return 0;  // need more bytes

  memcpy(dest, _rx_buf, frame_length);
  memmove(_rx_buf, _rx_buf + frame_length, _rx_len - frame_length);
  _rx_len -= frame_length;
  resetReceivedFrameHeader();
  return frame_length;
}

size_t LinuxTcpInterface::checkRecvFrame(uint8_t dest[]) {
  if (!_enabled) return 0;

  acceptIfNeeded();  // kicks the previous client if a new one connects

  if (_client_fd < 0) {
    if (_device_connected) _device_connected = false;
    return 0;
  }

  if (!_device_connected) {
    _device_connected = true;
    fprintf(stderr, "Companion TCP: client connected\n");
  }

  drainSendQueue();
  if (!_device_connected) return 0;  // closed during drain

  // Top up rx buffer from socket
  if (_rx_len < sizeof(_rx_buf)) {
    ssize_t n = ::recv(_client_fd, _rx_buf + _rx_len, sizeof(_rx_buf) - _rx_len, MSG_DONTWAIT);
    if (n == 0) {
      fprintf(stderr, "Companion TCP: client disconnected\n");
      closeClient();
      return 0;
    }
    if (n < 0) {
      if (errno != EAGAIN && errno != EWOULDBLOCK) {
        fprintf(stderr, "Companion TCP: recv error: %s\n", strerror(errno));
        closeClient();
        return 0;
      }
    } else {
      _rx_len += (size_t)n;
    }
  }

  return readFrameFromBuffer(dest);
}

#endif // ARDULINUX_PLATFORM
