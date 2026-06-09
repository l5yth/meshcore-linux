#pragma once

#ifdef ARDULINUX_PLATFORM

#include "BaseSerialInterface.h"
#include <stddef.h>
#include <stdint.h>

class LinuxTcpInterface : public BaseSerialInterface {
  int _server_fd;
  int _client_fd;
  bool _enabled;
  bool _device_connected;
  uint16_t _port;
  unsigned long _last_write;

  struct FrameHeader {
    uint8_t type;
    uint16_t length;
  };

  struct Frame {
    uint8_t len;
    uint8_t buf[MAX_FRAME_SIZE];
  };

  FrameHeader _received_frame_header;

  static const int FRAME_QUEUE_SIZE = 4;
  int _send_queue_len;
  Frame _send_queue[FRAME_QUEUE_SIZE];

  uint8_t _rx_buf[MAX_FRAME_SIZE + 16];
  size_t _rx_len;

  void closeClient();
  void acceptIfNeeded();
  void drainSendQueue();
  size_t readFrameFromBuffer(uint8_t dest[]);
  bool hasReceivedFrameHeader() const;
  void resetReceivedFrameHeader();

public:
  LinuxTcpInterface();
  ~LinuxTcpInterface();

  bool begin(uint16_t port, const char* bind_addr);

  void enable() override;
  void disable() override;
  bool isEnabled() const override { return _enabled; }
  bool isConnected() const override { return _device_connected; }
  bool isWriteBusy() const override { return false; }
  size_t writeFrame(const uint8_t src[], size_t len) override;
  size_t checkRecvFrame(uint8_t dest[]) override;
};

#endif // ARDULINUX_PLATFORM
