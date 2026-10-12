package io.webrtc.signaling.gateway;

import io.netty.buffer.ByteBuf;
import io.webrtc.signaling.protocol.SignalEnvelope;

/** Allocation-bounded classification only; the strict decoder remains the authority. */
final class GatewayInputClass {
  private GatewayInputClass() {}

  static boolean critical(SignalEnvelope.Type type) {
    return switch (type) {
      case AUTH_REFRESH, CANCEL, HANGUP, REJECT, DECLINE_ALL, MEDIA_FAILED, EVENT_RECEIVED -> true;
      default -> false;
    };
  }

  static boolean critical(ByteBuf bytes) {
    if (bytes.readableBytes() < 1 || bytes.readableBytes() > 81920) return false;
    try {
      var scan = new Scan(bytes);
      scan.space();
      if (scan.take() != '{') return false;
      boolean found = false, critical = false;
      int fields = 0;
      scan.space();
      while (scan.peek() != '}') {
        if (++fields > 7) return false;
        String key = scan.string();
        scan.space();
        if (scan.take() != ':') return false;
        scan.space();
        if ("type".equals(key)) {
          if (found || scan.peek() != '"') return false;
          found = true;
          String type = scan.string();
          try {
            critical = type != null && critical(SignalEnvelope.Type.valueOf(type));
          } catch (IllegalArgumentException invalid) {
            return false;
          }
        } else scan.value();
        scan.space();
        if (scan.peek() == '}') break;
        if (scan.take() != ',') return false;
        scan.space();
      }
      scan.take();
      scan.space();
      return found && critical && scan.at == scan.end;
    } catch (IllegalArgumentException invalid) {
      return false;
    }
  }

  private static final class Scan {
    private final ByteBuf bytes;
    private final int end;
    private int at;

    Scan(ByteBuf bytes) {
      this.bytes = bytes;
      at = bytes.readerIndex();
      end = bytes.writerIndex();
    }

    int peek() {
      if (at >= end) throw new IllegalArgumentException("Incomplete envelope");
      return bytes.getUnsignedByte(at);
    }

    int take() {
      int value = peek();
      at++;
      return value;
    }

    void space() {
      while (at < end) {
        int value = bytes.getUnsignedByte(at);
        if (value != ' ' && value != '\n' && value != '\r' && value != '\t') return;
        at++;
      }
    }

    String string() {
      if (take() != '"') throw new IllegalArgumentException("Expected string");
      char[] bounded = new char[64];
      int length = 0;
      while (true) {
        int value = take();
        if (value == '"') return length <= 64 ? new String(bounded, 0, length) : null;
        if (value < 32) throw new IllegalArgumentException("Invalid string");
        if (value == '\\') {
          value = take();
          value =
              switch (value) {
                case '"', '\\', '/' -> value;
                case 'b' -> '\b';
                case 'f' -> '\f';
                case 'n' -> '\n';
                case 'r' -> '\r';
                case 't' -> '\t';
                case 'u' -> unicode();
                default -> throw new IllegalArgumentException("Invalid escape");
              };
        }
        if (length < 64) bounded[length] = (char) value;
        length++;
      }
    }

    int unicode() {
      int result = 0;
      for (int i = 0; i < 4; i++) {
        int digit = Character.digit((char) take(), 16);
        if (digit < 0) throw new IllegalArgumentException("Invalid escape");
        result = (result << 4) | digit;
      }
      return result;
    }

    void value() {
      int start = at, depth = 0;
      while (at < end) {
        int value = peek();
        if (value == '"') {
          // No strings proportional to JWT/SDP/ICE payloads are allocated here.
          skipString();
          continue;
        }
        if (value == '{' || value == '[') {
          if (++depth > 8) throw new IllegalArgumentException("Nested envelope");
        } else if (value == '}' || value == ']') {
          if (depth == 0) break;
          depth--;
        } else if (value == ',' && depth == 0) break;
        at++;
      }
      if (at == start || depth != 0) throw new IllegalArgumentException("Incomplete value");
    }

    void skipString() {
      take();
      while (true) {
        int value = take();
        if (value == '"') return;
        if (value < 32) throw new IllegalArgumentException("Invalid string");
        if (value == '\\') {
          value = take();
          if (value == 'u') unicode();
          else if (value != '"'
              && value != '\\'
              && value != '/'
              && value != 'b'
              && value != 'f'
              && value != 'n'
              && value != 'r'
              && value != 't') throw new IllegalArgumentException("Invalid escape");
        }
      }
    }
  }
}
