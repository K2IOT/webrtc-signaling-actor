package io.webrtc.signaling.control;

public final class WrongCellException extends IllegalArgumentException {
  public WrongCellException() {
    super("WRONG_CELL");
  }
}
