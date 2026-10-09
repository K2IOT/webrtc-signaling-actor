package io.webrtc.signaling.control;

import io.webrtc.signaling.auth.AuthException;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;

/** Enrollment installs this HTTP boundary; rejection never reflects private token/identity data. */
@RestControllerAdvice(assignableTypes = BootstrapController.class)
public final class NativeBootstrapErrors {
  @ExceptionHandler(AuthException.class)
  public ResponseEntity<Void> unauthenticated() {
    return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
  }
}
