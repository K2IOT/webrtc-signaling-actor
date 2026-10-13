package io.webrtc.signaling.control;

import io.webrtc.signaling.auth.AuthException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * Enrollment installs this HTTP boundary; rejection never reflects private token/identity data.
 */
@RestControllerAdvice(assignableTypes = BootstrapController.class)
public final class NativeBootstrapErrors {
    @ExceptionHandler(AuthException.class)
    public ResponseEntity<Void> unauthenticated() {
        return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
    }
}
