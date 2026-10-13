package io.webrtc.signaling.identity;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import org.springframework.http.CacheControl;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class IdentityController {
  private final TestAccountRegistry accounts;
  private final JwtTokenIssuer tokens;
  private final IdentityProperties properties;
  private final RsaKeyMaterial keys;

  public IdentityController(
      TestAccountRegistry accounts,
      JwtTokenIssuer tokens,
      IdentityProperties properties,
      RsaKeyMaterial keys) {
    this.accounts = accounts;
    this.tokens = tokens;
    this.properties = properties;
    this.keys = keys;
  }

  @PostMapping("/api/v1/auth/login")
  public ResponseEntity<LoginResponse> login(@Valid @RequestBody LoginRequest request) {
    String userId =
        accounts
            .authenticate(request.username(), request.password())
            .orElseThrow(InvalidCredentialsException::new);
    return ResponseEntity.ok()
        .cacheControl(CacheControl.noStore())
        .body(new LoginResponse(tokens.issue(userId), "Bearer", properties.tokenTtl().toSeconds()));
  }

  @GetMapping("/.well-known/jwks.json")
  public java.util.Map<String, Object> jwks() {
    return keys.jwks();
  }

  public record LoginRequest(@NotBlank String username, @NotBlank String password) {}

  public record LoginResponse(String access_token, String token_type, long expires_in) {}
}
