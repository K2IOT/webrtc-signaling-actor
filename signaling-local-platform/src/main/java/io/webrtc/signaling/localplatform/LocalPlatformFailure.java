package io.webrtc.signaling.localplatform;

/**
 * Fail-closed start failure. The only observable text is the category name; no path, value, cause
 * or stack trace is carried.
 */
public final class LocalPlatformFailure extends RuntimeException {

  /** Why the process refused to start. */
  public enum Category {
    ACKNOWLEDGEMENT,
    CONFIG,
    IDENTITY,
    KEY,
    TLS,
    DATABASE
  }

  private static final long serialVersionUID = 1L;

  private final Category category;

  public LocalPlatformFailure(Category category) {
    super(category.name(), null, false, false);
    this.category = category;
  }

  public Category category() {
    return category;
  }

  public String message() {
    return category.name();
  }
}
