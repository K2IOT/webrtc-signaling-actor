package io.webrtc.signaling.app.runtime.local;

import java.util.Locale;

/**
 * Specific, secret-free startup failure of the local runtime adapter. It names the input role and a
 * failure category only; it never carries file content, paths, identifiers or a cause.
 */
public final class LocalRuntimeFailure extends IllegalStateException {
    public enum Role {
        CONTRACT,
        PROFILE,
        IDENTITY,
        PATH
    }

    public enum Category {
        /** Absent, not a regular file, or unreadable. */
        MISSING,
        TOO_LARGE,
        /** Empty, not UTF-8, not JSON, trailing content, or beyond the nesting bound. */
        MALFORMED,
        DUPLICATE_KEY,
        /** Well-formed JSON that fails the typed schema or its validation. */
        INVALID,
        IDENTITY_MISMATCH,
        PATH_ESCAPE,
        /** A readable contract that declares an environment other than local-test-only. */
        ENVIRONMENT_MISMATCH
    }

    private final Role role;
    private final Category category;

    LocalRuntimeFailure(Role role, Category category) {
        super("Local runtime " + role.name().toLowerCase(Locale.ROOT) + ": " + category.name());
        this.role = role;
        this.category = category;
    }

    public Role role() {
        return role;
    }

    public Category category() {
        return category;
    }
}
