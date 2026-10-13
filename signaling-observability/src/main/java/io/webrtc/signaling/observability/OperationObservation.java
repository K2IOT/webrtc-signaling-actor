package io.webrtc.signaling.observability;

/**
 * Injected observer: failures cannot alter an operation's original completion or authority.
 */
@FunctionalInterface
public interface OperationObservation {
    OperationObservation NONE = operation -> new Receipt() {
    };

    static void safeHydration(
            OperationObservation observer,
            SignalingMetrics.HydrationOutcome outcome,
            java.time.Duration elapsed) {
        try {
            observer.recoveryHydration(outcome, elapsed);
        } catch (RuntimeException ignored) {
        }
    }

    static void safeRenewal(
            OperationObservation observer,
            java.time.Instant checkedAt,
            java.time.Instant leaseUntil,
            java.time.Duration phaseTtl) {
        try {
            observer.renewalObservation(checkedAt, leaseUntil, phaseTtl);
        } catch (RuntimeException ignored) {
        }
    }

    static Receipt safeLogicalStart(
            OperationObservation observer, SignalingMetrics.Operation operation) {
        try {
            return safeReceipt(observer.startLogical(operation));
        } catch (RuntimeException ignored) {
            return new Receipt() {
            };
        }
    }

    static Receipt safeReceipt(Receipt receipt) {
        if (receipt == null) return new Receipt() {
        };
        return new Receipt() {
            public String traceId() {
                try {
                    return receipt.traceId();
                } catch (RuntimeException ignored) {
                    return "";
                }
            }

            public AutoCloseable activate() {
                final AutoCloseable scope;
                try {
                    scope = receipt.activate();
                } catch (RuntimeException ignored) {
                    return () -> {
                    };
                }
                return () -> {
                    if (scope != null)
                        try {
                            scope.close();
                        } catch (Exception ignored) {
                        }
                };
            }

            public void logical(SignalingMetrics.Outcome outcome) {
                try {
                    receipt.logical(outcome);
                } catch (RuntimeException ignored) {
                }
            }

            public void physical(boolean finished) {
                try {
                    receipt.physical(finished);
                } catch (RuntimeException ignored) {
                }
            }

            public void queue(long nanos) {
                try {
                    receipt.queue(nanos);
                } catch (RuntimeException ignored) {
                }
            }

            public void poolWait(long nanos) {
                try {
                    receipt.poolWait(nanos);
                } catch (RuntimeException ignored) {
                }
            }

            public void commit(long nanos) {
                try {
                    receipt.commit(nanos);
                } catch (RuntimeException ignored) {
                }
            }

            public void relay(SignalingMetrics.RelayOutcome outcome) {
                try {
                    receipt.relay(outcome);
                } catch (RuntimeException ignored) {
                }
            }
        };
    }

    static Receipt safeStart(OperationObservation observer, SignalingMetrics.Operation operation) {
        try {
            return safeReceipt(observer.start(operation));
        } catch (RuntimeException ignored) {
            return new Receipt() {
            };
        }
    }

    Receipt start(SignalingMetrics.Operation operation);

    default Receipt startLogical(SignalingMetrics.Operation operation) {
        return start(operation);
    }

    default void renewalObservation(
            java.time.Instant checkedAt, java.time.Instant leaseUntil, java.time.Duration phaseTtl) {
    }

    default void recoveryHydration(
            SignalingMetrics.HydrationOutcome outcome, java.time.Duration elapsed) {
    }

    interface Receipt {
        default String traceId() {
            return "";
        }

        default AutoCloseable activate() {
            return () -> {
            };
        }

        default void logical(SignalingMetrics.Outcome outcome) {
        }

        default void physical(boolean finished) {
        }

        default void queue(long elapsedNanos) {
        }

        default void poolWait(long elapsedNanos) {
        }

        default void commit(long elapsedNanos) {
        }

        default void relay(SignalingMetrics.RelayOutcome outcome) {
        }
    }
}
