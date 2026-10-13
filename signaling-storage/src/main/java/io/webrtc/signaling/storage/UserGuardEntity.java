package io.webrtc.signaling.storage;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/**
 * Administrative persistence mapping; never loaded for authority decisions.
 */
@Entity
@Table(name = "user_guard")
public class UserGuardEntity {
    @Id
    @Column(name = "user_id")
    private String userId;

    protected UserGuardEntity() {
    }
}
