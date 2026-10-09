package io.webrtc.signaling.storage;

import jakarta.persistence.*;

/** Administrative persistence mapping; never loaded for authority decisions. */
@Entity
@Table(name = "user_guard")
public class UserGuardEntity {
  @Id
  @Column(name = "user_id")
  private String userId;

  protected UserGuardEntity() {}
}
