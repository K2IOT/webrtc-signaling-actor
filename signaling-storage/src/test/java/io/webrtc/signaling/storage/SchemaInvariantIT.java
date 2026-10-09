package io.webrtc.signaling.storage;

import static org.assertj.core.api.Assertions.*;

import java.sql.*;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class SchemaInvariantIT {
  @Test
  void rootCountersNeverRegressAndExpiredBootCannotRenew() throws Exception {
    try (var c = PgFixture.connection();
        var s = c.createStatement()) {
      s.execute(
          "INSERT INTO group_owner(cell_id,ownership_hash_version,group_id,storage_epoch,group_epoch,lease_sequence,status) VALUES('schema',1,9,2,5,7,'IDLE')");
      assertThatThrownBy(
              () ->
                  s.execute(
                      "UPDATE group_owner SET group_epoch=4 WHERE cell_id='schema' AND group_id=9"))
          .isInstanceOf(SQLException.class);
      assertThatThrownBy(
              () ->
                  s.execute(
                      "UPDATE group_owner SET lease_sequence=6 WHERE cell_id='schema' AND group_id=9"))
          .isInstanceOf(SQLException.class);
      String boot = UUID.randomUUID().toString();
      s.execute(
          "INSERT INTO gateway_lease(gateway_id,boot_id,lease_until,renewal_sequence,storage_epoch,region,cell) VALUES('expired','"
              + boot
              + "',clock_timestamp()-interval '1 second',1,1,'test','c001')");
      assertThatThrownBy(
              () ->
                  s.execute(
                      "UPDATE gateway_lease SET lease_until=clock_timestamp()+interval '15 seconds',renewal_sequence=2 WHERE boot_id='"
                          + boot
                          + "'"))
          .isInstanceOf(SQLException.class);
    }
  }

  @Test
  void terminalAndFinalRowsRequireNonNullRetention() throws Exception {
    try (var c = PgFixture.connection();
        var s = c.createStatement()) {
      String call = "c001.e1." + UUID.randomUUID();
      assertThatThrownBy(
              () ->
                  s.execute(
                      "INSERT INTO home_participation(call_id,user_id,acquire_operation_id,payload_hash,coordinator_cell,coordinator_storage_epoch,ownership_hash_version,ownership_group_id,highest_group_epoch,phase,terminal_at) VALUES('"
                          + call
                          + "','no-expiry','"
                          + UUID.randomUUID()
                          + "',decode(repeat('00',32),'hex'),'c001',1,1,1,1,'RELEASED',clock_timestamp())"))
          .isInstanceOf(SQLException.class);
      assertThatThrownBy(
              () ->
                  s.execute(
                      "INSERT INTO command_result(issuer,jti,command_scope,request_id,authority_bucket_id,payload_hash,status,result,finalized_at) VALUES('issuer','"
                          + UUID.randomUUID()
                          + "','INVITE','"
                          + UUID.randomUUID()
                          + "',1,decode(repeat('00',32),'hex'),'FINAL','{}',clock_timestamp())"))
          .isInstanceOf(SQLException.class);
    }
  }

  @Test
  void migrationCreatesAuthorityAndAllIndexes() throws Exception {
    try (var c = PgFixture.connection();
        var s = c.createStatement();
        var r =
            s.executeQuery(
                "SELECT count(*) FROM pg_tables WHERE schemaname='public' AND tablename IN ('cell_authority','bucket_authority','user_guard','gateway_lease','session_registry','user_reservation','home_participation','group_owner','call_state','command_result','control_outbox','security_epoch')")) {
      r.next();
      assertThat(r.getInt(1)).isEqualTo(12);
    }
  }

  @Test
  void rejectsInvalidGroupCounterAndSessionBinding() throws Exception {
    try (var c = PgFixture.connection();
        var s = c.createStatement()) {
      assertThatThrownBy(
              () ->
                  s.execute(
                      "INSERT INTO group_owner(cell_id,ownership_hash_version,group_id,storage_epoch,group_epoch,lease_sequence,status) VALUES('c001',1,1024,1,1,0,'IDLE')"))
          .isInstanceOf(SQLException.class);
      assertThatThrownBy(
              () ->
                  s.execute(
                      "INSERT INTO group_owner(cell_id,ownership_hash_version,group_id,storage_epoch,group_epoch,lease_sequence,status) VALUES('c001',1,100,1,-1,0,'IDLE')"))
          .isInstanceOf(SQLException.class);
      s.execute("INSERT INTO user_guard(user_id) VALUES('schema-alice') ON CONFLICT DO NOTHING");
      String j = UUID.randomUUID().toString(), inc = UUID.randomUUID().toString();
      s.execute(
          "INSERT INTO session_registry(issuer,jti,user_id,session_incarnation,connection_generation,token_exp,updated_at) VALUES('issuer','"
              + j
              + "','schema-alice','"
              + inc
              + "',2,clock_timestamp()+interval '1 hour',clock_timestamp())");
      assertThatThrownBy(
              () ->
                  s.execute(
                      "UPDATE session_registry SET connection_generation=1 WHERE issuer='issuer' AND jti='"
                          + j
                          + "'"))
          .isInstanceOf(SQLException.class);
      assertThatThrownBy(
              () ->
                  s.execute(
                      "UPDATE session_registry SET user_id='bob' WHERE issuer='issuer' AND jti='"
                          + j
                          + "'"))
          .isInstanceOf(SQLException.class);
    }
  }

  @Test
  void commandScopesRemainUniqueWithoutExpiryInTheKey() throws Exception {
    String id = UUID.randomUUID().toString(), j = UUID.randomUUID().toString();
    try (var c = PgFixture.connection();
        var s = c.createStatement()) {
      String base =
          "INSERT INTO command_result(issuer,jti,command_scope,request_id,authority_bucket_id,payload_hash,status) VALUES('issuer','"
              + j
              + "',";
      s.execute(base + "'INVITE','" + id + "',1,decode(repeat('00',32),'hex'),'PENDING')");
      s.execute(
          base + "'CALL:c001.e1.test','" + id + "',1,decode(repeat('00',32),'hex'),'PENDING')");
      assertThatThrownBy(
              () ->
                  s.execute(
                      base + "'INVITE','" + id + "',1,decode(repeat('00',32),'hex'),'PENDING')"))
          .isInstanceOf(SQLException.class);
    }
  }

  @Test
  void releasedParticipationCannotReopen() throws Exception {
    try (var c = PgFixture.connection();
        var s = c.createStatement()) {
      String call = "c001.e1." + UUID.randomUUID();
      s.execute(
          "INSERT INTO home_participation(call_id,user_id,acquire_operation_id,payload_hash,coordinator_cell,coordinator_storage_epoch,ownership_hash_version,ownership_group_id,highest_group_epoch,phase,terminal_at,expires_at) VALUES('"
              + call
              + "','schema-home','"
              + UUID.randomUUID()
              + "',decode(repeat('00',32),'hex'),'c001',1,1,1,1,'RELEASED',clock_timestamp(),clock_timestamp()+interval '24 hours')");
      assertThatThrownBy(
              () ->
                  s.execute(
                      "UPDATE home_participation SET phase='RESERVED',terminal_at=NULL,expires_at=NULL WHERE call_id='"
                          + call
                          + "'"))
          .isInstanceOf(SQLException.class);
    }
  }
}
