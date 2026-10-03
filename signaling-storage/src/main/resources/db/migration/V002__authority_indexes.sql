CREATE INDEX session_current_user_idx ON session_registry(user_id,issuer,jti) WHERE closed_at IS NULL;
CREATE INDEX session_current_gateway_idx ON session_registry(gateway_id,boot_id,issuer,jti) WHERE closed_at IS NULL;
CREATE INDEX call_active_group_idx ON call_state(ownership_hash_version,ownership_group_id,call_id) WHERE terminal_at IS NULL;
CREATE INDEX call_terminal_retire_idx ON call_state(expires_at,call_id) WHERE terminal_at IS NOT NULL;
CREATE INDEX home_terminal_retire_idx ON home_participation(expires_at,call_id,user_id) WHERE terminal_at IS NOT NULL;
CREATE INDEX command_final_retire_idx ON command_result(expires_at,issuer,jti,command_scope,request_id) WHERE status='FINAL';
CREATE INDEX outbox_pending_idx ON control_outbox(next_attempt,event_id) WHERE delivery_state='PENDING';
CREATE INDEX outbox_reclaim_idx ON control_outbox(dispatch_until,event_id) WHERE delivery_state='INFLIGHT';
