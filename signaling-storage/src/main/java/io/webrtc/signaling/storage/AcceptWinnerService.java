package io.webrtc.signaling.storage;
import io.webrtc.signaling.storage.HomeParticipationService.*;
import java.sql.*;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
public final class AcceptWinnerService {
    public record Claim(String outcome,Winner winner,UUID reservation,long version,java.time.Instant validUntil) {}
    private final HomeParticipationService home;private final SessionRepository sessions=new SessionRepository();
    public AcceptWinnerService(HomeParticipationService home){this.home=home;}
    public CompletionStage<Claim> claimAccept(Request r,UUID reservation,SessionRepository.Route route){return home.submit(r,DbClass.CRITICAL,c->{
        if(!r.user().equals(route.user()))throw new SessionRepository.BindingRejected();sessions.requireCurrent(c,route);
        Participation current=home.find(c,r);if(current==null||current.terminal())return new Claim("TERMINAL",current==null?null:current.winner(),reservation,0,null);
        if(!reservation.equals(current.reservationId())||current.leaseUntil()==null||r.grant().groupEpoch()<current.highestGroupEpoch())throw new AuthoritySql.FencedException();
        try(var s=c.prepareStatement("SELECT lease_until>clock_timestamp() FROM user_reservation WHERE user_id=? AND call_id=? AND reservation_id=?")){s.setString(1,r.user().value());s.setString(2,r.call().value());s.setObject(3,reservation);try(var result=s.executeQuery()){if(!result.next()||!result.getBoolean(1))throw new AuthoritySql.FencedException();}}
        Winner proposed=new Winner(route.key(),route.incarnation(),route.connectionGeneration());
        if(current.winner()!=null)return new Claim(current.winner().equals(proposed)?"CLAIMED":"ANSWERED_ELSEWHERE",current.winner(),reservation,current.version(),current.leaseUntil());
        long version=Math.addExact(current.version(),1);
        try(var s=c.prepareStatement("UPDATE user_reservation SET winner_issuer=?,winner_jti=?,winner_incarnation=?,winner_generation=?,phase='CLAIMED',reservation_version=? WHERE user_id=? AND call_id=? AND reservation_id=? AND reservation_version=? AND winner_issuer IS NULL AND lease_until>clock_timestamp()")){
            bindWinner(s,proposed);s.setLong(5,version);s.setString(6,r.user().value());s.setString(7,r.call().value());s.setObject(8,reservation);s.setLong(9,current.version());if(s.executeUpdate()!=1)throw new AuthoritySql.FencedException();
        }
        try(var s=c.prepareStatement("UPDATE home_participation SET winner_issuer=?,winner_jti=?,winner_incarnation=?,winner_generation=?,phase='CLAIMED',highest_group_epoch=? WHERE call_id=? AND user_id=? AND phase='RESERVED' AND winner_issuer IS NULL")){
            bindWinner(s,proposed);s.setLong(5,r.grant().groupEpoch());s.setString(6,r.call().value());s.setString(7,r.user().value());if(s.executeUpdate()!=1)throw new AuthoritySql.FencedException();
        }return new Claim("CLAIMED",proposed,reservation,version,current.leaseUntil());
    });}
    private static void bindWinner(PreparedStatement s,Winner w)throws SQLException {s.setString(1,w.key().issuer());s.setString(2,w.key().jti());s.setObject(3,w.incarnation().value());s.setLong(4,w.generation());}
}
