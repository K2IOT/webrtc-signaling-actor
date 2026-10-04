package io.webrtc.signaling.storage;
import io.webrtc.signaling.storage.HomeParticipationService.*;
import java.sql.*;
import java.time.*;
import java.util.*;
/** Confirm live homes in independent guarded transactions. A duplicate confirmation never renews a lease. */
public final class HomeActivationService {
    public record Confirmation(UUID reservationId,long version,UUID activationId,long callVersion,Winner winner,Instant validUntil,Instant confirmedAt) {}
    private final HomeParticipationService home;private final HomeProofReadService.SecurityPolicy security;
    public HomeActivationService(HomeParticipationService home){this(home,(c,u)->false);}
    public HomeActivationService(HomeParticipationService home,HomeProofReadService.SecurityPolicy security){this.home=Objects.requireNonNull(home);this.security=Objects.requireNonNull(security);}
    public DbOperation<Confirmation> confirmTracked(Request request,UUID reservation,long expectedVersion,UUID activation,long callVersion,Winner winner,UUID operation,Duration budget){
        Objects.requireNonNull(reservation);Objects.requireNonNull(activation);Objects.requireNonNull(operation);if(expectedVersion<1||callVersion<1)throw new IllegalArgumentException("Invalid activation version");
        return home.submitTracked(request,DbClass.CRITICAL,budget,new AuthorizationIntent("CONFIRM",reservation,expectedVersion,activation,callVersion,winner,null),c->{if(!security.current(c,request.user()))throw new AuthoritySql.FencedException();var current=home.find(c,request);
            if(current==null||current.terminal()||!reservation.equals(current.reservationId())||request.grant().groupEpoch()<current.highestGroupEpoch()||!Objects.equals(winner,current.winner()))throw new AuthoritySql.FencedException();
            UUID bound,previousOperation;long boundVersion;Instant confirmed;
            try(var q=c.prepareStatement("SELECT h.activation_id,h.activation_call_version,h.activation_operation,h.activation_confirmed_at,r.lease_until>clock_timestamp()+interval '5 seconds' FROM home_participation h JOIN user_reservation r USING(user_id,call_id,reservation_id) WHERE h.call_id=? AND h.user_id=?")){q.setString(1,request.call().value());q.setString(2,request.user().value());try(var r=q.executeQuery()){if(!r.next()||!r.getBoolean(5))throw new AuthoritySql.FencedException();bound=r.getObject(1,UUID.class);boundVersion=r.getLong(2);previousOperation=r.getObject(3,UUID.class);confirmed=r.getTimestamp(4)==null?null:r.getTimestamp(4).toInstant();}}
            if(bound!=null){if(!activation.equals(bound)||boundVersion!=callVersion||!operation.equals(previousOperation))throw new AuthoritySql.FencedException();return new Confirmation(reservation,current.version(),bound,boundVersion,current.winner(),current.leaseUntil(),confirmed);}
            if(current.version()!=expectedVersion)throw new AuthoritySql.FencedException();long version=Math.addExact(expectedVersion,1);
            try(var q=c.prepareStatement("UPDATE user_reservation SET activation_id=?,activation_call_version=?,phase='ACTIVE',reservation_version=?,highest_group_epoch=? WHERE user_id=? AND call_id=? AND reservation_id=? AND reservation_version=? AND lease_until>clock_timestamp()+interval '5 seconds'")){q.setObject(1,activation);q.setLong(2,callVersion);q.setLong(3,version);q.setLong(4,request.grant().groupEpoch());q.setString(5,request.user().value());q.setString(6,request.call().value());q.setObject(7,reservation);q.setLong(8,expectedVersion);if(q.executeUpdate()!=1)throw new AuthoritySql.FencedException();}
            try(var q=c.prepareStatement("UPDATE home_participation SET activation_id=?,activation_call_version=?,activation_operation=?,activation_confirmed_at=clock_timestamp(),phase='ACTIVE',highest_group_epoch=? WHERE user_id=? AND call_id=? AND phase IN ('RESERVED','CLAIMED') AND activation_id IS NULL RETURNING activation_confirmed_at")){q.setObject(1,activation);q.setLong(2,callVersion);q.setObject(3,operation);q.setLong(4,request.grant().groupEpoch());q.setString(5,request.user().value());q.setString(6,request.call().value());try(var r=q.executeQuery()){if(!r.next())throw new AuthoritySql.FencedException();confirmed=r.getTimestamp(1).toInstant();}}
            return new Confirmation(reservation,version,activation,callVersion,current.winner(),current.leaseUntil(),confirmed);
        });
    }
}
