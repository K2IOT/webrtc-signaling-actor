package io.webrtc.signaling.actors.cluster;
import io.webrtc.signaling.actors.user.*;
import io.webrtc.signaling.actors.call.CallActor;
import io.webrtc.signaling.storage.*;
import io.webrtc.signaling.protocol.internal.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.*;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.core.*;
import org.apache.pekko.actor.ExtendedActorSystem;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.ActorRefResolver;
import org.apache.pekko.actor.typed.javadsl.Adapter;
import org.apache.pekko.serialization.SerializerWithStringManifest;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.*;
/** Stable catalog and protobuf tree. Java class names and arbitrary polymorphic objects never cross the wire. */
public final class ApplicationSerializer extends SerializerWithStringManifest {
    private static final Map<String,Class<?>> TYPES=Map.ofEntries(Map.entry("user.mutate",UserCommand.Mutate.class),Map.entry("user.result",UserCommand.Result.class),Map.entry("user.observe",UserCommand.GetState.class),Map.entry("user.state",UserState.class),Map.entry("user.stop",UserCommand.Stop.class),Map.entry("call.execute",CallActor.Execute.class),Map.entry("call.progress",CallActor.Progress.class),Map.entry("call.wake",CallActor.WakeCall.class),Map.entry("call.stop",CallActor.Stop.class),Map.entry("call.result",CallCommandService.Outcome.class),Map.entry("call.progress-result",CallWorkflowService.Outcome.class));
    private final ObjectMapper json;
    public ApplicationSerializer(org.apache.pekko.actor.ActorSystem system){
        ActorRefResolver resolver=ActorRefResolver.get(Adapter.toTyped(system));var module=new SimpleModule();
        @SuppressWarnings("unchecked") Class<ActorRef<?>> refClass=(Class<ActorRef<?>>)(Class<?>)ActorRef.class;
        module.addSerializer(refClass,new JsonSerializer<>(){@Override public void serialize(ActorRef<?> ref,JsonGenerator g,SerializerProvider p)throws IOException{g.writeString(resolver.toSerializationFormat(ref));}});
        module.addDeserializer(refClass,new JsonDeserializer<>(){@Override public ActorRef<?> deserialize(JsonParser p,DeserializationContext ctx)throws IOException{String reference=p.getValueAsString();if(reference==null||reference.length()>2048)throw new IOException("Invalid reply reference");return resolver.resolveActorRef(reference);}});
        json=new ObjectMapper().findAndRegisterModules().registerModule(module).enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    }
    public ApplicationSerializer(ExtendedActorSystem system){this((org.apache.pekko.actor.ActorSystem)system);}
    @Override public int identifier(){return 771101;}
    @Override public String manifest(Object value){if(TYPES.values().stream().noneMatch(c->c==value.getClass()))throw new IllegalArgumentException("Unregistered application message");return "v1";}
    @Override public byte[] toBinary(Object value){manifest(value);String kind=TYPES.entrySet().stream().filter(e->e.getValue()==value.getClass()).findFirst().orElseThrow().getKey();byte[] encoded=ActorEnvelope.newBuilder().setSchemaMajor(1).setSchemaMinor(0).setKind(kind).setPayload(encode(json.valueToTree(value),0,new int[]{0})).build().toByteArray();if(encoded.length>98304)throw new IllegalArgumentException("Application message exceeds 96KiB");return encoded;}
    @Override public Object fromBinary(byte[] encoded,String manifest){try{if(encoded.length<1||encoded.length>98304||!"v1".equals(manifest))throw new IllegalArgumentException("Invalid application envelope");var input=com.google.protobuf.CodedInputStream.newInstance(encoded);input.setRecursionLimit(48);var envelope=ActorEnvelope.parseFrom(input);Class<?> type=TYPES.get(envelope.getKind());if(envelope.getSchemaMajor()!=1||envelope.getSchemaMinor()>1||type==null||!envelope.hasPayload())throw new IllegalArgumentException("Unsupported application schema");return json.treeToValue(decode(envelope.getPayload(),0,new int[]{0}),type);}catch(Exception invalid){throw new IllegalArgumentException("Invalid versioned application message",invalid);}}
    private static void bound(int depth,int[] nodes){if(depth>16||++nodes[0]>8192)throw new IllegalArgumentException("Application DTO exceeds structural bounds");}
    private static ActorValue encode(JsonNode node,int depth,int[] nodes){bound(depth,nodes);var value=ActorValue.newBuilder();if(node.isNull())return value.setNullValue(true).build();if(node.isTextual())return value.setText(node.asText()).build();if(node.isNumber())return value.setDecimal(node.asText()).build();if(node.isBoolean())return value.setBoolean(node.asBoolean()).build();if(node.isArray()){var array=ActorArray.newBuilder();for(var item:node)array.addValues(encode(item,depth+1,nodes));return value.setArray(array).build();}if(node.isObject()){var object=ActorObject.newBuilder();node.fields().forEachRemaining(e->{if(e.getKey().length()>128)throw new IllegalArgumentException("Invalid field name");object.addFields(ActorField.newBuilder().setName(e.getKey()).setValue(encode(e.getValue(),depth+1,nodes)));});return value.setObject(object).build();}throw new IllegalArgumentException("Unsupported DTO node");}
    private static JsonNode decode(ActorValue value,int depth,int[] nodes){bound(depth,nodes);return switch(value.getValueCase()){
        case TEXT->TextNode.valueOf(value.getText());case DECIMAL->{if(value.getDecimal().length()>64||!value.getDecimal().matches("-?(0|[1-9][0-9]*)(\\.[0-9]+)?([eE][+-]?[0-9]+)?"))throw new IllegalArgumentException("Invalid decimal");yield DecimalNode.valueOf(new BigDecimal(value.getDecimal()));}
        case BOOLEAN->BooleanNode.valueOf(value.getBoolean());case NULL_VALUE->{if(!value.getNullValue())throw new IllegalArgumentException("Invalid null");yield NullNode.instance;}
        case ARRAY->{var array=JsonNodeFactory.instance.arrayNode();for(var item:value.getArray().getValuesList())array.add(decode(item,depth+1,nodes));yield array;}
        case OBJECT->{var object=JsonNodeFactory.instance.objectNode();for(var field:value.getObject().getFieldsList()){if(field.getName().isEmpty()||field.getName().length()>128||object.has(field.getName()))throw new IllegalArgumentException("Invalid/duplicate field");object.set(field.getName(),decode(field.getValue(),depth+1,nodes));}yield object;}
        default->throw new IllegalArgumentException("Missing DTO node");};}
}
