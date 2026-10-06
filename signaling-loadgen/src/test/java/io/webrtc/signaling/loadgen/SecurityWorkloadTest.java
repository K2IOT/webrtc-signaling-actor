package io.webrtc.signaling.loadgen;

import static org.assertj.core.api.Assertions.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.*;
import org.junit.jupiter.api.Test;

class SecurityWorkloadTest {
    private final ObjectMapper json=new ObjectMapper();
    @Test void deterministicSelectionUsesOriginalGlobalArrivalOrdinal()throws Exception {
        var profile=json.readTree("{\"abuse\":[\"malformed\",\"oversized\"],\"abuseFraction\":0.01}");
        var first=SecurityWorkload.parse(profile);var second=SecurityWorkload.parse(profile);
        var modes=EnumSet.noneOf(VirtualClient.ProbeKind.class);int selected=0;
        for(long ordinal=0;ordinal<100000;ordinal++){
            var a=first.kind(123,ordinal);assertThat(second.kind(123,ordinal)).isEqualTo(a);
            if(a.isPresent()){selected++;modes.add(a.get());}
        }
        assertThat(selected).isBetween(850,1150);
        assertThat(modes).containsExactlyInAnyOrder(VirtualClient.ProbeKind.MALFORMED,VirtualClient.ProbeKind.OVERSIZED);
    }
    @Test void emptyProfileNeverCreatesSecurityTraffic()throws Exception {
        var profile=SecurityWorkload.parse(json.readTree("{}"));
        assertThat(profile.kind(123,0)).isEmpty();assertThat(profile.snapshot().get("attempted")).isEqualTo(0L);
    }
}
