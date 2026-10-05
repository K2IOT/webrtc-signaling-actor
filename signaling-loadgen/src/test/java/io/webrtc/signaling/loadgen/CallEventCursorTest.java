package io.webrtc.signaling.loadgen;

import static org.assertj.core.api.Assertions.*;
import org.junit.jupiter.api.Test;

class CallEventCursorTest {
    @Test void delayedTerminalOfAnEarlierCallCannotTouchTheCurrentCall(){
        var cursor=new CallEventCursor();cursor.bind("call-a",1);assertThat(cursor.accept("call-a",5)).isTrue();cursor.clear("call-a");cursor.bind("call-b",1);
        assertThat(cursor.accept("call-a",6)).isFalse();assertThat(cursor.call()).isEqualTo("call-b");assertThat(cursor.version()).isEqualTo(1);
    }
    @Test void oldCallVersionsCannotRegressStateButSameVersionEventsRemainReadable(){
        var cursor=new CallEventCursor();cursor.bind("call-a",3);assertThat(cursor.accept("call-a",2)).isFalse();assertThat(cursor.accept("call-a",3)).isTrue();assertThat(cursor.accept("call-a",4)).isTrue();assertThat(cursor.version()).isEqualTo(4);
    }
    @Test void oldCallCleanupCannotClearANewerBinding(){
        var cursor=new CallEventCursor();cursor.bind("call-a",1);cursor.clear("call-a");cursor.bind("call-b",1);cursor.clear("call-a");assertThat(cursor.call()).isEqualTo("call-b");
        assertThatThrownBy(()->cursor.bind("call-c",1)).isInstanceOf(IllegalStateException.class);
    }
    @Test void counterValuesRemainExactAboveJavascriptIntegerPrecision(){
        var cursor=new CallEventCursor();cursor.bind("call-a",9007199254740993L);assertThat(cursor.accept("call-a",9007199254740992L)).isFalse();assertThat(cursor.accept("call-a",Long.MAX_VALUE)).isTrue();assertThat(cursor.version()).isEqualTo(Long.MAX_VALUE);
    }
    @Test void lateRingingCannotReopenTheJustClosedCall(){
        var cursor=new CallEventCursor();cursor.bind("call-a",1);cursor.clear("call-a");
        assertThatThrownBy(()->cursor.bind("call-a",2)).isInstanceOf(IllegalStateException.class);
        cursor.bind("call-b",1);assertThat(cursor.call()).isEqualTo("call-b");
    }

}
