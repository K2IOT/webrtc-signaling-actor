package io.webrtc.signaling.app;

/** Fixed diagnostic vocabulary; no exception messages, arguments, URLs or stack traces. */
final class SafeStartupFailure {
    private SafeStartupFailure(){}
    static String render(Throwable failure){
        String code="STARTUP_FAILED";
        for(int depth=0;failure!=null&&depth<16;depth++,failure=failure.getCause()){
            if(failure instanceof NativeRuntimeStartup.MissingRuntime){code="NATIVE_RUNTIME_NOT_INSTALLED";break;}
            if(failure instanceof org.springframework.boot.context.properties.bind.BindException){code="CONFIGURATION_REJECTED";break;}
        }
        return "{\"component\":\"PLATFORM\",\"level\":\"ERROR\",\"errorCode\":\""+code+"\"}";
    }
}
