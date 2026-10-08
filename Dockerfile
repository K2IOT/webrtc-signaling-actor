# Verified linux/amd64 manifest for the pinned JRE 21.0.8+9.
FROM eclipse-temurin:21.0.8_9-jre-jammy@sha256:cddd554e8d69b48b46e8b0c9d1ce72ae5fe8d84819dcdb7131328531e9cc100b
ARG SOURCE_COMMIT
LABEL org.opencontainers.image.title="webrtc-signaling" \
      org.opencontainers.image.revision="${SOURCE_COMMIT}" \
      org.opencontainers.image.base.name="eclipse-temurin:21.0.8_9-jre-jammy" \
      org.opencontainers.image.base.digest="sha256:cddd554e8d69b48b46e8b0c9d1ce72ae5fe8d84819dcdb7131328531e9cc100b" \
      io.webrtc.signaling.release.status="NOT_QUALIFIED"
WORKDIR /opt/signaling
ENV JDK_JAVA_OPTIONS="--add-exports=java.base/jdk.internal.misc=ALL-UNNAMED"
COPY --chown=10001:10001 signaling-app/target/signaling-app-0.1.0-SNAPSHOT-exec.jar app.jar
USER 10001:10001
ENTRYPOINT ["java", "-jar", "/opt/signaling/app.jar"]
