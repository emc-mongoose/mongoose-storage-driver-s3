ARG BASE_VERSION

FROM adoptopenjdk/openjdk11:alpine
ADD src ./src/
ADD gradle ./gradle/
ADD build.gradle ./build.gradle
ADD gradlew ./gradlew
ADD settings.gradle ./settings.gradle
RUN ./gradlew clean jar

FROM emcmongoose/mongoose-base:4.2.8-hacked
ARG BASE_VERSION
ARG STORAGE_DRIVER_COOP_VERSION
ARG STORAGE_DRIVER_NETTY_VERSION
ARG STORAGE_DRIVER_HTTP_VERSION
ARG VERSION
RUN mkdir -p $HOME/.mongoose/${BASE_VERSION}/ext
COPY --from=0 /build/libs/mongoose-storage-driver-s3-${VERSION}.jar /tmp/
ADD ci/docker/entrypoint_storage_driver_s3.sh /opt/mongoose/entrypoint_storage_driver_s3.sh
RUN mv -f /tmp/mongoose-storage-driver-s3-*.jar $HOME/.mongoose/${BASE_VERSION}/ext/ \
    && chmod +x /opt/mongoose/entrypoint_storage_driver_s3.sh \
    && curl http://repo.maven.apache.org/maven2/com/github/emc-mongoose/mongoose-storage-driver-coop/${STORAGE_DRIVER_COOP_VERSION}/mongoose-storage-driver-coop-${STORAGE_DRIVER_COOP_VERSION}.jar -o $HOME/.mongoose/${BASE_VERSION}/ext/mongoose-storage-driver-coop.jar \
    && curl http://repo.maven.apache.org/maven2/com/github/emc-mongoose/mongoose-storage-driver-netty/${STORAGE_DRIVER_NETTY_VERSION}/mongoose-storage-driver-netty-${STORAGE_DRIVER_NETTY_VERSION}.jar -o $HOME/.mongoose/${BASE_VERSION}/ext/mongoose-storage-driver-netty.jar \
    && curl http://repo.maven.apache.org/maven2/com/github/emc-mongoose/mongoose-storage-driver-http/${STORAGE_DRIVER_HTTP_VERSION}/mongoose-storage-driver-http-${STORAGE_DRIVER_HTTP_VERSION}.jar -o $HOME/.mongoose/${BASE_VERSION}/ext/mongoose-storage-driver-http.jar
ENTRYPOINT ["/opt/mongoose/entrypoint_storage_driver_s3.sh"]
