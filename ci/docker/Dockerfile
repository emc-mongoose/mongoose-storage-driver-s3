ARG BASE_DRIVER_VERSION

FROM adoptopenjdk/openjdk11:alpine
ADD src ./src/
ADD gradle ./gradle/
ADD build.gradle ./build.gradle
ADD gradlew ./gradlew
ADD settings.gradle ./settings.gradle
RUN ./gradlew clean jar

FROM emcmongoose/mongoose-storage-driver-http:${BASE_DRIVER_VERSION}
ARG BASE_VERSION
ARG VERSION
COPY --from=0 /build/libs/mongoose-storage-driver-s3-${VERSION}.jar /tmp/
ADD ci/docker/entrypoint_storage_driver_s3.sh /opt/mongoose/entrypoint_storage_driver_s3.sh
RUN mv -f /tmp/mongoose-storage-driver-s3-*.jar $HOME/.mongoose/${BASE_VERSION}/ext/; \
    chmod +x /opt/mongoose/entrypoint_storage_driver_s3.sh
ENTRYPOINT ["/opt/mongoose/entrypoint_storage_driver_s3.sh"]
