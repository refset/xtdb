FROM eclipse-temurin:21

LABEL org.opencontainers.image.source=https://github.com/xtdb/xtdb
LABEL org.opencontainers.image.description="XTDB 2.x"
LABEL org.opencontainers.image.licenses="MPL-2.0"

LABEL usage="Example docker-run options for custom logging configuration: \
--volume .:/config --env JDK_JAVA_OPTIONS='-Dlogback.configurationFile=/config/logback.xml'"

# Install CRIU using alternative method
RUN apt-get update && \
    apt-get install -y curl wget gnupg2 software-properties-common && \
    # Try to add CRIU PPA or use a static binary
    # First, try to add universe repository for older CRIU version
    add-apt-repository universe && \
    apt-get update && \
    # Install CRIU dependencies
    apt-get install -y \
        libnl-3-200 \
        libnet1 \
        libaio1 \
        libprotobuf32 \
        libprotobuf-c1 \
        python3 && \
    # Download and install CRIU binary if package doesn't work
    if ! apt-get install -y criu; then \
        echo "Package install failed, trying binary download..." && \
        cd /tmp && \
        wget https://github.com/checkpoint-restore/criu/releases/download/v3.19/criu-3.19.tar.bz2 && \
        tar xjf criu-3.19.tar.bz2 && \
        cd criu-3.19 && \
        apt-get install -y make gcc pkg-config libnl-3-dev libnet-dev libaio-dev libprotobuf-dev libprotobuf-c-dev protobuf-c-compiler protobuf-compiler python3-protobuf libbsd-dev libcap-dev && \
        make && \
        make install && \
        cd / && \
        rm -rf /tmp/criu-3.19* ; \
    fi && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /usr/local/lib/xtdb

ENTRYPOINT ["./checkpoint-entrypoint.sh"]

CMD ["-f", "local_config.yaml"]

HEALTHCHECK --start-period=15s --timeout=3s \
    CMD curl -f http://localhost:8080/healthz/alive || exit 1

EXPOSE 5432
EXPOSE 3000
EXPOSE 8080

RUN mkdir -p /var/lib/xtdb
VOLUME /var/lib/xtdb

ARG GIT_SHA
ARG XTDB_VERSION
ENV GIT_SHA=${GIT_SHA}
ENV XTDB_VERSION=${XTDB_VERSION}

LABEL org.opencontainers.image.version=${XTDB_VERSION}
LABEL org.opencontainers.image.revision=${GIT_SHA}

ADD local_config.yaml local_config.yaml
ADD build/libs/xtdb-standalone.jar xtdb-standalone.jar
ADD checkpoint-entrypoint.sh checkpoint-entrypoint.sh

RUN groupadd -g 20000 xtdb && useradd -u 20000 -g 20000 -r xtdb
RUN mkdir -p /home/xtdb && chown -R xtdb:xtdb /home/xtdb
RUN chown -R xtdb:xtdb /var/lib/xtdb
RUN chmod +x checkpoint-entrypoint.sh

USER xtdb