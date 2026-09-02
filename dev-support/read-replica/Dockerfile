# Stage 0: Cache Maven dependencies
ARG BASE_IMAGE=registry.access.redhat.com/ubi8/openjdk-17:1.23
FROM ${BASE_IMAGE} AS cache-stage

# Switch to user root (UID 0)
# hadolint ignore=DL3002
USER 0

# Install necessary packages for building Maven dependencies
# hadolint ignore=DL3041
RUN microdnf update -y && microdnf install -y \
        maven \
        git \
        hostname \
        diffutils \
    && microdnf clean all

# Copy the entire source code to cache dependencies
COPY ./hbase /opt/hbase-src

WORKDIR /opt/hbase-src

# Download and cache all dependencies
RUN mvn clean install -DskipTests -Dskip.license.check=true

# Stage 1: Build the HBase source code
FROM ${BASE_IMAGE} AS build-stage

# Switch to user root (UID 0)
# hadolint ignore=DL3002
USER 0

# Install necessary build packages
# hadolint ignore=DL3041
RUN microdnf update -y && microdnf install -y \
        maven \
        git \
        hostname \
        diffutils \
    && microdnf clean all

# Copy the cached Maven dependencies
COPY --from=cache-stage /root/.m2 /root/.m2

# Copy the HBase source code
COPY ./hbase /opt/hbase-src

WORKDIR /opt/hbase-src

# Build HBase source code using cached dependencies and enable parallel build
RUN mvn clean package -DskipTests -Dskip.license.check=true assembly:single -T 1C

# Stage 2: Create the final Docker image
FROM ${BASE_IMAGE}

# Switch to user root (UID 0)
USER 0

# Set environment variables
ENV HBASE_HOME=/opt/hbase
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk \
    HBASE_USER=hbase \
    HBASE_CONF_DIR=${HBASE_HOME}/conf \
    HBASE_LIB_DIR=${HBASE_HOME}/lib \
    HBASE_LOGS_DIR=${HBASE_HOME}/logs \
    DATA_DIR=/data-store

# Install necessary runtime packages
# hadolint ignore=DL3041
RUN microdnf update -y && microdnf install -y \
        unzip \
        gzip \
        wget \
        hostname \
        maven \
        git \
        diffutils \
        vim \
        openssh-clients \
        python3 \
        procps \
    && microdnf clean all

# Copy the built HBase binaries from the build-stage
COPY --from=build-stage /opt/hbase-src/hbase-assembly/target/hbase-4.0.0-alpha-1-SNAPSHOT-bin.tar.gz /opt/

# Extract HBase binaries
RUN tar -xzf /opt/hbase-4.0.0-alpha-1-SNAPSHOT-bin.tar.gz -C /opt \
    && ln -s /opt/hbase-4.0.0-alpha-1-SNAPSHOT /opt/hbase \
    && rm /opt/hbase-4.0.0-alpha-1-SNAPSHOT-bin.tar.gz

COPY --from=build-stage /root/.m2/repository/org/apache/hadoop/hadoop-mapreduce-client-common/*/hadoop-mapreduce-client-common-*.jar \
                        ${HBASE_HOME}/lib/

# Apply custom HBase build steps
RUN sed -i "s,^. export JAVA_HOME.*,export JAVA_HOME=$JAVA_HOME," ${HBASE_CONF_DIR}/hbase-env.sh \
    && sed -E -i 's/(.*)hbase\-daemons\.sh(.*zookeeper)/\1hbase-daemon.sh\2/g' ${HBASE_HOME}/bin/start-hbase.sh \
    && printf "JAVA_HOME=%s\nexport JAVA_HOME\nexport PATH=%s/jre/bin:%s\n" "$JAVA_HOME" "$JAVA_HOME" "$PATH" \
            > /etc/profile.d/defaults.sh \
    && ln -sf ${HBASE_HOME}/bin/* /usr/bin

# Create HBase user and home directory
RUN useradd -u 1000 -m ${HBASE_USER} \
    && mkdir -p /home/${HBASE_USER} \
    && chown -R ${HBASE_USER}:${HBASE_USER} /opt /home/${HBASE_USER} \
    && mkdir "$DATA_DIR" && chown -R ${HBASE_USER}:${HBASE_USER} "$DATA_DIR"

# Set permissions for jboss home directory
RUN chown -R ${HBASE_USER}:${HBASE_USER} /home/jboss

# Copy configuration files - Removed in order to mount config files individually #

# Expose required ports for HBase and related services
EXPOSE 8000 8080 8085 9090 9095 2181 16000 16010 16020 16030

# Set up the utils directory as a volume
VOLUME ["/opt/utils"]

# Switch to the HBase user (UID 1000)
USER 1000

# Create necessary directories for HBase to run
RUN mkdir -p "$DATA_DIR"/hbase "$DATA_DIR"/run "$DATA_DIR"/logs && \
    chmod -R 777 "$DATA_DIR"

# Add the 'ls -l' alias
RUN echo 'alias ll="ls -l"' >> /home/hbase/.bashrc && \
    echo 'alias ll="ls -l"' >> /home/jboss/.bashrc

# Start HBase and keep it running
ENTRYPOINT ["/bin/bash", "-c", "${HBASE_HOME}/bin/start-hbase.sh && tail -f ${HBASE_LOGS_DIR}/*.log"]
