FROM flink:1.19-scala_2.12-java11

# The official Flink image already provides a Java 11 runtime under
# /opt/java/openjdk. Do not hard-code an ARM64 Ubuntu path: this project also
# runs on x86_64 WSL/Docker Desktop machines.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3 \
        python3-pip \
        python3-dev \
        build-essential \
        wget && \
    rm -rf /var/lib/apt/lists/*

RUN ln -sf /usr/bin/python3 /usr/bin/python

ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="${JAVA_HOME}/bin:${PATH}"

RUN pip3 install --no-cache-dir --upgrade pip setuptools wheel
RUN pip3 install --no-cache-dir apache-flink==1.19.0
RUN pip3 install --no-cache-dir \
    confluent-kafka \
    fastavro \
    requests \
    clickhouse-driver

# Kafka SQL connector compatible with Flink 1.19.
RUN wget -P /opt/flink/lib/ \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.19/flink-sql-connector-kafka-3.3.0-1.19.jar

# Confluent Schema Registry Avro format.
RUN wget -P /opt/flink/lib/ \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-avro-confluent-registry/1.19.0/flink-sql-avro-confluent-registry-1.19.0.jar

# ClickHouse connector used by the existing platform.
RUN wget -P /opt/flink/lib/ \
    https://repo1.maven.org/maven2/com/clickhouse/flink/flink-connector-clickhouse-1.17/0.1.3/flink-connector-clickhouse-1.17-0.1.3-all.jar

# S3 filesystem plugin for MinIO-backed Flink checkpoints.
RUN mkdir -p /opt/flink/plugins/s3-fs-hadoop && \
    cp /opt/flink/opt/flink-s3-fs-hadoop-*.jar /opt/flink/plugins/s3-fs-hadoop/

# Iceberg runtime and AWS bundle for Nessie + MinIO.
RUN wget -P /opt/flink/lib/ \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.19/1.7.1/iceberg-flink-runtime-1.19-1.7.1.jar

RUN wget -P /opt/flink/lib/ \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.7.1/iceberg-aws-bundle-1.7.1.jar

RUN wget -P /opt/flink/lib/ \
    https://repo1.maven.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar

WORKDIR /opt/flink
