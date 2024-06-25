# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

# syntax = docker/dockerfile:1.7.0
# args
ARG MAVEN_VERSION=3.9.6
ARG MAVEN_MAJOR_VERSION=3
ARG XTABLE_VERSION=0.1.0-SNAPSHOT
ARG XTABLE_BRANCH=v0.1.0-beta1
ARG ICEBERG_VERSION=1.4.2
ARG SPARK_VERSION=3.4
ARG SCALA_VERSION=2.12

# Build Stage
from public.ecr.aws/amazonlinux/amazonlinux:2023.4.20240319.1 as build-stage

ARG MAVEN_VERSION
ARG MAVEN_MAJOR_VERSION
ARG XTABLE_BRANCH
ARG ICEBERG_VERSION
ARG SPARK_VERSION
ARG SCALA_VERSION

# install java
RUN yum install -y java-11-amazon-corretto-headless \
    git \
    unzip \
    wget \
    && yum clean all

# install maven
RUN wget https://dlcdn.apache.org/maven/maven-"$MAVEN_MAJOR_VERSION"/"$MAVEN_VERSION"/binaries/apache-maven-"$MAVEN_VERSION"-bin.zip
RUN unzip apache-maven-"$MAVEN_VERSION"-bin.zip

# clone sources
RUN git clone --depth 1 --branch "$XTABLE_BRANCH" https://github.com/apache/incubator-xtable.git

# build xtable jar
WORKDIR /incubator-xtable
RUN /apache-maven-"$MAVEN_VERSION"/bin/mvn package -DskipTests=true
WORKDIR /

# Download jars for iceberg and glue
RUN wget https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-aws-bundle/"$ICEBERG_VERSION"/iceberg-aws-bundle-"$ICEBERG_VERSION".jar
RUN wget https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-spark-runtime-"$SPARK_VERSION"_"$SCALA_VERSION"/"$ICEBERG_VERSION"/iceberg-spark-runtime-"$SPARK_VERSION"_"$SCALA_VERSION"-"$ICEBERG_VERSION".jar

USER 1000

# Run Stage
from scratch as out-stage

# args
ARG XTABLE_VERSION
ARG ICEBERG_VERSION
ARG SPARK_VERSION
ARG SCALA_VERSION

# Copy jar files
COPY --from=build-stage /incubator-xtable/utilities/target/utilities-"$XTABLE_VERSION"-bundled.jar /xtable_operator/jars/
COPY --from=build-stage /iceberg-aws-bundle-"$ICEBERG_VERSION".jar /xtable_operator/jars/
COPY --from=build-stage /iceberg-spark-runtime-"$SPARK_VERSION"_"$SCALA_VERSION"-"$ICEBERG_VERSION".jar /xtable_operator/jars/

# Add python operator
COPY operator.py /xtable_operator/
COPY cmd.py /xtable_operator/