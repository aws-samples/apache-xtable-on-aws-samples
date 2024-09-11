# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

# syntax = docker/dockerfile:1.7.0
# args
ARG MAVEN_VERSION=3.9.6
ARG MAVEN_MAJOR_VERSION=3
ARG XTABLE_VERSION=0.1.0-incubating
ARG XTABLE_BRANCH=0.1.0-incubating
ARG ICEBERG_VERSION=1.4.2
ARG SPARK_VERSION=3.4
ARG SCALA_VERSION=2.12
ARG ARCH=aarch64

# Build Stage
FROM public.ecr.aws/lambda/python:3.12.2024.07.10.11-arm64 AS build-stage
WORKDIR /

ARG MAVEN_VERSION
ARG MAVEN_MAJOR_VERSION
ARG XTABLE_BRANCH
ARG ICEBERG_VERSION
ARG SPARK_VERSION
ARG SCALA_VERSION

# install java
RUN dnf install -y java-11-amazon-corretto-headless \
    git \
    unzip \
    wget \
    gcc \
    g++ \
    && dnf clean all

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

# Copy requirements.txt
COPY requirements.txt .

# Install the specified packages
RUN pip install -r requirements.txt -t requirements/

USER 1000

# Run stage
FROM public.ecr.aws/lambda/python:3.12.2024.07.10.11-arm64

# args
ARG XTABLE_VERSION
ARG ICEBERG_VERSION
ARG SPARK_VERSION
ARG SCALA_VERSION
ARG ARCH

# copy java
COPY --from=build-stage /usr/lib/jvm/java-11-amazon-corretto."$ARCH" /usr/lib/jvm/java-11-amazon-corretto."$ARCH"

# Copy jar files
COPY --from=build-stage /incubator-xtable/xtable-utilities/target/xtable-utilities-"$XTABLE_VERSION"-bundled.jar "$LAMBDA_TASK_ROOT"/jars/
COPY --from=build-stage /iceberg-aws-bundle-"$ICEBERG_VERSION".jar "$LAMBDA_TASK_ROOT"/jars_iceberg/
COPY --from=build-stage /iceberg-spark-runtime-"$SPARK_VERSION"_"$SCALA_VERSION"-"$ICEBERG_VERSION".jar "$LAMBDA_TASK_ROOT"/jars_iceberg/

# Copy python requirements
COPY --from=build-stage /requirements "$LAMBDA_TASK_ROOT"

# Copy src code
COPY . "$LAMBDA_TASK_ROOT"