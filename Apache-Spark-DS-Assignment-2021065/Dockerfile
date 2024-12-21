# Start with OpenJDK 8 as the base image
FROM openjdk:8
LABEL maintainer="Your Name <youremail@example.com>"

# Install dependencies and ensure Java compatibility
RUN apt-get update && apt-get install -y \
    wget \
    curl \
    software-properties-common \
    default-jre-headless \
    && apt-get clean

# Add the sbt repository and install sbt
RUN echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" > /etc/apt/sources.list.d/sbt.list && \
    curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x99e82a75642ac823" | apt-key add && \
    apt-get update && apt-get install -y sbt

# Install Scala
ENV SCALA_VERSION=2.12.17
RUN wget https://downloads.lightbend.com/scala/${SCALA_VERSION}/scala-${SCALA_VERSION}.deb && \
    dpkg -i scala-${SCALA_VERSION}.deb || apt-get install -f -y && \
    rm scala-${SCALA_VERSION}.deb

# Install Apache Spark
ENV SPARK_VERSION=3.4.0
RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    tar -xvzf spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop3.tgz

# Set environment variables
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH

# Set working directory
WORKDIR /app

# Copy application files into the container
COPY . /app

# Pre-build the project (optional)
RUN sbt compile

# Default command
CMD ["sbt", "run"]
