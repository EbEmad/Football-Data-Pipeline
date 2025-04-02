FROM apache/airflow:2.8.3

USER root

# Install Java and Spark
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jdk \
        procps \
        curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Download and install Spark
RUN curl -s https://downloads.apache.org/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz | tar -xz -C /usr/local && \
    ln -s /usr/local/spark-3.5.0-bin-hadoop3 /usr/local/spark

# Set environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_HOME=/usr/local/spark
ENV PATH="${JAVA_HOME}/bin:${SPARK_HOME}/bin:${PATH}"

USER airflow

# Install PySpark (optional, only needed for Python code)
RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark==4.1.0 \
    pyspark==3.5.0