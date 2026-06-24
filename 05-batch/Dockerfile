ARG python_version=3.12

FROM python:${python_version}-slim-bookworm

ARG spark_version=3.5.5
ARG hadoop_version=3
ARG jdk_version=17

RUN apt-get update -y && \
    apt-get install -y wget procps openjdk-${jdk_version}-jre-headless && \
    apt-get clean && \
    wget https://dlcdn.apache.org/spark/spark-${spark_version}/spark-${spark_version}-bin-hadoop3.tgz && \
    tar -xf spark-${spark_version}-bin-hadoop3.tgz -C /usr/local/bin/ && \
    rm spark-${spark_version}-bin-hadoop3.tgz && \
    pip install notebook && \
    useradd -m -u 1000 -g 0 user

ENV SPARK_HOME=/usr/local/bin/spark-${spark_version}-bin-hadoop3
ENV PYSPARK_PYTHON=python
ENV PATH=$PATH:$SPARK_HOME/bin
ENV PYTHONPATH=$SPARK_HOME/python/:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip

USER user
WORKDIR /home/user/code

ENTRYPOINT [ "jupyter", "notebook", "--ip=0.0.0.0", "--port=8080", "--NotebookApp.token=''", "--no-browser" ]