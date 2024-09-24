FROM apache/airflow:2.7.1-python3.11

USER root
RUN apt-get update && \
    apt-get install -y gcc python3-dev openjdk-11-jdk openjdk-11-jre && \
    apt-get clean
# Установите переменную окружения JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64
ENV PATH $PATH:$JAVA_HOME/bin  
USER airflow

# Установите совместимые версии провайдеров
RUN pip install --upgrade pip
RUN pip install apache-airflow==2.7.1 apache-airflow-providers-apache-spark==3.0.0 pyspark
