FROM apache/airflow:2.7.1-python3.11  

USER root  
 
RUN apt-get update && \
    apt-get install -y gcc python3-dev openjdk-11-jdk openjdk-11-jre procps && \
    apt-get clean  


ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64  
ENV PATH="$JAVA_HOME/bin:$PATH"  

RUN mkdir -p /opt/airflow/logs /opt/airflow/dags /opt/airflow/input /opt/airflow/output /opt/airflow/intermediate /opt/airflow/src && \
    chown -R airflow /opt/airflow/logs /opt/airflow/dags /opt/airflow/input /opt/airflow/output /opt/airflow/intermediate /opt/airflow/src && \
    chmod -R 755 /opt/airflow/logs /opt/airflow/dags /opt/airflow/input /opt/airflow/output /opt/airflow/intermediate /opt/airflow/src

COPY ./input /opt/airflow/input
 
USER airflow  

RUN pip install --upgrade pip  
RUN pip install apache-airflow==2.7.1 apache-airflow-providers-apache-spark==3.0.0 pyspark  
