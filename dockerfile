FROM apache/airflow:2.10.4

USER root

RUN apt-get update && \
    APT_KEY_DONT_WARN_ON_DANGEROUS_USAGE=1 ACCEPT_EULA=Y apt-get install -y msodbcsql17 && \
    apt-get install -y --no-install-recommends \
       gcc \
       heimdal-dev && \
    apt-get autoremove -yqq --purge && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir\
    pyodbc \
    psycopg2-binary \
    apache-airflow-providers-microsoft-mssql \
    apache-airflow-providers-postgres \
    apache-airflow-providers-apache-hdfs \
    pandas \
    sharepy \
    Office365-REST-Python-Client \
    certifi