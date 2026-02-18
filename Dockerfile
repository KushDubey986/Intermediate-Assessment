FROM jupyter/pyspark-notebook:latest

USER root

RUN wget https://jdbc.postgresql.org/download/postgresql-42.6.0.jar -P /usr/local/spark/jars/

WORKDIR /home/jovyan/work
