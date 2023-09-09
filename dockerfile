# docker build -t assignment:v1 .
# docker run assignment:v1 driver python/Assignment.py

FROM datamechanics/spark:3.1-latest
ENV PYSPARK_MAJOR_PYTHON_VERSION=3
USER 0
RUN mkdir -p /Assignment/
RUN mkdir -p /Assignment/DataLake
RUN mkdir -p /Assignment/DataLake/raw
RUN mkdir -p /Assignment/DataLake/raw/searches
RUN mkdir -p /Assignment/DataLake/raw/visitors
RUN mkdir -p /Assignment/archive
RUN mkdir -p /Assignment/archive/searches
RUN mkdir -p /Assignment/archive/visitors
RUN mkdir -p /sparkCache
WORKDIR /Assignment/
COPY $PWD /Assignment/