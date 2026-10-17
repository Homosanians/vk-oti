FROM apache/spark:3.5.9-scala2.12-java17-python3-ubuntu
USER root
ARG ICEBERG=1.10.2
RUN cd /opt/spark/jars \
 && curl -fsSLO https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG}/iceberg-spark-runtime-3.5_2.12-${ICEBERG}.jar \
 && curl -fsSLO https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG}/iceberg-aws-bundle-${ICEBERG}.jar
COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf
ENV PATH=$PATH:/opt/spark/bin AWS_REGION=us-east-1
WORKDIR /opt/jobs
