FROM apache/spark-py:v3.4.0

ENV SPARK_NO_DAEMONIZE=true

USER root

RUN mkdir -p /opt/spark/logs /opt/spark/work /opt/spark/.ivy2/ /opt/spark/data/  /opt/spark/code/

RUN chmod -R a+wr /opt/spark/

COPY ./entrypoint.sh /opt/spark/entrypoint.sh

RUN chmod +x /opt/spark/entrypoint.sh

ENTRYPOINT [ "tail", "-f", "/dev/null" ]