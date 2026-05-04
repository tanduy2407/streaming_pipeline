FROM flink:1.18

# Install Python
RUN apt-get update && \
    apt-get install -y python3 python3-pip curl && \
    ln -s /usr/bin/python3 /usr/bin/python

# Install PyFlink
RUN pip3 install apache-flink

COPY main/job/*.py /opt/flink/app/
COPY entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh
RUN wget -P /opt/flink/lib https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-kafka/3.2.0-1.18/flink-connector-kafka-3.2.0-1.18.jar
RUN wget -P /opt/flink/lib https://repo.maven.apache.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar

ENV PYTHONPATH="/opt/flink/app"

CMD ["/entrypoint.sh"]