FROM zhangyuan/kafka

RUN wget https://api.hub.confluent.io/api/plugins/confluentinc/kafka-connect-jdbc/versions/4.1.1/archive -O kafka-connect-jdbc.zip
RUN mkdir -p /kafka/plugins && \
    unzip kafka-connect-jdbc.zip && \
    mv confluentinc-kafka-connect-jdbc-4.1.1 /kafka/plugins/ && \
    rm kafka-connect-jdbc.zip
COPY connect-standalone.properties jdbc-source.properties jdbc-sink.properties /kafka/config/
