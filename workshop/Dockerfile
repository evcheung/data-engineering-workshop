FROM zhangyuan/spark

WORKDIR /app

COPY target/scala-2.11/data-engineering-workshop-assembly-0.1.jar /app/app.jar
COPY docker-entrypoint.sh /app

RUN chmod 775 /app && chmod a+r /app/app.jar

ENTRYPOINT [ "/app/docker-entrypoint.sh" ]
