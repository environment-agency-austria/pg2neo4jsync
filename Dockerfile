# Use a base image with a Java Runtime Environment (JRE)
FROM eclipse-temurin:21

ARG JAR_FILE

ENV PG_JDBC_URL not-set
ENV PG_USER not-set
ENV PG_PASSWORD not-set
ENV NEO4J_API_URL not-set
ENV NEO4J_AUTH not-set

WORKDIR /app

RUN mkdir /app/libs

COPY ${JAR_FILE} /app/pg2neo4jsync-shaded.jar
#COPY target/pg2neo4jsync-shaded.jar /app/pg2neo4jsync-shaded.jar

cmd java -jar pg2neo4jsync-shaded.jar "${PG_JDBC_URL}" "${PG_USER}" "${PG_PASSWORD}" "${NEO4J_API_URL}" "${NEO4J_AUTH}"

#ENTRYPOINT ["java", "-jar", "pg2neo4jsync-shaded.jar", "${PG_JDBC_URL}", "${PG_USER}", "${PG_PASSWORD}", "${NEO4J_API_URL}", "${NEO4J_AUTH}"]
