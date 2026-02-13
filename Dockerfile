FROM maven:3.9.11-sapmachine-21 as build

WORKDIR /build
COPY pom.xml .
RUN mvn dependency:go-offline -B
COPY src ./src
RUN mvn package -DskipTests

FROM eclipse-temurin:11-jre-jammy
WORKDIR /app
COPY --from=build /build/target/*.jar /app/etl.jar
EXPOSE 8080
CMD ["java", "-jar", "etl.jar"]