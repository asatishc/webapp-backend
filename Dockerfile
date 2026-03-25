# Stage 1: Build
FROM maven:3.9.6-eclipse-temurin-21 AS build
WORKDIR /app

# Copy only the pom and wrapper first (better for caching)
COPY mvnw .
COPY .mvn .mvn
COPY pom.xml .

# Grant permission
RUN chmod +x mvnw

# Copy the rest of the source
COPY src src

# Build
RUN ./mvnw clean package -DskipTests

#COPY . .
#RUN chmod +x mvnw && ./mvnw clean package -DskipTests

# Stage 2: Run
FROM eclipse-temurin:21-jre-jammy
WORKDIR /app
COPY --from=build /app/target/*.jar app.jar
ENV TZ="Asia/Kolkata"
EXPOSE 8080
ENTRYPOINT ["java", "-Duser.timezone=${TZ}", "-jar", "app.jar"]

