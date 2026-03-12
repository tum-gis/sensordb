# Builder stage 1 #############################################################
FROM alpine/git AS first-builder

WORKDIR /home
RUN git clone --branch v5.1.0 --depth 1 https://github.com/3dcitydb/3dcitydb.git

COPY . /home
RUN dos2unix /home/docker-scripts/sensordb-initdb.sh

# Builder stage 2 #############################################################
FROM eclipse-temurin:17-jdk-jammy AS second-builder

WORKDIR /home
COPY --from=first-builder /home/3dcitydb /home/3dcitydb

RUN (cd /home/3dcitydb && chmod u+x ./gradlew && ./gradlew installDist)


# Runtime stage ###############################################################
FROM postgis/postgis:17-3.5 AS runtime

RUN mkdir -p /docker-entrypoint-initdb.d

# see: https://github.com/3dcitydb/3dcitydb/blob/master/postgresql/Dockerfile
ENV POSTGIS_SFCGAL=true

# Copy SQL scripts
WORKDIR /app
COPY --from=first-builder /home/docker-scripts/sensordb-initdb.sh /docker-entrypoint-initdb.d/11_sensordb.sh
COPY --from=first-builder /home/sql-scripts ./sensordb

COPY --from=second-builder /home/3dcitydb/build/install/3dcitydb/version.txt .
COPY --from=second-builder /home/3dcitydb/build/install/3dcitydb/postgresql/sql-scripts .
COPY --from=second-builder /home/3dcitydb/build/install/3dcitydb/postgresql/docker-scripts/3dcitydb-initdb.sh /docker-entrypoint-initdb.d/12_citydb.sh

# Make init script executable
RUN chmod +x /docker-entrypoint-initdb.d/11_sensordb.sh
RUN chmod +x /docker-entrypoint-initdb.d/12_citydb.sh

# Set labels
LABEL maintainer="Benedikt Schwab"
LABEL maintainer.email="benedikt.schwab(at)tum.de"
LABEL maintainer.organization="Chair of Geoinformatics, Technical University of Munich (TUM)"
LABEL source.repo="https://github.com/tum-gis/sensordb"
