#!/usr/bin/env bash

# Print commands and their arguments as they are executed
set -e;
# psql should stop on error
psql=( psql -v ON_ERROR_STOP=1 )

# create extensions
"${psql[@]}" -d "$POSTGRES_DB" -c "CREATE EXTENSION IF NOT EXISTS postgis;"
"${psql[@]}" -d "$POSTGRES_DB" -c "CREATE EXTENSION IF NOT EXISTS postgis_sfcgal;"
"${psql[@]}" -d "$POSTGRES_DB" -c "CREATE EXTENSION IF NOT EXISTS postgis_topology;"
"${psql[@]}" -d "$POSTGRES_DB" -c "CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;"
"${psql[@]}" -d "$POSTGRES_DB" -c "CREATE EXTENSION IF NOT EXISTS postgis_tiger_geocoder;"


# Setup 3DSensorDB schema -------------------------------------------------------
if [ "${SETUP_SENSORDB_SCHEMA}" != "false" ]; then
  echo
  echo "Setting up 3DSensorDB database schema in database '$POSTGRES_DB' ..."

  "${psql[@]}" -d "$POSTGRES_DB" -f "sensordb/create-db.sql" \
    -v srid="$SRID" -v srs_name="$SRS_NAME" -v changelog="$CHANGELOG" > /dev/null

  echo "Setting up 3DSensorDB database schema in database '$POSTGRES_DB' ...done!"
fi

# Echo info -------------------------------------------------------------------
cat <<EOF

###############################################################################
#    ____  _____   _____                           _____  ____
#   |___ \|  __ \ / ____|                         |  __ \|  _ \\
#     __) | |  | | (___   ___ _ __  ___  ___  _ __| |  | | |_) |
#    |__ <| |  | |\___ \ / _ \ '_ \/ __|/ _ \| '__| |  | |  _ <
#    ___) | |__| |____) |  __/ | | \__ \ (_) | |  | |__| | |_) |
#   |____/|_____/|_____/ \___|_| |_|___/\___/|_|  |_____/|____/
#
# 3DSensorDB Docker PostGIS
#
# PostgreSQL/PostGIS ----------------------------------------------------------
#   PostgreSQL version  $PG_MAJOR - $PG_VERSION
#   PostGIS version     $POSTGIS_VERSION
#
# 3DCityDB --------------------------------------------------------------------
#   3DCityDB version    $CITYDB_VERSION
#   DBNAME              $POSTGRES_DB
#   SRID                $SRID
#   SRSNAME             $SRS_NAME
#   HEIGHT_EPSG         $HEIGHT_EPSG
#   SFCGAL enabled      $SFCGAL
#   CHANGELOG enabled   $CHANGELOG
#
#   https://github.com/tum-gis/sensordb
#
# Maintainer ------------------------------------------------------------------
#   Benedikt Schwab
#   TUM Chair of Geoinformatics
#   benedikt.schwab(at)tum.de
#
###############################################################################

EOF