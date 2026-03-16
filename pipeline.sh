#!/bin/bash
#
# Run as postgres user, e.g.:
#
#     sudo -u postgres ./pipeline.sh full
#
# Errors are handled:
#   - in psql by the ON_ERROR_STOP setting
#   - in SQL by using a RAISE statement in stored procedures when validation SELECT values aren't as expected (TODO)
#   - in bash by checking $? after each psql call in `check_error()`
#     (rather than `set -e`, which wouldn't allow subsequent actions such as notifying me)
#     (maybe a calling script could use `set -e`, such as one in a GitHub action script)
#

set -o pipefail

[ "$(whoami)" != "postgres" ] && \
    echo -ne "$0 must be run as user 'postgres', ie.\n  sudo -u postgres $0 ...\n" 1>&2 && \
    exit 1

logroot=/tmp/sql-data-warehouse
mkdir -p $logroot
tstamp=$(date +%Y-%m-%d_%H%M%S)

erron='-v ON_ERROR_STOP=ON'

check_error ()
{
    rc=$?
    if [ $rc -ne 0 ]; then
        echo ''
        echo ---------------------------------------------------
        echo ERROR. RC=$rc
        echo ---------------------------------------------------
        exit 1
    else
        echo ''
        echo ---------------------------------------------------
        echo Done.
        echo ---------------------------------------------------
    fi
}

psql_task ()
{
    psql -v schema=$2 -v ON_ERROR_STOP=on -f $1 2>&1 | tee -a "${logroot}/${tstamp}.log";
    check_error
}

init_db ()
{
    psql_task ./src/init_database.sql
}

bronze_create ()
{
    psql_task ./src/bronze/ddl_bronze.sql
}

bronze_load ()
{
    psql_task ./src/bronze/load_bronze.sql
}

silver_create ()
{
    psql_task ./src/silver/ddl_silver.sql
}

silver_load ()
{
    psql_task ./src/silver/load_silver.sql
}

silver_validate ()
{
    psql_task ./src/silver/validate_silver.sql
}

gold_create ()
{
    psql_task ./src/gold/ddl_gold.sql
}

gold_validate ()
{
    psql_task ./src/gold/validate_gold.sql
}

SECONDS=0
# tag::options[]
case $1 in
    "init")
        init_db
        ;;
    "bronze-create")
        bronze_create
        ;;
    "bronze-load")
        bronze_load
        ;;
    "bronze-all")
        bronze_create
        bronze_load
        ;;
    "silver-create")
        silver_create
        ;;
    "silver-load")
        silver_load
        ;;
    "silver-validate")
        silver_validate
        ;;
    "silver-all")
        silver_create
        silver_load
        silver_validate
        ;;
    "gold-create")
        gold_create
        ;;
    "gold-validate")
        gold_validate
        ;;
    "gold-all")
        gold_create
        gold_validate
        ;;
    "full")
        init_db
        bronze_create
        bronze_load
        silver_create
        silver_load
        silver_validate
        gold_create
        gold_validate
        ;;
    *)  echo -ne "\nUsage: $0 {init\n  |bronze-create|bronze-load|bronze-all\n  |silver-create|silver-load|silver-validate|silver-all\n  |gold-create|gold-validate|gold-all|full}\n"
esac
# end::options[]

echo -ne "\n=================================\nElapsed time: $SECONDS seconds\n=================================\n" | tee -a "${logroot}/${tstamp}.log"

