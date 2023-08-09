#!/bin/sh

# This script tests all of lightbeam's functionality against an Ed-Fi API
# The default API configuration specificed in `lightbeam.yml` should work 
# if you first spin up a local Ed-Fi API via Docker - see
# - https://techdocs.ed-fi.org/display/EDFITOOLS/Docker+Deployment
# - https://github.com/Ed-Fi-Alliance-OSS/Ed-Fi-ODS-Docker
# and run something like
# $ docker-compose -f ./compose/pgsql/compose-sandbox-env.yml --env-file .\.env up -d
# to stand up a local Ed-Fi API.

echo "counting elements in Ed-Fi API..."
lightbeam count -e *Descriptors
echo "  ... done!"

echo "validating sample data..."
lightbeam validate -f
echo "  ... done!"

echo "sending sample data..."
lightbeam send -f
echo "  ... done!"

echo "counting schools and LEAs in Ed-Fi API..."
lightbeam count -s localEducationAgencies,schools -e *Descriptors
echo "  ... done!"

echo "deleting sent data"
lightbeam delete -f
echo "  ... done!"

echo "counting schools and LEAs in Ed-Fi API..."
lightbeam count -s localEducationAgencies,schools -e *Descriptors
echo "  ... done!"

echo "all finished, goodbye"