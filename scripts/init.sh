#!/bin/sh

POLARIS_URL="http://polaris:8181"

echo "Requesting access token..."

TOKEN_RESPONSE=$(curl -s -X POST "$POLARIS_URL/api/catalog/v1/oauth/tokens" -H "Authorization: Basic cm9vdDpzZWNyZXQ=" -H "Content-Type: application/x-www-form-urlencoded" --data "grant_type=client_credentials&scope=PRINCIPAL_ROLE:ALL")

echo "Token response:"
echo "$TOKEN_RESPONSE"

ACCESS_TOKEN=$(echo "$TOKEN_RESPONSE" | sed -n 's/.*"access_token":"\([^"]*\)".*/\1/p')

echo "Extracted ACCESS_TOKEN:"
echo "$ACCESS_TOKEN"

echo "Creating catalog..."

curl -X POST "$POLARIS_URL/api/management/v1/catalogs" -H "Authorization: Bearer $ACCESS_TOKEN" -H "Content-Type: application/json" --data '{"name":"polariscatalog","type":"INTERNAL","properties":{"default-base-location":"s3://warehouse","s3.endpoint":"http://minio:9000","s3.path-style-access":"true","s3.access-key-id":"admin","s3.secret-access-key":"password","s3.region":"dummy-region"},"storageConfigInfo":{"roleArn":"arn:aws:iam::000000000000:role/minio-polaris-role","storageType":"S3","allowedLocations":["s3://warehouse/*"]}}'

echo "Listing catalogs..."

curl -X GET "$POLARIS_URL/api/management/v1/catalogs" -H "Authorization: Bearer $ACCESS_TOKEN"

echo "Creating catalog_admin role..."

curl -X PUT "$POLARIS_URL/api/management/v1/catalogs/polariscatalog/catalog-roles/catalog_admin/grants" -H "Authorization: Bearer $ACCESS_TOKEN" -H "Content-Type: application/json" --data '{"grant":{"type":"catalog","privilege":"CATALOG_MANAGE_CONTENT"}}'

echo "Creating data_engineer role..."

curl -X POST "$POLARIS_URL/api/management/v1/principal-roles" -H "Authorization: Bearer $ACCESS_TOKEN" -H "Content-Type: application/json" --data '{"principalRole":{"name":"data_engineer"}}'

echo "Connecting roles..."

curl -X PUT "$POLARIS_URL/api/management/v1/principal-roles/data_engineer/catalog-roles/polariscatalog" -H "Authorization: Bearer $ACCESS_TOKEN" -H "Content-Type: application/json" --data '{"catalogRole":{"name":"catalog_admin"}}'

echo "Assigning role to root..."

curl -X PUT "$POLARIS_URL/api/management/v1/principals/root/principal-roles" -H "Authorization: Bearer $ACCESS_TOKEN" -H "Content-Type: application/json" --data '{"principalRole":{"name":"data_engineer"}}'

echo "Polaris initialization finished."
