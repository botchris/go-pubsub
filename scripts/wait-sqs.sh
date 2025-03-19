#!/bin/sh
echo "Waiting for SQS at address http://localhost:4566/_localstack/health, attempting every 5s"
curl http://localhost:4566/_localstack/health
until $(curl --silent --fail http://localhost:4566/_localstack/health | grep "\"sqs\": \"running\"" > /dev/null); do
    printf '.'
    sleep 5
done
echo ' Success: Reached SQS'