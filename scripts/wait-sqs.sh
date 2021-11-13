#!/bin/sh
echo "Waiting for SQS at address http://localhost:4566/health, attempting every 5s"
until $(curl --silent --fail http://localhost:4566/health | grep "\"sqs\": \"running\"" > /dev/null); do
    printf '.'
    sleep 5
done
echo ' Success: Reached SQS'