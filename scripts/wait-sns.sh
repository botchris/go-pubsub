#!/bin/sh
echo "Waiting for SNS at address http://localhost:4566/health, attempting every 5s"
until $(curl --silent --fail http://localhost:4566/health | grep "\"sns\": \"running\"" > /dev/null); do
    printf '.'
    sleep 5
done
echo ' Success: Reached SNS'