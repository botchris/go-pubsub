#!/bin/sh
echo "Waiting for KubeMQ at address http://localhost:8080/ready, attempting every 5s"
until $(curl --silent --fail http://localhost:8080/ready | grep "\"is_ready\":true" > /dev/null); do
    printf '.'
    sleep 5
done
echo ' Success: Reached KubeMQ'