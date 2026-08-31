#!/bin/bash

{
echo "HSET user:1 name Alice"
echo "HSET user:1 age 18"
echo "HGET user:1 name"
echo "HGET user:1 age"

echo "RSET score:1 100"
echo "RSET score:2 90"
echo "RGET score:1"

echo "HSET order:1001 price 99"
echo "HSET order:1001 status paid"
echo "HGET order:1001 price"

echo "MEMSTAT"
} | nc 127.0.0.1 5000
