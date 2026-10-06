#!/usr/bin/env bash
# Genera el par de llaves RSA para Snowpipe Streaming e imprime la llave
# pública en una sola línea, lista para pegar en 00_setup.sql (ALTER USER).
set -euo pipefail
cd "$(dirname "$0")"
umask 077   # la llave privada nace legible solo para ti
if [ ! -f rsa_key.p8 ]; then
  openssl genrsa 2048 2>/dev/null | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
  openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub 2>/dev/null
fi
echo "Pega esto en 00_setup.sql, dentro de RSA_PUBLIC_KEY = '...':"
echo
grep -v -- '-----' rsa_key.pub | tr -d '\n'; echo
