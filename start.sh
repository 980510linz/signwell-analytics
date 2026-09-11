#!/bin/sh
export PORT="${PORT:-8787}"
export ALLOWED_ORIGINS="${ALLOWED_ORIGINS:-https://980510linz.github.io}"
exec node server.js
