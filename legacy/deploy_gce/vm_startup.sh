#!/usr/bin/env bash
# Runs once as root when the VM first boots. Installs Docker and adds swap
# so Airflow does not run out of memory.
set -e
if ! command -v docker >/dev/null; then
  apt-get update
  apt-get install -y ca-certificates curl git
  curl -fsSL https://get.docker.com | sh
fi
if [ ! -f /swapfile ]; then
  fallocate -l 2G /swapfile
  chmod 600 /swapfile
  mkswap /swapfile
  swapon /swapfile
  echo '/swapfile none swap sw 0 0' >> /etc/fstab
fi
