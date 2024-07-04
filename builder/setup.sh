#!/bin/bash

# NOTE: This script is not ran by default for the template docker image.
#       If you use a custom base image you can add your required system dependencies here.

set -e # Stop script on error
apt-get update && apt-get install -y poppler-utils
