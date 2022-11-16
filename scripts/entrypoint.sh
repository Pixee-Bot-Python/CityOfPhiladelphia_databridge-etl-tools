#!/usr/bin/env bash

# This is needed to use both an entrypoint and a command with Docker
exec "${@}"
