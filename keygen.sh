#! /bin/sh

ssh-keygen -o -f github.key \
    -C "generated $(date --iso-8601) for pushes to GitHub from jobs running on Travis CI" \
    "$@"
