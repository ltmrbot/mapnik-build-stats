#! /bin/sh

find "$@" -ipath '*/sources/[0-9a-f][0-9a-f]' -type d -prune ! -empty |
while read -r src
do
    dst=${src%?}
    mkdir -vp -- "$dst"
    mv -vt "$dst" -- "$src"/*
done
