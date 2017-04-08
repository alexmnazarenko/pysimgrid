#!/bin/bash

for dir in `ls . | grep host`;
do
    $dir/bin/start.sh
done
