#!/bin/bash

for dir in `ls . | grep host`;
do
    $dir/bin/stop.sh
    rm -rf $dir/tasks/*
    rm -rf $dir/cache/*
done
