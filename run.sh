#!/bin/bash

for p in 1 5 10
do
    for q in 95 99
    do
        python simulate.py --PW ${p} --KA ${q} --method hybrid --dir_name "final/hybrid_${p}_${q}_ka20" --max_time 200 --RANGE_OF_HISTOGRAM 240 --keep_alive_period 20
    done
done