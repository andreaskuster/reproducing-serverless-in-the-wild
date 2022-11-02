#!/bin/bash

for p in 0 1 5 15 25 40
do
    python simulate.py --PW ${p} --KA 99.0 --method hybrid --dir_name "hybrid_${p}_99" --max_time 200 --RANGE_OF_HISTOGRAM 240
done

