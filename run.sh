#!/bin/bash

# python simulate.py --max_time 60 --dir_name hybrid_1h_result --method hybrid

# python simulate.py --max_time 120 --dir_name keep_alive_2h_result --method keep_alive

# for p in 1 5 10 15 25 50 75

# p=10
# python simulate.py -p ${p} --dir_name "hybrid_${p}_result_solve" --method hybrid --max_time 100
# python simulate.py -p ${p} --dir_name "keep_alive_${p}_result_solve" --method keep_alive --max_time 100

# for p in 2
# do
#   python simulate.py -CV ${p} --dir_name "hybrid_CV_${p}_downsample" --method hybrid --max_time 200
# done

p=2
python simulate.py -CV ${p} --dir_name "hybrid_CV_${p}_downsample" --method hybrid --max_time 200
# python simulate.py --dir_name "keep_alive_10_downsample" --method keep_alive --max_time 200