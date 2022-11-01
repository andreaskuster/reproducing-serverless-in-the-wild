for p in 40 80 120 160 200
do
    python simulate.py --RANGE_OF_HISTOGRAM ${p} --PW 5 --KA 99.0 --method hybrid --dir_name "hybrid_5_99_range${p}" --max_time 200 
done