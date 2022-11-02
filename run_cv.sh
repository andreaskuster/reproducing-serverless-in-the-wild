for p in 0 2 5 10
do
    python simulate.py --CV ${p} --PW 5 --KA 99 --method hybrid --dir_name "debug/hybrid_5_99_CV${p}_KA5" --max_time 200 --RANGE_OF_HISTOGRAM 240 --keep_alive_period 5
done