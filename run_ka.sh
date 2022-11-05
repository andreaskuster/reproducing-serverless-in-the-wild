for p in 0 5 
do
    python simulate.py --PW ${p} --KA 100 --method hybrid --dir_name "hybrid_${p}_100" --max_time 200 --RANGE_OF_HISTOGRAM 240
done