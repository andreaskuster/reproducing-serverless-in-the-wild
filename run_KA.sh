for p in 0 5
do
    python simulate.py --PW ${p} --dir_name "final/hybrid_${p}_100" --method hybrid --max_time 200 --KA 100.0
done