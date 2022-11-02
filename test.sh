for p in 5 10 15 25 50 75
do
    python simulate.py --dir_name "keep_alive_${p}_200" --method keep_alive --max_time 200
done


for p in 60 120 180 240
do
    python simulate.py --dir_name "hybrid_range_${p}" --method hybrid --max_time 200
done
