# Reproducing-serverless-in-the-wild

# Install
```
pip install -r requirements.txt
```

# Assumptions
- Function invocation duration: take Average (we could extend with Gaussian distribution)
- Function memory consumption: use AverageAllocatedMb (we could extend with Gaussian distribution)
- A function execution does not require additional memory (e.g. exection of the same function twice on the same compute node uses only 1x the memory allocated to the function)



# Usage
Directly run it by following:
```shell
python simulate.py

usage: simulate.py [-h] [--day_index DAY_INDEX] [--max_time MAX_TIME] [--num_nodes NUM_NODES] [--node_mem_mb NODE_MEM_MB] [--method {keep_alive,hybrid,reinfored}]
                   [--fast_read FAST_READ] [--dir_name DIR_NAME]

optional arguments:
  -h, --help            show this help message and exit
  --day_index DAY_INDEX
                        Data day index, subset of [1, .. , 12], default: day1
  --max_time MAX_TIME   Time period for simuation, maximum = 1440
  --num_nodes NUM_NODES
                        Number of compute nodes, default: 1
  --node_mem_mb NODE_MEM_MB
                        Memory capacity per node, default: 2T
  --method {keep_alive,hybrid,reinfored}
                        Controller stragety for pre-warming window and keep-alive window
  --fast_read FAST_READ
                        read data saved in 'app_xxx'
  --dir_name DIR_NAME   dir to save result
```
or,
```
bash run.sh
```


# Dataset

https://github.com/Azure/AzurePublicDataset

```
Mohammad Shahrad, Rodrigo Fonseca, Inigo Goiri, Gohar Chaudhry, Paul Batum, Jason Cooke, Eduardo Laureano, Colby Tresness, Mark Russinovich, Ricardo Bianchini. "Serverless in the Wild: Characterizing and Optimizing the Serverless Workload at a Large Cloud Provider", in Proceedings of the 2020 USENIX Annual Technical Conference (USENIX ATC 20). USENIX Association, Boston, MA, July 2020.
```