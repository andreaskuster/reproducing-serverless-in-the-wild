# reproducing-serverless-in-the-wild



# Install

```
source setup_env.sh
```

# Assumptions

- Function invocation duration: take Average (we could extend with Gaussian distribution)
- Function memory consumption: use AverageAllocatedMb (we could extend with Gaussian distribution)
- A function execution does not require additional memory (e.g. exection of the same function twice on the same compute node uses only 1x the memory allocated to the function)


Q: What is the execution difference between cold start / warm start?
A: Maybe add a delay as a function of memory usage?



# Dataset

https://github.com/Azure/AzurePublicDataset

`Mohammad Shahrad, Rodrigo Fonseca, Inigo Goiri, Gohar Chaudhry, Paul Batum, Jason Cooke, Eduardo Laureano, Colby Tresness, Mark Russinovich, Ricardo Bianchini. "Serverless in the Wild: Characterizing and Optimizing the Serverless Workload at a Large Cloud Provider", in Proceedings of the 2020 USENIX Annual Technical Conference (USENIX ATC 20). USENIX Association, Boston, MA, July 2020.`