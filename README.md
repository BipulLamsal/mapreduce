### MapReduce     

Experimental implemenation of map reduce programming model. It is some sort of `hello world` program of distbuted systems. And ofc, I haven't looked under the network security and authentication part of real RPC call so this is straight impl from the paper, to wrap the concept on the head. 

- [x] Local Master Server accepts connection workers.   
- [x] Keep tracks of the workers with some sort of buffer(Hashmap).
- [x] User framework defination of map
- [x] Framework sends map function and file path
- [x] FIFO scheduler 
- [x] Master to file divide into chunks metadata
- [x] Implement some kind of queue, so workers picks task when they are free.
- [x] Worker to signal periodically their state 
- [x] Map Worker produces intermediate k/v pairs
- [x] e2e test for client master and map worker 
- [x] chunkstate for bookkeeping on the master   
- [x] worker assign unique id for job its proccesing
- [ ] Worker should create R files acting as paritions
- [ ] Once all workers are done, then only reduce operations needs to be performed
- [ ] R Reduce Worker are only required, each picking task from each parition of worker

