### MapReduce     

Experimental implemenation of map reduce programming model. 
It is some sort of `hello world` program of distbuted systems.
And ofc, I haven't looked under the network security and 
authentication part of real RPC call so this is straight 
impl from the paper, to wrap the concept on the head. 

- [x] Local Master Server accepts connection workers.   
- [x] Keep tracks of the workers with some sort of buffer(Hashmap).
- [ ] Implement some kind of queue, so workers picks task when they are free.
