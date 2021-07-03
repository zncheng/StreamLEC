# Overview
+ StreamLEC is a stream processing system that specifically designed for stream machine learning applications and provides lightweight proactive fault tolerance via erasure coding.
This source code showcases the prototype of StreamLEC and its usage.

# Publication
+ Zhinan Cheng, Lu Tang, Qun Huang, and Patrick P. C. Lee.
 [Enabling Low-Redundancy Proactive Fault Tolerance for Stream Machine Learning via Erasure Coding](http://www.cse.cuhk.edu.hk/~pclee/www/pubs/srds21streamlec.pdf)
 Proceedings of the 40th International Symposium on Reliable Distributed Systems (SRDS 2021), September 2021.



# Running
### Requirements
+ gcc and camke (version 2.6.4 or above), we have tested StreamLEC in Ubuntu 16.04 with gcc 5.4.0 and cmake 3.5.1.
+ CPU and compiler support SIMD instruction AVX and AVX2.
    + check the CPU flag avx and avx2 via `cat /proc/cpuinfo`.
+ StreamLEC also relies on ZMQ and iniparser (with source code built-in).
### Compiles and runs
+ Step 1: Build StreamLEC core library.
    + Create a directory in project root directory (e.g., release), create Makefile, and build the code
    ``` 
    mkdir release && cd release
    cmake ..
    make -j
    ```
    + Set up the library.
    ```
    sudo cp libstreamlec.so /usr/lib
    cd ..
    ```
+ Step 2: Compile example application.
    + Provided applications
        + Logistic regression training, without enabling hybrid coded computation
        + Logistic regression prediction, enabling hybrid coded computation
    + Here we take the streaming logistic regression training as example (check `./apps/logistic_regression/`).
        + We train a logistic regression model in real time using the provided sample input
            + use default encoder in source.
            + use default decoder and implement `Recompute` and `Aggregate` in the sink.
            + implement the `ProcessData` interfaces in each processors
    + For compile
        + use the provided Makefile.
        ```
        cd apps/logistic_regression
        make -j
        ```
+ Step 3: Run example application 
    + Configurations
        + one Source, one Sink, and three processors
        + check the configuration file in `./apps/logistic_regression/sample.ini`.
            + k = 2, r = 1, micro_batch size = 300.
    + Input trace
        + check `./apps/trace/sample.txt` for the example trace.
            + 10000 items, each with 12 attributes.
    + Run pseudo distributed mode using a single machine
        + open one terminal, run the Source.
        ```
        ./source sample.ini Encoder
        ```      
        + run 3 processors, each on a new terminal.
        ```
        ./processor sample.ini Processor1
        ./processor sample.ini Processor2
        ./processor sample.ini Processor3
        ```
        + open another terminal, run the sink
        ```
        ./sink sample.ini Decoder
        ```      
        + check the output in each terminal to see the progress
    + Immediately failure recovery
        + You can kill any *r* processors (e.g., 1 in this case) during their running to verify the immediate recovery
    + run in distributed mode
        + Set up the core library on each node
            + copy the core library `libstreamlec.so` to the `/usr/lib` directory of each node
        + change the configuration file
            + update the ip address of Encoder, Decoder, and each Processor
        + check the script in `./script` to run StreamLEC
            + fill in the ip address of the cluster
            + run `./setup.sh` to setup the preparation
            + run `./run_all.sh` to run an application
                + check the arguments for the script
            + run `./stop_all.sh` to stop an application

### Programming model
+ users can check the provided interfaces and templates for building application in directory `./programming`. 
+ Source
    + StreamLEC provides a default decoder based on RS code in the source (check `/source_interfaces.hpp`), users have no need to modify the source code if use RS codes.
    + StreamLEC also allows users to deploy other erasure codes by re-implementing the `Encode()` interfaces in `source_interface.hpp`).
+ Sink
    + StreamLEC also provides a default decoder based on RS code in the sink (check `sink_interface.hpp`), users only need to implement the `Recompute` and `Aggregate` interfaces (check `.sink_interface.hpp`) for an applications.
    + Users also needs to re-implementing `Decode()` interfaces if they want to uses a new erasure codes (check `sink_interface.hpp`).
+ Processor
    + For processor, users only need to fill in the required interfaces, check `processor_interface.hpp`.
        + if enable coded computation, users need to decouple the computation into two parts and fill in the `ProcessLinear` and `ProcessNonlinear`
        + otherwise only need to fill in the `ProcessData` interface
        + fill in the `ProcessFeedback` interfaces if there are feedbacks in the applications

### Contact
+ Please contact zncheng@cse.cuhk.edu.hk if you have any problems.
