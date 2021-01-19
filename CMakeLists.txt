cmake_minimum_required (VERSION 2.6.4)

project (StreamLEC)

#---------------------- shared library ----------------------

### add ZeroMQ library
    include_directories(third_party/zeromq/include)
    file(GLOB_RECURSE ZMQ_SRC third_party/zeromq/src/*.cpp)
    add_library(zmq OBJECT ${ZMQ_SRC})
    #target_link_libraries(zmq pthread rt)

### add ZooKeeper C Bindings
    include_directories(third_party/zookeeper-3.4.7 third_party/zookeeper-3.4.7/include third_party/zookeeper-3.4.7/generated)
    file(GLOB_RECURSE ZOOKEEPER_MT_SRC
        third_party/zookeeper-3.4.7/generated/*.c
        third_party/zookeeper-3.4.7/src/mt_adaptor.c
        third_party/zookeeper-3.4.7/src/zk_hashtable.c
        third_party/zookeeper-3.4.7/src/zk_log.c
        third_party/zookeeper-3.4.7/src/recordio.c
        third_party/zookeeper-3.4.7/src/hashtable/*.c
        third_party/zookeeper-3.4.7/src/zookeeper.c)
    add_library(zkmt OBJECT ${ZOOKEEPER_MT_SRC})
    #target_link_libraries(zkmt m rt pthread)

### add iniParser library
    include_directories(third_party/iniparser/src)
    file(GLOB_RECURSE INIPARSER_SRC third_party/iniparser/src/*.c)
    add_library(iniparser OBJECT ${INIPARSER_SRC})

# add ZeroMQ mailbox library
    include_directories(third_party/zeromq/include)
    file(GLOB_RECURSE ZMQ_MAILBOX_SRC src/control_channel/*.cpp)
    add_library(zmq_mailbox OBJECT ${ZMQ_MAILBOX_SRC})
    #target_link_libraries(zmq_mailbox rt)

#--------------------------- StreamLEC Core---------------------------

include_directories(src)
include_directories(src/control_channel)

add_library(streamlec SHARED
    src/config.cpp
    src/zmq_err.cpp
    src/thread/thread.cpp
    src/thread/adapter_base.cpp
    src/thread/thread_id.cpp
    src/controller/ZkUtil.cpp
    src/controller/zk_worker_client.cpp
    src/net/epoll.cpp
    src/net/poller_base.cpp
    src/net/clock.cpp
    src/net/options.cpp
    src/net/tcp_listener.cpp
    src/net/tcp_connecter.cpp
    src/net/random.cpp
    src/net/address.cpp
    src/net/tcp_address.cpp
    src/net/tcp.cpp
    src/net/io_object.cpp
    src/net/stream_engine.cpp
    src/net/msg.cpp
    src/net/v2_encoder.cpp
    src/net/v2_decoder.cpp
    src/net/null_mechanism.cpp
    src/net/mechanism.cpp
    ${ZOOKEEPER_MT_SRC}
    ${INIPARSER_SRC}
    $<TARGET_OBJECTS:zmq>
    $<TARGET_OBJECTS:zmq_mailbox>
    )
target_link_libraries(streamlec pthread rt)

#------------ Encoder/Decoder ---------------
#
#add_executable(Encoder
#    src/coding/Encoder.cpp
#    )
#
#add_executable(Decoder
#    src/coding/Decoder.cpp
#    )
#target_link_libraries(Encoder codestream)
#target_link_libraries(Decoder codestream)

#--------------------------- Flags --------------------------

## to compile multi-threaded ZooKeeper C Bindings
ADD_DEFINITIONS("-DTHREADED")

## to print PRIu64
ADD_DEFINITIONS("-D__STDC_FORMAT_MACROS")

set(CMAKE_BUILD_TYPE Release)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -mavx2 -mavx -mfma -fPIC")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -Wall -g -DDEBUG -mavx2 -mavx -mfma")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -Wall -mavx2 -mavx -mfma -g -O3 -Wl,-no-as-needed")
