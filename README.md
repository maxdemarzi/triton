# Codename Triton

In Memory Property Graph Server using Multicore Shared Nothing Sharding. 

The Triton server can host multiple Graphs. The graphs are accessible via a REST API (see below).
Each Graph is split into multiple Shards. One Shard per Core of the server. 
Shards communicate by explicit message passing. Nodes and Relationships have internal and external ids.
The external ids embed which Shard they belong to. 
The internal ids are pointers into vectors that hold the data of each Node and Relationship.
The Relationship ids are replicated to both incoming and outgoing Nodes.
The Relationship Object (and properties) belong to the Outgoing Node (and shard). 
Each Node must have a singular Type and unique Key on creation which the server stores in a HashMap for retrieval.
External and Internal Ids are assigned upon creation for both Nodes and Relationships.

Along side an HTTP API, Triton also has a Lua http endpoint that allows users to send complex queries.
These queries are interpreted by LuaJIT, compiled and executed within a Seastar Thread that allows blocking. 
By not having a "query language" we avoid parsing, query planning, query execution and a host of [problems](https://maxdemarzi.com/2020/05/25/declarative-query-languages-are-the-iraq-war-of-computer-science/). 

## HTTP API

### Nodes

#### Get All Nodes

    :GET /db/{graph}/nodes?limit=100&offset=0

#### Get All Nodes of a Type

    :GET /db/{graph}/nodes/{type}?limit=100&offset=0

#### Get A Node By Type and Key

    :GET /db/{graph}/node/{type}/{key}

#### Get A Node By Id

    :GET /db/{graph}/node/{id}

#### Create A Node

    :POST /db/{graph}/node/{type}/{key}
    JSON formatted Body: {properties}

#### Delete A Node By Type and Key

    :DELETE /db/{graph}/node/{type}/{key}

#### Delete A Node By Id

    :DELETE /db/{graph}/node/{id}

### Node Properties

#### Get the Properties of a Node By Type and Key

    :GET /db/{graph}/node/{type}/{key}/properties

#### Get the Properties of a Node By Id

    :GET /db/{graph}/node/{id}/properties

#### Reset the Properties of a Node By Type and Key

    :POST /db/{graph}/node/{type}/{key}/properties
    JSON formatted Body: {properties}

#### Reset the Properties of a Node By Id

    :POST /db/{graph}/node/{id}/properties
    JSON formatted Body: {properties}

#### Set some Properties of a Node By Type and Key

    :PUT /db/{graph}/node/{type}/{key}/properties
    JSON formatted Body: {properties}

#### Set some Properties of a Node By Id

    :PUT /db/{graph}/node/{id}/properties
    JSON formatted Body: {properties}

#### Delete the Properties of a Node By Type and Key

    :DELETE /db/{graph}/node/{type}/{key}/properties

#### Delete the Properties of a Node By Id

    :DELETE /db/{graph}/node/{id}/properties

#### Get a Property of a Node By Type and Key

    :GET /db/{graph}/node/{type}/{key}/property/{property}

#### Get a Property of a Node By Id

    :GET /db/{graph}/node/{id}/property/{property}

#### Create a Property of a Node By Type and Key

    :PUT /db/{graph}/node/{type}/{key}/property/{property}
    JSON formatted Body: {property}

#### Create a Property of a Node By Id

    :PUT /db/{graph}/node/{id}/property/{property}
    JSON formatted Body: {property}

#### Delete a Property of a Node By Type and Key

    :DELETE /db/{graph}/node/{type}/{key}/property/{property}

#### Delete a Property of a Node By Id

    :DELETE /db/{graph}/node/{id}/property/{property}

### Relationships

#### Get A Relationship

    :GET /db/{graph}/relationship/{id}

#### Create A Relationship By Node Types

    :POST /db/{graph}/node/{type_1}/{key_1}/relationship/{type_2}/{key_2}/{rel_type}
    JSON formatted Body: {properties}

#### Create A Relationship By Node Ids

    :POST /db/{graph}/node/{id_1}/relationship/{id_2}/{rel_type}
    JSON formatted Body: {properties}

#### Delete A Relationship

    :DELETE /db/{graph}/relationship/{id}

#### Get the Relationships of a Node By Node Type

    :GET /db/{graph}/node/{type}/{key}/relationships
    :GET /db/{graph}/node/{type}/{key}/relationships/{direction [all, in, out]} 
    :GET /db/{graph}/node/{type}/{key}/relationships/{direction [all, in, out]}/{type TYPE_ONE}
    :GET /db/{graph}/node/{type}/{key}/relationships/{direction [all, in, out]}/{type(s) TYPE_ONE&TYPE_TWO}

#### Get the Relationships of a Node By Node Id

    :GET /db/{graph}/node/{id}/relationships
    :GET /db/{graph}/node/{id}/relationships/{direction [all, in, out]} 
    :GET /db/{graph}/node/{id}/relationships/{direction [all, in, out]}/{type TYPE_ONE}
    :GET /db/{graph}/node/{id}/relationships/{direction [all, in, out]}/{type(s) TYPE_ONE&TYPE_TWO}

### Relationship Properties

#### Get the Properties of a Relationship

    :GET /db/{graph}/relationship/{id}/properties

#### Reset the Properties of a Relationship

    :POST /db/{graph}/relationship/{id}/properties
    JSON formatted Body: {properties}

#### Set some Properties of a Relationship

    :PUT /db/{graph}/relationship/{id}/properties
    JSON formatted Body: {properties}

#### Delete the Properties of a Relationship

    :DELETE /db/{graph}/relationship/{id}/properties

#### Get a Property of a Relationship

    :GET /db/{graph}/relationship/{id}/property/{property}

#### Create a Property of a Relationship

    :PUT /db/{graph}/relationship/{id}/property/{property}
    JSON formatted Body: {property}

#### Delete a Property of a Relationship

    :DELETE /db/{graph}/relationship/{id}/property/{property}

### Node Degrees

#### Get the Degree of a Node By Node Type

    :GET /db/{graph}/node/{type}/{key}/degree
    :GET /db/{graph}/node/{type}/{key}/degree/{direction [all, in, out]} 
    :GET /db/{graph}/node/{type}/{key}/degree/{direction [all, in, out]}/{type TYPE_ONE}
    :GET /db/{graph}/node/{type}/{key}/degree/{direction [all, in, out]}/{type(s) TYPE_ONE&TYPE_TWO}

#### Get the Degree of a Node By Node Id

    :GET /db/{graph}/node/{id}/degree
    :GET /db/{graph}/node/{id}/degree/{direction [all, in, out]} 
    :GET /db/{graph}/node/{id}/degree/{direction [all, in, out]}/{type TYPE_ONE}
    :GET /db/{graph}/node/{id}/degree/{direction [all, in, out]}/{type(s) TYPE_ONE&TYPE_TWO}

### Node Neighbors

#### Get the Neighbors of a Node By Node Type

    :GET /db/{graph}/node/{type}/{key}/neighbors
    :GET /db/{graph}/node/{type}/{key}/neighbors/{direction [all, in, out]} 
    :GET /db/{graph}/node/{type}/{key}/neighbors/{direction [all, in, out]}/{type TYPE_ONE}
    :GET /db/{graph}/node/{type}/{key}/neighbors/{direction [all, in, out]}/{type(s) TYPE_ONE&TYPE_TWO}

#### Get the Neighbors of a Node By Node Id

    :GET /db/{graph}/node/{id}/neighbors
    :GET /db/{graph}/node/{id}/neighbors/{direction [all, in, out]} 
    :GET /db/{graph}/node/{id}/neighbors/{direction [all, in, out]}/{type TYPE_ONE}
    :GET /db/{graph}/node/{id}/neighbors/{direction [all, in, out]}/{type(s) TYPE_ONE&TYPE_TWO}

### Lua

    :POST db/{graph}/lua
    STRING formatted Body: {script}

The script must end in one or more values that will be returned in JSON format inside an Array.
Within the script the user can access to graph functions. For example:

    -- Get some things about a node
    a = NodeGetId("Node","Max")
    b = NodeTypesGetCount()
    c = NodeTypesGetCountByType("Node")
    d = NodePropertyGet("Node", "Max", "name")
    e = NodePropertyGetById(a, "name")
    a, b, c, d, e

A second example:

    -- get the names of nodes I have relationships with
    names = {}
    ids = NodeGetRelationshipsIds("Node", "Max")
    for k=1,#ids do
        v = ids[k]
        table.insert(names, NodePropertyGetById(v.node_id, "name"))
    end
    names


## Installing

Start up an EC2 instance running Ubuntu 20.04. For higher performance reduce threads per core to 1.

Installation Notes:

    # Install some basics
    sudo apt-get update && apt-get install -y ccache build-essential git pkg-config gcc-10 g++-10

    # Switch to gcc 10
    sudo update-alternatives --config gcc

    # update cmake to a more recent version
    sudo apt purge --auto-remove cmake
    wget -O - https://apt.kitware.com/keys/kitware-archive-latest.asc 2>/dev/null | gpg --dearmor - | sudo tee /etc/apt/trusted.gpg.d/kitware.gpg >/dev/null
    sudo apt-add-repository 'deb https://apt.kitware.com/ubuntu/ focal main'
    sudo apt update
    sudo apt install cmake=3.17.3-0kitware1ubuntu20.04.1 cmake-data=3.17.3-0kitware1ubuntu20.04.1 

    # Install Seastar (this will take a while)
    git clone https://github.com/scylladb/seastar.git
    cd seastar
    git checkout seastar-20.05-branch
    sudo ./install_dependencies.sh
    ./configure.py --mode=release --prefix=/usr/local
    sudo ninja -C build/release install

    # Install luajit
    sudo apt-get install -y luajit-dev luajit

    # The Linux kernel provides the Asynchronous non-blocking I/O (AIO) feature that allows a process to initiate multiple I/O operations simultaneously without having to wait for any of them to complete. 
    # This helps boost performance for applications that are able to overlap processing and I/O.
     ~ sudo emacs /etc/sysctl.conf  
     ~ sudo sysctl -p           
    fs.aio-max-nr = 1048576
     ~ cat /proc/sys/fs/aio-max-nr
    1048576

    # Install Triton
    git clone https://github.com/maxdemarzi/triton.git
    cd triton
    cmake .
    make
    

## Running it

After installing it, go to the directory and run ./triton

Default Parameters:

    address             "0.0.0.0"       HTTP Server address
    port                10000           HTTP Server port
    prometheus_port     9180            Prometheus port. Set to zero in order to disable.
    prometheus_address  "0.0.0.0"       Prometheus address
    prometheus_prefix   "triton_httpd"  Prometheus metrics prefix

You should see something like:

    Running on 4 cores.
    starting prometheus API server
    Seastar HTTP server listening on 0.0.0.0:10000 ...

Prometheus Metrics are available on:

    http://localhost:9180/metrics

## Testing

TODO: Since moving Lua to the Shards, the Test project needs to get Sol and Lua added to it in order to compile.