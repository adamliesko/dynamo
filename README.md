# Dynamo #

DynamoDB clone as a assignment for DPRS@FIIT STU written by Barbora, Adam and Roman.

### How to start up the server ###

`make && ./_rel/dynamo_release/bin/dynamo_release console|start|stop|restart` - for more look at _rel/dynamo_release

### What is implemented ###

* consistent hashing
* NRW quorum
* Replica sync with merkle trees
* Gossip protocol
* Vector Clocks

### Resources ###

[Cowboy - HTTP server](https://github.com/ninenines/cowboy)

[Erlang.mk - build and test utility](https://github.com/ninenines/erlang.mk)
