# immersadb - embedded tree database

ImmersaDB is an embedded database written in Go.

## Features

* Transactional (ACID)
* Append-Only using persistent data structures
* Blazing fast: 3-4K transactions per second on an SSD
* Memory-Mapped - memory paging used to cache reads, leveraging the whole available ram without needing to allocate it
* Constant resident memory requirements for both reading and modifying data, independent on data size
* Stores arbitrary deep tree of hashes and arrays as nodes and Data as leaves
* Event model for listeners of changes for sub-trees
* Embedded Golang library - no need for a server in a separate process

## Use cases

ImmersaDB is good when there is a single writer and many consumers.


## Usage

```go

db := immersadb.New("./mydb", 4 * 1024)

```
