# immersadb - embedded tree database

ImmersaDB is an embedded database written in Go.

## Features

* Transactional
* Like redis but with a tree
* Pub/Sub
* Primitive types: Data, Hashes and Lists

## Use cases

ImmersaDB is good when there is a single writer and many consumers.


## Usage

```go

db := immersadb.New("./mydb", 4 * 1024)

```

## License

...
