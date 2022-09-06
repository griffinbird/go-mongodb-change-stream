### Introduction
This repository provides a sample implementation of using MongoDB Change Streams feature in Golang which works in both Mongo Atlas and Cosmos DB Mongo API. 

When you use the Cosmos DB Mongo API each request will also surface the Request Charge and the time taken to complete the request.

### What is MongoDB Change Stream
Change streams allow applications to access real-time data changes without the complexity and risk of tailing the oplog. Applications can use change streams to subscribe to all data changes on a single collection, a database, or an entire deployment, and immediately react to them. Because change streams use the aggregation framework, applications can also filter for specific changes or transform the notifications at will [Read More](https://docs.mongodb.com/manual/changeStreams)

### Usage

```
# export MongoDB URI or update .env file.

git clone https://github.com/griffinbird/go-mongodb-change-stream.git
cd go-mongodb-change-stream
go mod tidy
go run main.go
```