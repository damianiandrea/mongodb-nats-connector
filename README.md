# MongoDB-NATS Connector

![License](https://img.shields.io/github/license/damianiandrea/mongodb-nats-connector)
![CI](https://github.com/damianiandrea/mongodb-nats-connector/actions/workflows/ci.yml/badge.svg)
[![Go Report Card](https://goreportcard.com/badge/github.com/damianiandrea/mongodb-nats-connector)](https://goreportcard.com/report/github.com/damianiandrea/mongodb-nats-connector)

A connector that uses MongoDB's change streams to capture data changes and publishes those changes to NATS JetStream.

I wanted an easy way to do CDC (change data capture) with MongoDB and sink the data to NATS, but couldn't find any 
existing solution, so I decided to build my own.

## Quick Start

The quickest way to start is to clone the repository and use the following command to run a MongoDB replica set, 
a NATS cluster with JetStream enabled, and the connector itself:

```
make run
```

The connector will pick up the example `connector.yaml` configuration file, and it will perform the following actions:
* Create a MongoDB database named `test-connector`, and two MongoDB collections named `coll1` 
and `coll2`, if they do not already exist. These are the collections to be watched.
* Create a MongoDB database named `resume-tokens`, and two MongoDB collections named `coll1` 
and `coll2`, if they do not already exist. These are the collections where the resume tokens will be stored. 
* Create two streams on NATS JetStream, `COLL1` and `COLL2`, if they do not already exist.
* Start watching the `coll1` and `coll2` collections, publishing any change event to NATS `COLL1` and `COLL2` streams 
respectively. Depending on the operation type, a different stream subject will be used, for example inserting a document
in `coll1` will result in a message being published on `COLL1.insert`, for updates it will be `COLL1.update`, and for 
deletions `COLL1.delete`.

Check that the connector is up and running:

```
curl -i localhost:8080/healthz
```

If it is running correctly you should get the following response:

```
HTTP/1.1 200 OK
Content-Type: application/json
Date: Tue, 09 May 2023 12:01:54 GMT
Content-Length: 78

{"status":"UP","components":{"mongo":{"status":"UP"},"nats":{"status":"UP"}}}
```

Now let's see it in action by inserting a new document in one of the watched MongoDB collections:

```
docker exec -it mongo1 mongosh
```

Switch to our example database `test-connector`:

```
use test-connector
```

Add a new record to our example collection `coll1`:

```
db.coll1.insertOne({"message": "hi"})
```

Now look at the connector's logs:

```
docker-compose logs -f connector
```

You should see something like the following:

```
connector  | {"time":"2023-05-09T12:59:38.0312255Z","level":"DEBUG","msg":"received change event","changeEvent":"{\"_id\":{\"_data\":\"82645A43BA000000012B022C0100296E5A100441C14B6
03DF24D51BCD95A16D118E42F46645F69640064645A43BA84439E9C4F4144EB0004\"},\"operationType\":\"insert\",\"clusterTime\":{\"$timestamp\":{\"t\":1683637178,\"i\":1}},\"wallTime\":{\"$dat
e\":\"2023-05-09T12:59:38.017Z\"},\"fullDocument\":{\"_id\":{\"$oid\":\"645a43ba84439e9c4f4144eb\"},\"message\":\"hi\"},\"ns\":{\"db\":\"test-connector\",\"coll\":\"coll1\"},\"docu
mentKey\":{\"_id\":{\"$oid\":\"645a43ba84439e9c4f4144eb\"}}}"}

connector  | {"time":"2023-05-09T12:59:38.0350466Z","level":"DEBUG","msg":"published message","subj":"COLL1.insert","data":"{\"_id\":{\"_data\":\"82645A43BA000000012B022C0100296E5A
100441C14B603DF24D51BCD95A16D118E42F46645F69640064645A43BA84439E9C4F4144EB0004\"},\"operationType\":\"insert\",\"clusterTime\":{\"$timestamp\":{\"t\":1683637178,\"i\":1}},\"wallTim
e\":{\"$date\":\"2023-05-09T12:59:38.017Z\"},\"fullDocument\":{\"_id\":{\"$oid\":\"645a43ba84439e9c4f4144eb\"},\"message\":\"hi\"},\"ns\":{\"db\":\"test-connector\",\"coll\":\"coll
1\"},\"documentKey\":{\"_id\":{\"$oid\":\"645a43ba84439e9c4f4144eb\"}}}"}
```

As you can see the change event was received and the connector published a message on NATS JetStream. 
Let's view our example stream `COLL1` with [NATS CLI](https://github.com/nats-io/natscli):

```
nats stream view COLL1
```

You should see something like this:

```
[1] Subject: COLL1.insert Received: 2023-05-09T14:59:38+02:00

  Nats-Msg-Id: 82645A43BA000000012B022C0100296E5A100441C14B603DF24D51BCD95A16D118E42F46645F69640064645A43BA84439E9C4F4144EB0004

{"_id":{"_data":"82645A43BA000000012B022C0100296E5A100441C14B603DF24D51BCD95A16D118E42F46645F69640064645A43BA84439E9C4F4144EB0004"},"operationType":"insert","clusterTime":{"$timest
amp":{"t":1683637178,"i":1}},"wallTime":{"$date":"2023-05-09T12:59:38.017Z"},"fullDocument":{"_id":{"$oid":"645a43ba84439e9c4f4144eb"},"message":"hi"},"ns":{"db":"test-connector","
coll":"coll1"},"documentKey":{"_id":{"$oid":"645a43ba84439e9c4f4144eb"}}}

15:00:14 Reached apparent end of data
```

That's it! The stream `COLL1` received the message under the `insert` subject, as expected.

You can try more operations, such as updating or deleting documents, and when you're done you can stop all the containers with:

```
make stop
```

## Resume Tokens

A MongoDB change stream is composed of change events and each change event has an `_id` field that contains a resume token.
Resume tokens are used to resume the processing of change streams in case of interruptions.

The connector leverages MongoDB's resume tokens by persisting them in a specific collection, this way it is able to track 
what was the last processed change event.

There are a few possible scenarios:
* The connector crashes before publishing the message to NATS and persisting the resume token.
* The connector fails to publish the message to NATS.
* The connector publishes the message to NATS, but fails to persist the resume token.

In all the aforementioned cases the connector will resume from the previous change event token and try again.
While in the first two cases there will be no issues, in the third case, however, a duplicate message is to be expected.
For this reason the connector uses the resume token as a NATS message id, so that NATS consumers can use it as a way
to discard duplicates, more info 
[here](https://docs.nats.io/using-nats/developer/develop_jetstream/model_deep_dive#message-deduplication).

## Customization

You can easily override any configuration by providing your own `connector.yaml` file and run the connector with a few 
environment variables.

### Configuration File

Let's start with the configuration file: it is used to tell the connector what collections it should watch, and in what
streams it should publish events when those collections change (as in, when insertions, updates or deletions are 
performed).

For each collection, the following properties can be configured:

* `dbName`, the name of the database where the collection to watch resides.
* `collName`, the name of the collection to watch.
* `changeStreamPreAndPostImages`, this is a MongoDB configuration, more info
[here](https://www.mongodb.com/docs/manual/changeStreams/#change-streams-with-document-pre--and-post-images).
* `tokensDbName`, the name of the database where the resume tokens collection will reside.
* `tokensCollName`, the name of the resume tokens collection for the watched collection.
* `tokensCollCapped`, whether the resume tokens collection is capped or not.
* `tokensCollSizeInBytes`, the size of the resume tokens collection, if capped.
* `streamName`, the name of the stream where the change events of the watched collection will be published.

Here's an example:

```yaml
connector:
  collections:
    - dbName: twitter-db
      collName: tweets
      changeStreamPreAndPostImages: true
      tokensDbName: resume-tokens
      tokensCollName: tweets
      tokensCollCapped: true
      tokensCollSizeInBytes: 4096
      streamName: TWEETS
    # add more collections here...
```

The configuration above will tell the connector to start watching the `tweets` collection in the `twitter-db` database, 
and to publish its changes to the `TWEETS` stream. It will also tell the connector to store the resume tokens in a capped 
collection of size 4096, with the same name as the watched collection, but in a different database, named `resume-tokens`.

### Environment Variables

The connector supports the following environment variables:

* `CONFIG_FILE`, the path to the configuration file, including the file name. Default value is `connector.yaml`.
* `LOG_LEVEL`, the connector's log level, can be one of the following: `debug`, `info`, `warn`, `error`.
Default value is `info`.
* `MONGO_URI`, your MongoDB URI.
* `NATS_URL`, your NATS URL.
* `SERVER_ADDR`, the connector's server address. Default value is `127.0.0.1:8080`.

Most of the time you will only need to set `MONGO_URI` and `NATS_URL`, for the other variables the defaults will suffice.

### Embedded Connector

The connector can be embedded within your go application! All you need to do is run

```
go get github.com/damianiandrea/mongodb-nats-connector/pkg/connector
```

and use it in your go code like this:

```go
package main

import (
	"context"
	"log"

	"github.com/damianiandrea/mongodb-nats-connector/pkg/connector"
)

func main() {
	c, err := connector.New(
		connector.WithLogLevel("debug"),
		connector.WithMongoUri("..."), // your MongoDB URI
		connector.WithNatsUrl("..."), // your NATS URL
		connector.WithContext(context.Background()),
		connector.WithServerAddr(":9000"),
		connector.WithCollection(
			"test-connector",
			"coll1",
			connector.WithChangeStreamPreAndPostImages(),
			connector.WithTokensDbName("resume-tokens"),
			connector.WithTokensCollName("coll1"),
			connector.WithTokensCollCapped(4096),
			connector.WithStreamName("COLL1"),
		),
	)

	if err != nil {
		log.Fatal(err)
	}

	log.Fatal(c.Run())
}
```

The `connector.New()` method accepts functional options that map to the same properties found in the yaml configuration
file.

### External Resources

* [Blog Post](https://nats.io/blog/mongodb-nats-connector/)
* [Other MongoDB Connectors](https://www.mongodb.com/connectors)
* [Other NATS Connectors](https://nats.io/download/#connectors-and-utilities)
