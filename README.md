# MongoDB-NATS Connector

![CI](https://github.com/damianiandrea/go-mongo-nats-connector/actions/workflows/ci.yml/badge.svg)

A connector that uses MongoDB's change streams to capture data changes and publishes those changes to NATS JetStream.

I wanted an easy way to do CDC (change data capture) with MongoDB and sink the data to NATS, but couldn't find any 
existing solution, so I decided to build my own.

## Quick Start

After cloning the repository you can use the following command to run a MongoDB replica set, a NATS cluster with 
JetStream enabled, and the connector itself.

If you have [Task](https://taskfile.dev/) installed (you really should, it's quite cool), you can simply use:

```bash
task run
```

If you don't, use this command instead:

```bash
docker-compose up --build -d mongo1 mongo2 mongo3 nats1 nats2 nats3 connector
```

The connector will pick up the default `connector.yaml` configuration file, and it will perform the following actions:
* Create a MongoDB database named `test-connector`, and two MongoDB collections named `coll1` 
and `coll2`, if they do not already exist. These are the collections to be watched.
* Create a MongoDB database named `resume-tokens`, and two MongoDB collections named `coll1` 
and `coll2`, if they do not already exist. These are the collections where the resume tokens will be stored. 
* Create two streams on NATS JetStream, `COLL1` and `COLL2`, if they do not already exist.
* Start watching the `coll1` and `coll2` collections, publishing any change event to NATS `COLL1` and `COLL2` streams 
respectively. Depending on the operation type, a different stream subject will be used, for example inserting a document
in `coll1` will result in a message being published on `COLL1.insert`, for updates it will be `COLL1.update`, and for 
deletions `COLL1.delete`.

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
to discard duplicates.

## Customization

You can easily override any configuration by providing your own `connector.yaml` file.

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
      tokensCollSize: 4096
      streamName: TWEETS
```

The configuration above will tell the connector to start watching the `tweets` collection in the `twitter-db` database, 
and to publish its changes to the `TWEETS` stream. It will also tell the connector to store the resume tokens in a capped 
collection of size 4096, with the same name as the watched collection, but in a different database, named `resume-tokens`.
