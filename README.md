# Mongo-Nats Connector

A connector that uses MongoDB's change streams to capture data changes and publishes those changes to Nats JetStream.

I wanted an easy way to do CDC (change data capture) with MongoDB and sink the data to NATS, and couldn't find any existing solution, so
I decided to build my own.

# Quick Start

After cloning the repository you can use the following command to run a MongoDB replica set, a NATS cluster with 
JetStream enabled, and the connector itself.

If you have [Task](https://taskfile.dev/) installed (you really should, it's quite cool!), you can simply use:

```bash
task run
```

If you don't, use this command instead:

```bash
docker-compose up --build -d mongo1 mongo2 mongo3 nats1 nats2 nats3 connector
```

The connector will pick up the default `connector.yaml` configuration file, and it will perform the following actions:
* Create (if they do not exist) a MongoDB database named `test-connector`, and two MongoDB collections named `coll1` 
and `coll2`. These are the collections to be watched.
* Create (if they do not exist) a MongoDB database named `resume-tokens`, and two MongoDB collections named `coll1` 
and `coll2`. These are the collections where the resume tokens will be stored.
* Create two streams on NATS JetStream, `COLL1` and `COLL2`.
* Start watching the `coll1` and `coll2` collections, publishing any change stream to NATS `COLL1` and `COLL2` streams 
respectively.

# Customization

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

The previous configuration will tell the connector to create a `tweets` collection in the `twitter-db` database 
(unless they already exist), store the tokens in a capped collection of size 4096, with the same name, but in a 
different database, named `resume-tokens`.