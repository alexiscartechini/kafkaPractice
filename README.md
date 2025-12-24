# Kafka Practice Project (Java)

This project demonstrates core Kafka concepts using plain Java:
- Producers and consumers
- String vs custom (JSON) serialization
- Manual vs automatic offset commits
- Asynchronous vs synchronous commits
- Consumer rebalance handling
- Seeking offsets manually
- Persisting offsets outside Kafka (educational purposes)

This is a learning project, not production-ready code.

### Modules / Examples

## Modules / Examples

| Class                      | What it demonstrates                                    |
|----------------------------|---------------------------------------------------------|
| MessageProducer            | Basic Kafka producer with String key/value              |
| JsonItemProducer           | Producing JSON messages without a custom serializer     |
| TypedItemProducer          | Producing typed messages using a custom Kafka serializer |
| BasicStringConsumer        | Basic Kafka consumer with String key/value              |
| JsonItemConsumer           | Consuming JSON messages and deserializing manually      |
| TypedItemConsumer          | Consuming typed messages using a custom deserializer    |
| AsynchronousCommitConsumer | Manual asynchronous offset commits                      |
| SynchronousCommitConsumer  | Manual synchronous offset commits                       |
| PartitionCommitConsumer    | Committing offsets per partition                        |
| RebalanceAwareConsumer     | Handling consumer rebalance events                      |
| SeekOffsetConsumer         | Seeking offsets manually on partition assignment        |
| OffsetRebalanceListener    | Custom ConsumerRebalanceListener implementation         |


### Persisting offsets to file

This example demonstrates manual offset storage for educational purposes.
In real systems, offsets should be stored in Kafka or an external durable store.