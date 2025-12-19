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

## Modules / Examples

| Class                              | What it demonstrates |
|------------------------------------|----------------------|
| BasicStringConsumer                | Basic string producer |
| ItemProducerWithSpecificSerializer | Custom value serializer |
| JsonStringItemConsumer             | Manual JSON deserialization |
| AsyncCommitConsumer                | Async commit |
| PartitionOffsetCommitConsumer      | Commit per partition |
| MessageConsumerRebalanceListener   | Rebalance handling |
| MessageConsumerSeekByOffset        | Seek using stored offsets |
