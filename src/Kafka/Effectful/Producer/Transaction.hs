module Kafka.Effectful.Producer.Transaction (
    commitOffsetMessageTransaction,
)
where

import Effectful (Eff, (:>))
import Kafka.Consumer.Types (ConsumerRecord)
import Kafka.Effectful.Consumer.Effect (KafkaConsumer, askConsumerHandle)
import Kafka.Effectful.Producer.Effect (KafkaProducer, sendOffsetsToTransaction)
import Kafka.Transaction (TxError)
import Kafka.Types (Timeout)

{- | Commit the offset of a single 'ConsumerRecord' inside the current
transaction of the surrounding 'KafkaProducer'.

This is the cross-effect counterpart to
'Kafka.Effectful.Consumer.Effect.commitOffsetMessage' — the record's
offset is not stored via the consumer's offset-commit path but
instead shipped as part of the producer's open transaction, which
preserves the exactly-once semantics required by Scenario 5 of
@hw-kafka-client@'s producer best practices.

Returns @Nothing@ on success or @Just TxError@ on failure. The
caller must dispatch on the three 'TxError' discriminators
(@kafkaErrorTxnRequiresAbort@ first, then @kafkaErrorIsRetriable@,
then @kafkaErrorIsFatal@) to decide whether to abort the
transaction, retry the commit, or crash.

@since 0.2.0.0
-}
commitOffsetMessageTransaction ::
    (KafkaProducer :> es, KafkaConsumer :> es) =>
    ConsumerRecord k v ->
    Timeout ->
    Eff es (Maybe TxError)
commitOffsetMessageTransaction record timeout = do
    consumer <- askConsumerHandle
    sendOffsetsToTransaction consumer record timeout
