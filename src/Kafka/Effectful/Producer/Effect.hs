module Kafka.Effectful.Producer.Effect (
    -- * Effect
    KafkaProducer (..),

    -- * Operations
    produceMessage,
    produceMessage',
    produceMessageSync,
    produceMessageBatch,
    flushProducer,

    -- * Transactions
    initTransactions,
    beginTransaction,
    commitTransaction,
    abortTransaction,

    -- ** Internal — cross-effect plumbing
    sendOffsetsToTransaction,
    askProducerHandle,
)
where

import Effectful (Dispatch (..), DispatchOf, Eff, Effect, (:>))
import Effectful.Dispatch.Dynamic (send)
import Kafka.Consumer.Types (ConsumerRecord, Offset)
import Kafka.Consumer.Types qualified as KC
import Kafka.Producer.Types (DeliveryReport, ProducerRecord)
import Kafka.Producer.Types qualified as KP
import Kafka.Transaction (TxError)
import Kafka.Types (KafkaError, Timeout)

-- | Effect for Kafka producer operations.
data KafkaProducer :: Effect where
    ProduceMessage ::
        ProducerRecord ->
        KafkaProducer m ()
    ProduceMessage' ::
        ProducerRecord ->
        (DeliveryReport -> IO ()) ->
        KafkaProducer m ()
    ProduceMessageSync ::
        ProducerRecord ->
        KafkaProducer m Offset
    ProduceMessageBatch ::
        [ProducerRecord] ->
        KafkaProducer m [(ProducerRecord, KafkaError)]
    FlushProducer ::
        KafkaProducer m ()
    InitTransactions ::
        Timeout ->
        KafkaProducer m ()
    BeginTransaction ::
        KafkaProducer m ()
    CommitTransaction ::
        Timeout ->
        KafkaProducer m (Maybe TxError)
    AbortTransaction ::
        Timeout ->
        KafkaProducer m ()
    SendOffsetsToTransaction ::
        KC.KafkaConsumer ->
        ConsumerRecord k v ->
        Timeout ->
        KafkaProducer m (Maybe TxError)
    AskProducerHandle ::
        KafkaProducer m KP.KafkaProducer

type instance DispatchOf KafkaProducer = 'Dynamic

{- | Send a single message to Kafka.
Throws 'KafkaError' via the 'Error' effect on failure.
-}
produceMessage :: (KafkaProducer :> es) => ProducerRecord -> Eff es ()
produceMessage = send . ProduceMessage

{- | Send a single message with a per-message 'DeliveryReport' callback.

The callback runs on a librdkafka-forked thread, so blocking operations
(such as writing to an @MVar@) are safe. Throws 'KafkaError' via the
'Error' effect when the underlying send fails to enqueue
(@ImmediateError@).

This is the low-level primitive for Scenario 2 of
@hw-kafka-client@'s producer best practices. Callers that only need a
single synchronous send should prefer 'produceMessageSync'.

@since 0.2.0.0
-}
produceMessage' ::
    (KafkaProducer :> es) =>
    ProducerRecord ->
    (DeliveryReport -> IO ()) ->
    Eff es ()
produceMessage' record cb = send (ProduceMessage' record cb)

{- | Send a single message and block until the broker acknowledges it,
returning the broker-assigned 'Offset'.

Throws 'KafkaError' via the 'Error' effect on enqueue failure
(@ImmediateError@) or on delivery failure reported via the
'DeliveryReport'.

This is the high-level convenience for Scenario 2 of
@hw-kafka-client@'s producer best practices.

@since 0.2.0.0
-}
produceMessageSync ::
    (KafkaProducer :> es) =>
    ProducerRecord ->
    Eff es Offset
produceMessageSync = send . ProduceMessageSync

{- | Send many records in one call.

Returns only the records that failed to enqueue, paired with the
error librdkafka reported for that record. Successful records are
omitted. This mirrors @Kafka.Producer.produceMessageBatch@.

Combined with @linger.ms@ and @batch.size@ set on the
@ProducerProperties@, this is the throughput-oriented path — Scenario
4 of @hw-kafka-client@'s producer best practices.

@since 0.2.0.0
-}
produceMessageBatch ::
    (KafkaProducer :> es) =>
    [ProducerRecord] ->
    Eff es [(ProducerRecord, KafkaError)]
produceMessageBatch = send . ProduceMessageBatch

-- | Flush the producer's outbound queue, blocking until all messages are sent.
flushProducer :: (KafkaProducer :> es) => Eff es ()
flushProducer = send FlushProducer

{- | Initialise the transactional producer.

Must be called exactly once per producer, after @runKafkaProducer@
has acquired the handle and before any call to 'beginTransaction'.
The producer's @ProducerProperties@ must set @transactional.id@,
@enable.idempotence=true@, and @acks=all@.

Throws 'KafkaError' via the 'Error' effect on failure.

@since 0.2.0.0
-}
initTransactions :: (KafkaProducer :> es) => Timeout -> Eff es ()
initTransactions = send . InitTransactions

{- | Open a new transaction.

Must be preceded by exactly one successful 'initTransactions' on the
same producer handle. Throws 'KafkaError' via the 'Error' effect on
failure.

@since 0.2.0.0
-}
beginTransaction :: (KafkaProducer :> es) => Eff es ()
beginTransaction = send BeginTransaction

{- | Commit the currently-open transaction.

Returns @Nothing@ on success or @Just TxError@ on failure. The
caller must branch on the three 'TxError' discriminators
(@kafkaErrorTxnRequiresAbort@ first, then @kafkaErrorIsRetriable@,
then @kafkaErrorIsFatal@) to decide whether to abort, retry, or
crash.

@since 0.2.0.0
-}
commitTransaction :: (KafkaProducer :> es) => Timeout -> Eff es (Maybe TxError)
commitTransaction = send . CommitTransaction

{- | Abort the currently-open transaction.

Throws 'KafkaError' via the 'Error' effect on failure.

@since 0.2.0.0
-}
abortTransaction :: (KafkaProducer :> es) => Timeout -> Eff es ()
abortTransaction = send . AbortTransaction

{- | Send the consumer offsets for a single 'ConsumerRecord' to the
open transaction.

This is plumbing used by
'Kafka.Effectful.Producer.Transaction.commitOffsetMessageTransaction'
and is not intended to be called directly — end-users should use the
helper, which picks up the consumer handle automatically via
@askConsumerHandle@.

@since 0.2.0.0
-}
sendOffsetsToTransaction ::
    (KafkaProducer :> es) =>
    KC.KafkaConsumer ->
    ConsumerRecord k v ->
    Timeout ->
    Eff es (Maybe TxError)
sendOffsetsToTransaction consumer record timeout =
    send (SendOffsetsToTransaction consumer record timeout)

{- | Escape hatch: return the raw @Kafka.Producer.KafkaProducer@ handle
acquired by @runKafkaProducer@.

Exposed to enable the cross-effect
'Kafka.Effectful.Producer.Transaction.commitOffsetMessageTransaction'
helper, which must reach both the producer and consumer handles to
call the underlying transactional offset-commit primitive. New
operations should go through the 'KafkaProducer' effect rather than
this handle.

@since 0.2.0.0
-}
askProducerHandle :: (KafkaProducer :> es) => Eff es KP.KafkaProducer
askProducerHandle = send AskProducerHandle
