package rmq

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCleaner(t *testing.T) {
	flushConn, err := OpenConnection("cleaner-flush", "tcp", "localhost:6379", 1, nil)
	assert.NoError(t, err)
	assert.NoError(t, flushConn.stopHeartbeat())
	assert.NoError(t, flushConn.flushDb())

	conn, err := OpenConnection("cleaner-conn1", "tcp", "localhost:6379", 1, nil)
	assert.NoError(t, err)
	queues, err := conn.GetOpenQueues()
	assert.NoError(t, err)
	assert.Len(t, queues, 0)
	queue, err := conn.OpenQueue("q1")
	assert.NoError(t, err)
	queues, err = conn.GetOpenQueues()
	assert.NoError(t, err)
	assert.Len(t, queues, 1)
	_, err = conn.OpenQueue("q2")
	assert.NoError(t, err)
	queues, err = conn.GetOpenQueues()
	assert.NoError(t, err)
	assert.Len(t, queues, 2)

	count, err := queue.readyCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), count)
	assert.NoError(t, queue.Publish("del1"))
	count, err = queue.readyCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), count)
	assert.NoError(t, queue.Publish("del2"))
	count, err = queue.readyCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count)
	assert.NoError(t, queue.Publish("del3"))
	count, err = queue.readyCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(3), count)
	assert.NoError(t, queue.Publish("del4"))
	count, err = queue.readyCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(4), count)
	assert.NoError(t, queue.Publish("del5"))
	count, err = queue.readyCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(5), count)
	assert.NoError(t, queue.Publish("del6"))
	count, err = queue.readyCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(6), count)

	count, err = queue.unackedCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), count)
	assert.NoError(t, queue.StartConsuming(2, time.Millisecond, nil))
	time.Sleep(time.Millisecond)
	count, err = queue.unackedCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count)
	count, err = queue.readyCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(4), count)

	consumer := NewTestConsumer("c-A")
	consumer.AutoFinish = false
	consumer.AutoAck = false

	_, err = queue.AddConsumer("consumer1", consumer)
	assert.NoError(t, err)
	time.Sleep(10 * time.Millisecond)
	count, err = queue.unackedCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count)
	count, err = queue.readyCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(4), count)

	require.NotNil(t, consumer.LastDelivery)
	assert.Equal(t, "del1", consumer.LastDelivery.Payload())
	assert.NoError(t, consumer.LastDelivery.Ack(nil, nil))
	time.Sleep(10 * time.Millisecond)
	count, err = queue.unackedCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count)
	count, err = queue.readyCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(3), count)

	consumer.Finish()
	time.Sleep(10 * time.Millisecond)
	count, err = queue.unackedCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count)
	count, err = queue.readyCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(3), count)
	assert.Equal(t, "del2", consumer.LastDelivery.Payload())

	queue.StopConsuming()
	assert.NoError(t, conn.stopHeartbeat())
	time.Sleep(time.Millisecond)

	conn, err = OpenConnection("cleaner-conn1", "tcp", "localhost:6379", 1, nil)
	assert.NoError(t, err)
	queue, err = conn.OpenQueue("q1")
	assert.NoError(t, err)

	assert.NoError(t, queue.Publish("del7"))
	count, err = queue.readyCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(4), count)
	assert.NoError(t, queue.Publish("del8"))
	count, err = queue.readyCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(5), count)
	assert.NoError(t, queue.Publish("del9"))
	count, err = queue.readyCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(6), count)
	assert.NoError(t, queue.Publish("del10"))
	count, err = queue.readyCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(7), count)
	assert.NoError(t, queue.Publish("del11"))
	count, err = queue.readyCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(8), count)

	count, err = queue.unackedCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), count)
	assert.NoError(t, queue.StartConsuming(2, time.Millisecond, nil))
	time.Sleep(time.Millisecond)
	count, err = queue.unackedCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count)
	count, err = queue.readyCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(6), count)

	consumer = NewTestConsumer("c-B")
	consumer.AutoFinish = false
	consumer.AutoAck = false

	_, err = queue.AddConsumer("consumer2", consumer)
	assert.NoError(t, err)
	time.Sleep(10 * time.Millisecond)
	count, err = queue.unackedCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count)
	count, err = queue.readyCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(6), count)
	assert.Equal(t, "del4", consumer.LastDelivery.Payload())

	consumer.Finish() // unacked
	time.Sleep(10 * time.Millisecond)
	count, err = queue.unackedCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count)
	count, err = queue.readyCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(6), count)

	assert.Equal(t, "del5", consumer.LastDelivery.Payload())
	assert.NoError(t, consumer.LastDelivery.Ack(nil, nil))
	time.Sleep(10 * time.Millisecond)
	count, err = queue.unackedCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count)
	count, err = queue.readyCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(5), count)

	queue.StopConsuming()
	assert.NoError(t, conn.stopHeartbeat())
	time.Sleep(time.Millisecond)

	cleanerConn, err := OpenConnection("cleaner-conn", "tcp", "localhost:6379", 1, nil)
	assert.NoError(t, err)
	cleaner := NewCleaner(cleanerConn)
	returned, err := cleaner.Clean()
	assert.NoError(t, err)
	assert.Equal(t, int64(4), returned)
	count, err = queue.readyCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(9), count) // 2 of 11 were acked above
	queues, err = conn.GetOpenQueues()
	assert.NoError(t, err)
	assert.Len(t, queues, 2)

	conn, err = OpenConnection("cleaner-conn1", "tcp", "localhost:6379", 1, nil)
	assert.NoError(t, err)
	queue, err = conn.OpenQueue("q1")
	assert.NoError(t, err)
	assert.NoError(t, queue.StartConsuming(10, time.Millisecond, nil))
	consumer = NewTestConsumer("c-C")

	_, err = queue.AddConsumer("consumer3", consumer)
	assert.NoError(t, err)
	time.Sleep(10 * time.Millisecond)
	assert.Len(t, consumer.LastDeliveries, 9)

	queue.StopConsuming()
	assert.NoError(t, conn.stopHeartbeat())
	time.Sleep(time.Millisecond)

	returned, err = cleaner.Clean()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), returned)
	assert.NoError(t, cleanerConn.stopHeartbeat())
}
