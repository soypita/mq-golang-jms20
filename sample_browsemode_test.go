package main

import (
	"fmt"
	"testing"

	"github.com/soypita/mq-golang-jms20/jms20subset"
	"github.com/soypita/mq-golang-jms20/mqjms"
	"github.com/stretchr/testify/assert"
)

func TestBrowseAll(t *testing.T) {
	// Loads CF parameters from connection_info.json and apiKey.json in the Downloads directory
	cf, cfErr := mqjms.CreateConnectionFactoryFromDefaultJSONFiles()
	assert.Nil(t, cfErr)

	cf.BrowseMode = true

	// Creates a connection to the queue manager, using defer to close it automatically
	// at the end of the function (if it was created successfully)
	context, ctxErr := cf.CreateContext()

	assert.Nil(t, ctxErr)
	if context != nil {
		defer context.Close()
	}

	// Equivalent to a JNDI lookup or other declarative definition
	queue := context.CreateQueue("DEV.QUEUE.1")

	// Set up the consumer ready to receive messages.
	consumer, conErr := context.CreateConsumer(queue)
	assert.Nil(t, conErr)
	if consumer != nil {
		defer consumer.Close()
	}

	// There is no messages in queue
	testMsgList, err := consumer.BrowseAllNoWait()
	assert.Nil(t, testMsgList)
	assert.Nil(t, err)

	// Send a list of messages.
	countOfMessages := 10
	producer := context.CreateProducer()
	for i := 0; i < countOfMessages; i++ {
		bodyMsg := fmt.Sprintf("MyMessage_%d", i)
		err := producer.SendString(queue, bodyMsg)
		assert.Nil(t, err)
	}

	// Try to browse messages for first time
	testMsgList, err2 := consumer.BrowseAllNoWait()
	assert.NotNil(t, testMsgList)
	assert.Nil(t, err2)
	assert.Equal(t, countOfMessages, len(testMsgList))

	// Try to browse second time
	testMsgList, err2 = consumer.BrowseAllNoWait()
	assert.NotNil(t, testMsgList)
	assert.Nil(t, err2)
	assert.Equal(t, countOfMessages, len(testMsgList))

	// Clear queue
	clearQueue(cf, queue, countOfMessages)
}

func TestBrowseAllStringBody(t *testing.T) {
	// Loads CF parameters from connection_info.json and apiKey.json in the Downloads directory
	cf, cfErr := mqjms.CreateConnectionFactoryFromDefaultJSONFiles()
	assert.Nil(t, cfErr)

	cf.BrowseMode = true

	// Creates a connection to the queue manager, using defer to close it automatically
	// at the end of the function (if it was created successfully)
	context, ctxErr := cf.CreateContext()

	assert.Nil(t, ctxErr)
	if context != nil {
		defer context.Close()
	}

	// Equivalent to a JNDI lookup or other declarative definition
	queue := context.CreateQueue("DEV.QUEUE.1")

	// Set up the consumer ready to receive messages.
	consumer, conErr := context.CreateConsumer(queue)
	assert.Nil(t, conErr)
	if consumer != nil {
		defer consumer.Close()
	}

	// There is no messages in queue
	testMsgList, err := consumer.BrowseAllStringBodyNoWait()
	assert.Nil(t, testMsgList)
	assert.Nil(t, err)

	// Send a list of messages.
	countOfMessages := 10
	producer := context.CreateProducer()
	for i := 0; i < countOfMessages; i++ {
		bodyMsg := fmt.Sprintf("MyMessage_%d", i)
		err := producer.SendString(queue, bodyMsg)
		assert.Nil(t, err)
	}

	// Try to browse messages for first time
	testMsgList, err2 := consumer.BrowseAllStringBodyNoWait()
	assert.NotNil(t, testMsgList)
	assert.Nil(t, err2)
	assert.Equal(t, countOfMessages, len(testMsgList))

	for i := 0; i < countOfMessages; i++ {
		bodyMsg := fmt.Sprintf("MyMessage_%d", i)
		assert.Equal(t, bodyMsg, *testMsgList[i])
	}

	// Try to browse second time
	testMsgList, err2 = consumer.BrowseAllStringBodyNoWait()
	assert.NotNil(t, testMsgList)
	assert.Nil(t, err2)
	assert.Equal(t, countOfMessages, len(testMsgList))

	for i := 0; i < countOfMessages; i++ {
		bodyMsg := fmt.Sprintf("MyMessage_%d", i)
		assert.Equal(t, bodyMsg, *testMsgList[i])
	}

	// Clear queue
	clearQueue(cf, queue, countOfMessages)
}

func TestBrowseAllStringBodyWait(t *testing.T) {
	// Loads CF parameters from connection_info.json and apiKey.json in the Downloads directory
	cf, cfErr := mqjms.CreateConnectionFactoryFromDefaultJSONFiles()
	assert.Nil(t, cfErr)

	cf.BrowseMode = true

	// Creates a connection to the queue manager, using defer to close it automatically
	// at the end of the function (if it was created successfully)
	context, ctxErr := cf.CreateContext()

	assert.Nil(t, ctxErr)
	if context != nil {
		defer context.Close()
	}

	// Equivalent to a JNDI lookup or other declarative definition
	queue := context.CreateQueue("DEV.QUEUE.1")

	// Set up the consumer ready to receive messages.
	consumer, conErr := context.CreateConsumer(queue)
	assert.Nil(t, conErr)
	if consumer != nil {
		defer consumer.Close()
	}

	waitTime := int32(500)

	// No messages in queue
	startTime := currentTimeMillis()
	testMsgList, err := consumer.BrowseAllStringBody(waitTime)
	endTime := currentTimeMillis()
	assert.Nil(t, testMsgList)
	assert.Nil(t, err)

	// Within a reasonable margin of the expected wait time.
	assert.True(t, (endTime-startTime-int64(waitTime)) > -100)

	// Send a list of messages.
	countOfMessages := 10
	producer := context.CreateProducer()
	for i := 0; i < countOfMessages; i++ {
		bodyMsg := fmt.Sprintf("MyMessage_%d", i)
		err := producer.SendString(queue, bodyMsg)
		assert.Nil(t, err)
	}

	// Try to browse messages
	startTime = currentTimeMillis()
	testMsgList, err = consumer.BrowseAllStringBody(waitTime)
	endTime = currentTimeMillis()
	assert.NotNil(t, testMsgList)
	assert.Nil(t, err)
	assert.Equal(t, countOfMessages, len(testMsgList))

	// Within a reasonable margin of the expected wait time.
	assert.True(t, (endTime-startTime) < int64(waitTime*int32(countOfMessages)))

	for i := 0; i < countOfMessages; i++ {
		bodyMsg := fmt.Sprintf("MyMessage_%d", i)
		assert.Equal(t, bodyMsg, *testMsgList[i])
	}

	// Try to browse message for the second time
	startTime = currentTimeMillis()
	testMsgList, err = consumer.BrowseAllStringBody(waitTime)
	endTime = currentTimeMillis()
	assert.NotNil(t, testMsgList)
	assert.Nil(t, err)
	assert.Equal(t, countOfMessages, len(testMsgList))

	// Within a reasonable margin of the expected wait time.
	assert.True(t, (endTime-startTime) < int64(waitTime*int32(countOfMessages)))

	for i := 0; i < countOfMessages; i++ {
		bodyMsg := fmt.Sprintf("MyMessage_%d", i)
		assert.Equal(t, bodyMsg, *testMsgList[i])
	}

	// Clear queue
	clearQueue(cf, queue, countOfMessages)
}

func TestBrowseAllWait(t *testing.T) {
	// Loads CF parameters from connection_info.json and apiKey.json in the Downloads directory
	cf, cfErr := mqjms.CreateConnectionFactoryFromDefaultJSONFiles()
	assert.Nil(t, cfErr)

	cf.BrowseMode = true

	// Creates a connection to the queue manager, using defer to close it automatically
	// at the end of the function (if it was created successfully)
	context, ctxErr := cf.CreateContext()

	assert.Nil(t, ctxErr)
	if context != nil {
		defer context.Close()
	}

	// Equivalent to a JNDI lookup or other declarative definition
	queue := context.CreateQueue("DEV.QUEUE.1")

	// Set up the consumer ready to receive messages.
	consumer, conErr := context.CreateConsumer(queue)
	assert.Nil(t, conErr)
	if consumer != nil {
		defer consumer.Close()
	}

	waitTime := int32(500)

	// No messages in queue
	startTime := currentTimeMillis()
	testMsgList, err := consumer.BrowseAll(waitTime)
	endTime := currentTimeMillis()
	assert.Nil(t, testMsgList)
	assert.Nil(t, err)

	// Within a reasonable margin of the expected wait time.
	assert.True(t, (endTime-startTime-int64(waitTime)) > -100)

	// Send a list of messages.
	countOfMessages := 10
	producer := context.CreateProducer()
	for i := 0; i < countOfMessages; i++ {
		bodyMsg := fmt.Sprintf("MyMessage_%d", i)
		err := producer.SendString(queue, bodyMsg)
		assert.Nil(t, err)
	}

	// Try to browse messages
	startTime = currentTimeMillis()
	testMsgList, err = consumer.BrowseAll(waitTime)
	endTime = currentTimeMillis()
	assert.NotNil(t, testMsgList)
	assert.Nil(t, err)
	assert.Equal(t, countOfMessages, len(testMsgList))

	// Within a reasonable margin of the expected wait time.
	assert.True(t, (endTime-startTime) < int64(waitTime*int32(countOfMessages)))

	// Try to browse message for the second time
	startTime = currentTimeMillis()
	testMsgList, err = consumer.BrowseAll(waitTime)
	endTime = currentTimeMillis()
	assert.NotNil(t, testMsgList)
	assert.Nil(t, err)
	assert.Equal(t, countOfMessages, len(testMsgList))

	// Within a reasonable margin of the expected wait time.
	assert.True(t, (endTime-startTime) < int64(waitTime*int32(countOfMessages)))

	// Clear queue
	clearQueue(cf, queue, countOfMessages)
}

func clearQueue(connectionFactory mqjms.ConnectionFactoryImpl, queue jms20subset.Queue, queueLen int) {
	connectionFactory.BrowseMode = false
	ctx, _ := connectionFactory.CreateContext()

	consumer, conErr := ctx.CreateConsumer(queue)
	if conErr != nil {
		panic(conErr)
	}
	if consumer != nil {
		defer consumer.Close()
	}

	for queueLen != 0 {
		_, err := consumer.ReceiveStringBodyNoWait()
		if err != nil {
			panic(err)
		}
		queueLen--
	}
}
