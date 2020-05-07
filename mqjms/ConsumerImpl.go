// Copyright (c) IBM Corporation 2019.
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0, which is available at
// http://www.eclipse.org/legal/epl-2.0.
//
// SPDX-License-Identifier: EPL-2.0

//
package mqjms

import (
	"errors"
	"github.com/ibm-messaging/mq-golang-jms20/jms20subset"
	"github.com/ibm-messaging/mq-golang/ibmmq"
	"strconv"
	"strings"
)

// ConsumerImpl defines a struct that contains the necessary objects for
// receiving messages from a queue on an IBM MQ queue manager.
type ConsumerImpl struct {
	qObject    ibmmq.MQObject
	selector   string
	browseMode bool
}
// BrowseAllNoWait implements the IBM MQ logic necessary to browse all messages from
// a Destination, or immediately return a nil slice of Messages if there is no available
// messages to be browse.
func (consumer ConsumerImpl) BrowseAllNoWait() ([]jms20subset.Message, jms20subset.JMSException) {
	if !consumer.browseMode {
		return nil, jms20subset.CreateJMSException("consumer should be in browse mode", "", nil)
	}
	gmo := ibmmq.NewMQGMO()
	return consumer.browseInternal(gmo)
}

// BrowseAll(waitMillis) returns a slice of messages if at least one is available, or otherwise
// waits for up to the specified number of milliseconds for at least one to become
// available. A value of zero or less indicates to wait indefinitely.
func (consumer ConsumerImpl) BrowseAll(waitMillis int32) ([]jms20subset.Message, jms20subset.JMSException) {
	if !consumer.browseMode {
		return nil, jms20subset.CreateJMSException("consumer should be in browse mode", "", nil)
	}

	if waitMillis <= 0 {
		waitMillis = ibmmq.MQWI_UNLIMITED
	}

	gmo := ibmmq.NewMQGMO()
	gmo.Options |= ibmmq.MQGMO_WAIT
	gmo.WaitInterval = waitMillis

	return consumer.browseInternal(gmo)
}

// BrowseAllStringBodyNoWait implements the IBM MQ logic necessary to receive a
// message from a Destination and return its body as a string.
//
// If no message is immediately available to be returned then a nil is returned.
func (consumer ConsumerImpl) BrowseAllStringBodyNoWait() ([]*string, jms20subset.JMSException) {
	if !consumer.browseMode {
		return nil, jms20subset.CreateJMSException("consumer should be in browse mode", "", nil)
	}

	var resMessages []*string
	var msgBodyStrPtr *string
	var jmsErr jms20subset.JMSException

	// Get a message from the queue if one is available.
	msg, jmsErr := consumer.BrowseAllNoWait()

	// If we receive a message without any errors
	if jmsErr == nil && msg != nil {
		for _, rawMessage := range msg {
			switch msg := rawMessage.(type) {
			case jms20subset.TextMessage:
				msgBodyStrPtr = msg.GetText()
			default:
				jmsErr = jms20subset.CreateJMSException(
					"Received message is not a TextMessage", "MQJMS6068", nil)
				// skip this message
				continue
			}
			resMessages = append(resMessages, msgBodyStrPtr)
		}
	}

	return resMessages, jmsErr
}

// BrowseAllStringBody implements the IBM MQ logic necessary to receive a
// message from a Destination and return its body as a string.
//
// If no message is available the method blocks up to the specified number
// of milliseconds for one to become available.
func (consumer ConsumerImpl) BrowseAllStringBody(waitMillis int32) ([]*string, jms20subset.JMSException) {
	if !consumer.browseMode {
		return nil, jms20subset.CreateJMSException("consumer should be in browse mode", "", nil)
	}

	var resMessages []*string
	var msgBodyStrPtr *string
	var jmsErr jms20subset.JMSException

	// Get a message from the queue if one is available.
	msg, jmsErr := consumer.BrowseAll(waitMillis)

	// If we receive a message without any errors
	if jmsErr == nil && msg != nil {
		for _, rawMessage := range msg {
			switch msg := rawMessage.(type) {
			case jms20subset.TextMessage:
				msgBodyStrPtr = msg.GetText()
			default:
				jmsErr = jms20subset.CreateJMSException(
					"Received message is not a TextMessage", "MQJMS6068", nil)
				// skip this message
				continue
			}
			resMessages = append(resMessages, msgBodyStrPtr)
		}
	}

	return resMessages, jmsErr
}

// ReceiveNoWait implements the IBM MQ logic necessary to receive a message from
// a Destination, or immediately return a nil Message if there is no available
// message to be received.
func (consumer ConsumerImpl) ReceiveNoWait() (jms20subset.Message, jms20subset.JMSException) {

	gmo := ibmmq.NewMQGMO()
	return consumer.receiveInternal(gmo)

}

// Receive(waitMillis) returns a message if one is available, or otherwise
// waits for up to the specified number of milliseconds for one to become
// available. A value of zero or less indicates to wait indefinitely.
func (consumer ConsumerImpl) Receive(waitMillis int32) (jms20subset.Message, jms20subset.JMSException) {

	if waitMillis <= 0 {
		waitMillis = ibmmq.MQWI_UNLIMITED
	}

	gmo := ibmmq.NewMQGMO()
	gmo.Options |= ibmmq.MQGMO_WAIT
	gmo.WaitInterval = waitMillis

	return consumer.receiveInternal(gmo)

}

func (consumer ConsumerImpl) browseInternal(gmo *ibmmq.MQGMO) ([]jms20subset.Message, jms20subset.JMSException) {
	// Prepare objects to be used in receiving the message.
	var msg jms20subset.Message
	var resultMessages []jms20subset.Message
	var jmsErr jms20subset.JMSException

	buffer := make([]byte, 32768)

	msgAvail := true
	var datalen int
	var err error
	isFirstRead := true
	// Set the GMO (get message options)
	gmo.Options |= ibmmq.MQGMO_NO_SYNCPOINT
	gmo.Options |= ibmmq.MQGMO_FAIL_IF_QUIESCING
	gmo.Options |= ibmmq.MQGMO_BROWSE_FIRST

	for msgAvail == true && err == nil {
		getmqmd := ibmmq.NewMQMD()

		// Apply the selector if one has been specified in the Consumer
		err = applySelector(consumer.selector, getmqmd, gmo)
		if err != nil {
			jmsErr = jms20subset.CreateJMSException("ErrorParsingSelector", "ErrorParsingSelector", err)
			return nil, jmsErr
		}
		// Now we can try to get the message. This operation returns
		// a buffer that can be used directly.
		buffer, datalen, err = consumer.qObject.GetSlice(getmqmd, gmo, buffer)

		if err != nil {
			msgAvail = false
			mqret := err.(*ibmmq.MQReturn)
			if mqret.MQRC == ibmmq.MQRC_NO_MSG_AVAILABLE {
				err = nil
			}
		} else {
			// Message received successfully (without error).
			// Currently we only support TextMessage, so extract the content of the
			// message and populate it into a text string.
			var msgBodyStr *string

			if datalen > 0 {
				strContent := strings.TrimSpace(string(buffer[:datalen]))
				msgBodyStr = &strContent
			}

			msg = &TextMessageImpl{
				bodyStr: msgBodyStr,
				mqmd:    getmqmd,
			}
			resultMessages = append(resultMessages, msg)
		}

		if isFirstRead {
			gmo.Options ^= ibmmq.MQGMO_BROWSE_FIRST
			gmo.Options |= ibmmq.MQGMO_BROWSE_NEXT
			isFirstRead = false
		}
	}

	if err != nil {
		mqret := err.(*ibmmq.MQReturn)
		rcInt := int(mqret.MQRC)
		errCode := strconv.Itoa(rcInt)
		reason := ibmmq.MQItoString("RC", rcInt)

		jmsErr = jms20subset.CreateJMSException(reason, errCode, err)
	}

	return resultMessages, jmsErr
}

// Internal method to provide common functionality across the different types
// of receive.
func (consumer ConsumerImpl) receiveInternal(gmo *ibmmq.MQGMO) (jms20subset.Message, jms20subset.JMSException) {

	// Prepare objects to be used in receiving the message.
	var msg jms20subset.Message
	var jmsErr jms20subset.JMSException

	getmqmd := ibmmq.NewMQMD()
	buffer := make([]byte, 32768)

	// Set the GMO (get message options)
	gmo.Options |= ibmmq.MQGMO_NO_SYNCPOINT
	gmo.Options |= ibmmq.MQGMO_FAIL_IF_QUIESCING

	// Apply the selector if one has been specified in the Consumer
	err := applySelector(consumer.selector, getmqmd, gmo)
	if err != nil {
		jmsErr = jms20subset.CreateJMSException("ErrorParsingSelector", "ErrorParsingSelector", err)
		return nil, jmsErr
	}

	// Use the prepared objects to ask for a message from the queue.
	datalen, err := consumer.qObject.Get(getmqmd, gmo, buffer)

	if err == nil {

		// Message received successfully (without error).
		// Currently we only support TextMessage, so extract the content of the
		// message and populate it into a text string.
		var msgBodyStr *string

		if datalen > 0 {
			strContent := strings.TrimSpace(string(buffer[:datalen]))
			msgBodyStr = &strContent
		}

		msg = &TextMessageImpl{
			bodyStr: msgBodyStr,
			mqmd:    getmqmd,
		}

	} else {

		// Error code was returned from MQ call.
		mqret := err.(*ibmmq.MQReturn)

		if mqret.MQRC == ibmmq.MQRC_NO_MSG_AVAILABLE {

			// This isn't a real error - it's the way that MQ indicates that there
			// is no message available to be received.
			msg = nil

		} else {

			// Parse the details of the error and return it to the caller as
			// a JMSException
			rcInt := int(mqret.MQRC)
			errCode := strconv.Itoa(rcInt)
			reason := ibmmq.MQItoString("RC", rcInt)

			jmsErr = jms20subset.CreateJMSException(reason, errCode, err)
		}

	}

	return msg, jmsErr
}

// ReceiveStringBodyNoWait implements the IBM MQ logic necessary to receive a
// message from a Destination and return its body as a string.
//
// If no message is immediately available to be returned then a nil is returned.
func (consumer ConsumerImpl) ReceiveStringBodyNoWait() (*string, jms20subset.JMSException) {

	var msgBodyStrPtr *string
	var jmsErr jms20subset.JMSException

	// Get a message from the queue if one is available.
	msg, jmsErr := consumer.ReceiveNoWait()

	// If we receive a message without any errors
	if jmsErr == nil && msg != nil {

		switch msg := msg.(type) {
		case jms20subset.TextMessage:
			msgBodyStrPtr = msg.GetText()
		default:
			jmsErr = jms20subset.CreateJMSException(
				"Received message is not a TextMessage", "MQJMS6068", nil)
		}

	}

	return msgBodyStrPtr, jmsErr
}

// ReceiveStringBody implements the IBM MQ logic necessary to receive a
// message from a Destination and return its body as a string.
//
// If no message is available the method blocks up to the specified number
// of milliseconds for one to become available.
func (consumer ConsumerImpl) ReceiveStringBody(waitMillis int32) (*string, jms20subset.JMSException) {

	var msgBodyStrPtr *string
	var jmsErr jms20subset.JMSException

	// Get a message from the queue if one is available.
	msg, jmsErr := consumer.Receive(waitMillis)

	// If we receive a message without any errors
	if jmsErr == nil && msg != nil {

		switch msg := msg.(type) {
		case jms20subset.TextMessage:
			msgBodyStrPtr = msg.GetText()
		default:
			jmsErr = jms20subset.CreateJMSException(
				"Received message is not a TextMessage", "MQJMS6068", nil)
		}

	}

	return msgBodyStrPtr, jmsErr

}

// applySelector is responsible for converting the JMS style selector string
// into the relevant options on the MQI structures so that the correct messages
// are received by the application.
func applySelector(selector string, getmqmd *ibmmq.MQMD, gmo *ibmmq.MQGMO) error {

	if selector == "" {
		// No selector is provided, so nothing to do here.
		return nil
	}

	// looking for something like "JMSCorrelationID = '01020304050607'"
	clauseSplits := strings.Split(selector, "=")

	if len(clauseSplits) != 2 {
		return errors.New("Unable to parse selector " + selector)
	}

	if strings.TrimSpace(clauseSplits[0]) != "JMSCorrelationID" {
		// Currently we only support correlID selectors, so error out quickly
		// if we see anything else.
		return errors.New("Only selectors on JMSCorrelationID are currently supported.")
	}

	// Trim the value.
	value := strings.TrimSpace(clauseSplits[1])

	// Check for a quote delimited value for the selector clause.
	if strings.HasPrefix(value, "'") &&
		strings.HasSuffix(value, "'") {

		// Parse out the value, and convert it to bytes
		stringSplits := strings.Split(value, "'")
		correlIDStr := stringSplits[1]

		if correlIDStr != "" {
			correlBytes := convertStringToMQBytes(correlIDStr)
			getmqmd.CorrelId = correlBytes
		} else {
			return errors.New("No value was found for CorrelationID")
		}

	} else {
		return errors.New("Unable to parse quoted string from " + selector)
	}

	return nil
}

// Closes the JMSConsumer, releasing any resources that were allocated on
// behalf of that consumer.
func (consumer ConsumerImpl) Close() {

	if (ibmmq.MQObject{}) != consumer.qObject {
		consumer.qObject.Close(0)
	}

	return
}
