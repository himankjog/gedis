package handlers

import (
	"sync"

	"github.com/codecrafters-io/redis-starter-go/app/constants"
	"github.com/codecrafters-io/redis-starter-go/app/context"
)

type CmdExecutedNotifCallbackFunc func(constants.CommandExecutedNotification) (bool, error)
type ConnClosedNotifCallbackFunc func(constants.ConnectionClosedNotification) (bool, error)
type ConnectedReplicasHeartbeatNotifCallbackFunc func(constants.ConnectedReplicaHeartbeatNotification) (bool, error)

type PublishResponse struct {
	success bool
	err     error
}

type NotificationHandler struct {
	ctx                                         *context.Context
	cmdExecutedNotifSubscription                *Subscription[constants.CommandExecutedNotification]
	connClosedNotifSubscription                 *Subscription[constants.ConnectionClosedNotification]
	connectedReplicasHeartbeatNotifSubscription *Subscription[constants.ConnectedReplicaHeartbeatNotification]
}

type Subscription[T constants.Notification] struct {
	notificationChan chan T
	subscribers      []Subscriber[T]
	subLock          sync.RWMutex
}

type Subscriber[T constants.Notification] struct {
	callbackFunc func(T) (bool, error)
	responseChan chan PublishResponse
}

func (s *Subscription[T]) GetNotificationChannel() chan T {
	return s.notificationChan
}

func (s *Subscription[T]) GetSubscribers() []Subscriber[T] {
	return s.subscribers
}

func (s *Subscription[T]) Subscribe(subscriber Subscriber[T]) (bool, error) {
	s.subLock.Lock()
	defer s.subLock.Unlock()
	s.subscribers = append(s.subscribers, subscriber)
	return true, nil
}

func (s *Subscription[T]) Publish() {
	for notification := range s.notificationChan {
		s.subLock.Lock()
		for _, subscriber := range s.subscribers {
			go subscriber.Publish(notification)
		}
		s.subLock.Unlock()
	}
}

func (s *Subscriber[T]) Publish(notification T) {
	s.callbackFunc(notification)
}

func (h *NotificationHandler) SubscribeToCmdExecutedNotification(callbackFunc CmdExecutedNotifCallbackFunc) (bool, error) {
	subscriber := Subscriber[constants.CommandExecutedNotification]{
		callbackFunc: callbackFunc,
	}
	// TODO: Add error handling
	return h.cmdExecutedNotifSubscription.Subscribe(subscriber)
}

func (h *NotificationHandler) SubscribeToConnClosedNotification(callbackFunc ConnClosedNotifCallbackFunc) (bool, error) {
	subscriber := Subscriber[constants.ConnectionClosedNotification]{
		callbackFunc: callbackFunc,
	}
	// TODO: Add error handling
	return h.connClosedNotifSubscription.Subscribe(subscriber)
}

func (h *NotificationHandler) SubscribeToConnectedReplicasHeartbeatNotification(callbackFunc ConnectedReplicasHeartbeatNotifCallbackFunc) (bool, error) {
	subscriber := Subscriber[constants.ConnectedReplicaHeartbeatNotification]{
		callbackFunc: callbackFunc,
	}
	// TODO: Add error handling
	return h.connectedReplicasHeartbeatNotifSubscription.Subscribe(subscriber)
}

func NewNotificationHandler(ctx *context.Context) *NotificationHandler {
	cmdExecutedNotifSubscription := createSubscription(ctx.CommandExecutedNotificationChan)
	connClosedNotifSubscription := createSubscription(ctx.ConnectionClosedNotificationChan)
	connectedReplicasHeartbeatNotifSubscription := createSubscription(ctx.ConnectedReplicasHeartbeatNotificationChan)

	notificationHandler := NotificationHandler{
		ctx:                          ctx,
		cmdExecutedNotifSubscription: cmdExecutedNotifSubscription,
		connClosedNotifSubscription:  connClosedNotifSubscription,
		connectedReplicasHeartbeatNotifSubscription: connectedReplicasHeartbeatNotifSubscription,
	}

	notificationHandler.startPublishing()
	return &notificationHandler
}

func createSubscription[T constants.Notification](notificationChan chan T) *Subscription[T] {
	subscription := Subscription[T]{
		notificationChan: notificationChan,
		subscribers:      make([]Subscriber[T], 0),
	}

	return &subscription
}

func (h *NotificationHandler) startPublishing() {
	go h.cmdExecutedNotifSubscription.Publish()
	go h.connClosedNotifSubscription.Publish()
	go h.connectedReplicasHeartbeatNotifSubscription.Publish()
}
