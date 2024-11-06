package earmq

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/eris-apple/ealogger"
	"github.com/eris-apple/eautils/url"
	"github.com/streadway/amqp"
)

type Connection = amqp.Connection
type Channel = amqp.Channel
type Queue = amqp.Queue
type Delivery = amqp.Delivery

// ConnectConfig — configuration of the rabbitmq connection.
type ConnectConfig struct {
	Host     string
	Path     string
	User     string
	Password string
}

// Service — rabbitmq service.
type Service struct {
	l *ealogger.Logger
	c *ConnectConfig

	conn *Connection

	channels map[string]*Channel
	queues   map[string]*Queue

	traceName string
}

// Init — initializing the connection with rabbitmq.
func (s *Service) Init() error {
	URL := url.NewURLConnectionString("amqp", s.c.Host, s.c.Path, "", s.c.User, s.c.Password)

	conn, err := amqp.Dial(URL)
	if err != nil {
		s.l.ErrorT(s.traceName, "Failed to connect to RabbitMQ", err)
		return err
	}

	s.l.InfoT(s.traceName, "Successfully connected to RabbitMQ")

	s.conn = conn
	return nil
}

// Disconnect — disconnecting from rabbitmq.
func (s *Service) Disconnect() error {
	if s.conn == nil {
		return errors.New("connection is not initialized")
	}
	if err := s.conn.Close(); err != nil {
		s.l.ErrorT(s.traceName, "Failed to disconnect from RabbitMQ", err)
		return err
	}

	s.l.InfoT(s.traceName, "Successfully disconnected to RabbitMQ")
	s.conn = nil
	return nil
}

// GetClient — returns the rabbitmq Connection client.
func (s *Service) GetClient() *Connection {
	return s.conn
}

// CreateChannel — creates a Channel with the specified name.
func (s *Service) CreateChannel(name string) error {
	if s.conn == nil {
		return errors.New("connection is not initialized")
	}
	if s.channels[name] != nil {
		s.l.ErrorT(s.traceName, "A channel with that name already exists", name)
		return errors.New("the channel already exists")
	}

	ch, err := s.conn.Channel()
	if err != nil {
		s.l.ErrorT(s.traceName, "Failed to create a channel", err)
		return err
	}

	s.l.DebugT(s.traceName, "The channel has been successfully created")
	s.channels[name] = ch
	return nil
}

// CloseChannel — closes the requested Channel.
func (s *Service) CloseChannel(name string) error {
	if s.conn == nil {
		return errors.New("connection is not initialized")
	}
	if _, err := s.GetChannel(name); err != nil {
		return err
	}

	if err := s.channels[name].Close(); err != nil {
		s.l.ErrorT(s.traceName, "The channel could not be closed", name, err)
		return err
	}

	s.l.InfoT(s.traceName, "The channel has been successfully closed", name)
	return nil
}

// GetChannel — returns the instance of the requested Channel.
func (s *Service) GetChannel(name string) (*Channel, error) {
	if s.conn == nil {
		return nil, errors.New("connection is not initialized")
	}
	if s.channels[name] == nil {
		s.l.ErrorT(s.traceName, "A channel with that name was not found", name)
		return nil, errors.New("the channel was not found")
	}

	return s.channels[name], nil
}

// CreateQueue — creates a new Queue based on the specified Queue and Channel names.
func (s *Service) CreateQueue(name string, channelName string, durable bool) error {
	if s.conn == nil {
		return errors.New("connection is not initialized")
	}
	if _, err := s.GetChannel(channelName); err != nil {
		return err
	}

	if s.queues[name] != nil {
		s.l.ErrorT(s.traceName, "A queue with that name already exist", name, channelName)
		return errors.New("the queue already exists")
	}

	q, err := s.channels[channelName].QueueDeclare(
		name,
		durable,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		s.l.ErrorT(s.traceName, "Failed to create a queue", err)
		return err
	}

	s.l.DebugT(s.traceName, "The queue has been successfully created")
	s.queues[name] = &q
	return nil
}

// GetQueue — returns the instance of the requested Queue.
func (s *Service) GetQueue(name string) (*Queue, error) {
	if s.conn == nil {
		return nil, errors.New("connection is not initialized")
	}
	if s.queues[name] == nil {
		s.l.ErrorT(s.traceName, "A queue with this name was not found", name)
		return nil, errors.New("the queue was not found")
	}

	return s.queues[name], nil
}

// PublishJSON — adds a JSON message to the Queue.
func (s *Service) PublishJSON(name, channelName, exchange string, data interface{}) error {
	if s.conn == nil {
		return errors.New("connection is not initialized")
	}
	if _, err := s.GetQueue(name); err != nil {
		return err
	}
	if _, err := s.GetChannel(channelName); err != nil {
		return err
	}

	body, err := json.Marshal(data)
	if err != nil {
		s.l.ErrorT(s.traceName, "Cannot marshal data", name, channelName, err)
		return err
	}

	if err := s.channels[channelName].Publish(
		exchange,
		name,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        []byte(body),
		}); err != nil {
		s.l.ErrorT(s.traceName, "Failed to publish a message", err)
		return err
	}

	s.l.InfoT(s.traceName, "The JSON message was successfully published", name, channelName)
	return nil
}

// PublishString — adds a string message to the Queue.
func (s *Service) PublishString(name, channelName, exchange string, data string) error {
	if s.conn == nil {
		return errors.New("connection is not initialized")
	}
	if _, err := s.GetQueue(name); err != nil {
		return err
	}
	if _, err := s.GetChannel(channelName); err != nil {
		return err
	}

	if err := s.channels[channelName].Publish(
		exchange,
		name,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(data),
		}); err != nil {
		s.l.ErrorT(s.traceName, "Failed to publish a message", err)
		return err
	}

	s.l.InfoT(s.traceName, "The message was successfully published", name, channelName)
	return nil
}

// AddConsumer — adds a message handler from the Queue.
func (s *Service) AddConsumer(queueName string, channelName string, taskLimitCount int, consumer func(d Delivery)) error {
	if s.conn == nil {
		return errors.New("connection is not initialized")
	}
	if _, err := s.GetQueue(queueName); err != nil {
		return err
	}
	if _, err := s.GetChannel(channelName); err != nil {
		return err
	}

	msgs, err := s.channels[channelName].Consume(
		s.queues[queueName].Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		s.l.ErrorT(s.traceName, "Error consuming message", queueName, channelName, err)
		return err
	}

	taskLimiter := make(chan struct{}, taskLimitCount)

	go func() {
		for d := range msgs {
			taskLimiter <- struct{}{}

			go func(d Delivery) {
				defer func() { <-taskLimiter }()
				consumer(d)
			}(d)
		}
	}()

	s.l.DebugT(s.traceName, "The consumer has been successfully added")
	return nil
}

// NewService — returns the Service instance.
func NewService(l *ealogger.Logger, c *ConnectConfig, traceName string) *Service {
	return &Service{
		l: l,
		c: c,

		channels: make(map[string]*Channel),
		queues:   make(map[string]*Queue),

		traceName: fmt.Sprintf("[%s_RabbitMQService]", traceName),
	}
}
