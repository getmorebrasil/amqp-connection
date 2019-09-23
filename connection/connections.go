package amqp_connection

import (
	"github.com/streadway/amqp"
	"log"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func Connect(conString string) (connection *amqp.Connection, conChannel *amqp.Channel) {
	conn, err := amqp.Dial(conString)
	failOnError(err, "Failed to connect to RabbitMQ")

	ch, err := conn.Channel()
	failOnError(err, "Failed to opem a channel")

	return conn, ch
}

func CreateQueue(name string, durable bool, del_when_unused bool, exclusive bool, noWait bool, arguments amqp.Table, channel *amqp.Channel) (queue amqp.Queue) {
	q, err := channel.QueueDeclare(
		name,            //name
		durable,         //durable
		del_when_unused, // delete when unused
		exclusive,       // exlusive
		noWait,          //no-wait
		nil,             //arguments
	)
	failOnError(err, "Failed to declare a queue")
	return q
}

func ConsumeQueue(name string, autoAck bool, exclusive bool, noLocal bool, noWait bool, args amqp.Table, ch *amqp.Channel) (messages <-chan amqp.Delivery) {
	msgs, err := ch.Consume(
		name,      //queue
		"",        //consumer
		autoAck,   //auto-ack
		exclusive, //exclusive
		noLocal,   //no-local
		noWait,    //no-wait
		args,      //args
	)
	failOnError(err, "Failed to register a consumer")
	return msgs
}

func PublishQueue(exchange string, routingKey string, mandatory bool, immediate bool, options amqp.Publishing, ch *amqp.Channel) {
	err := ch.Publish(
		exchange,   // exchange
		routingKey, // routing key
		mandatory,  // mandatory
		immediate,  // immediate
		options)
	failOnError(err, "Failed to register a consumer")
}
