package main

import (
	"fmt"
	"os"
	"os/signal"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

var Kafka KafkaPbusub = KafkaPbusub{}

//KafkaPbusub kafka implementation of flow.ai pubsub
type KafkaPbusub struct {
	consumer      kafka.Consumer
	producer      kafka.Producer
	subscriptions map[string]func(msg ...interface{})
}

func (k *KafkaPbusub) Init() {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": "kafka-new.flow.svc.cluster.local:9092",
		"group.id":          "interactions-go-rdkafka",
		// "bootstrap.servers":       "b-1.flow-test.q0ut0v.c10.kafka.us-west-2.amazonaws.com:9092,b-2.flow-test.q0ut0v.c10.kafka.us-west-2.amazonaws.com:9092,b-3.flow-test.q0ut0v.c10.kafka.us-west-2.amazonaws.com:9092",
		// "group.id":                "test-consumer-group",
		"session.timeout.ms":      10000,
		"auto.commit.interval.ms": 400,
		"socket.keepalive.enable": true,
		"heartbeat.interval.ms":   800,
		"socket.timeout.ms":       30000,
		"enable.auto.commit":      true,
		"log_level":               4,
		"debug":                   "broker"}
	c, err := kafka.NewConsumer(configMap)
	if err == nil {
		k.consumer = *c
		fmt.Println("Kafka connected")
	} else {
		fmt.Println("Kafka Consumer is not initialized, you probably didn't run bootstrapper")
		panic(err)
	}
	p, pErr := kafka.NewProducer(configMap)
	if pErr != nil {
		fmt.Println("Kafka Producer is not initialized, you probably didn't run bootstrapper")
		panic(err)
	}
	k.producer = *p

	// Delivery report handler for produced messages
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	keys := reflect.ValueOf(k.subscriptions).MapKeys()
	topics := []string{}
	for k := range keys {
		topics = append(topics, keys[k].Interface().(string))
	}

	fmt.Println("***************************")
	fmt.Printf("topics: %v", topics)

	//TODO add rebalance options
	subErr := k.consumer.SubscribeTopics(topics, nil)
	if subErr != nil {
		fmt.Println("errrorrrrrrrrrrrrrrrr")
		fmt.Printf("%v", subErr)
	}
}

//Init inits kafka connection
func consume(k *KafkaPbusub, wg *sync.WaitGroup) {

	//TODO add params reading
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	keys := reflect.ValueOf(k.subscriptions).MapKeys()
	topics := []string{}
	for k := range keys {
		topics = append(topics, keys[k].Interface().(string))
	}
	run := true

	for run == true {
		select {
		//TODO move to Implementation of closer interface
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false
		default:
			ev := k.consumer.Poll(10)
			if ev == nil {
				continue
			}

			fmt.Println("is not nil")

			switch e := ev.(type) {
			case *kafka.Message:
				topic := e.TopicPartition.Topic
				fmt.Printf("%% Message on %s:\n%s\n", *topic, string(e.Value))

				if k.subscriptions[*topic] != nil {
					go k.subscriptions[*topic](e)
				} else {
					for _, subsTopic := range topics {
						if strings.Contains(subsTopic, "^") || strings.Contains(subsTopic, "$") || strings.Contains(subsTopic, "*") {
							reg := regexp.MustCompile(subsTopic)
							if reg.MatchString(*topic) {
								go k.subscriptions[subsTopic](e)
								//caching for future hit
								k.subscriptions[*topic] = k.subscriptions[subsTopic]

							}
						}
					}
				}
				if string(e.Value) == "999" {
					run = false
				}

			case kafka.Error:
				// Errors should generally be considered
				// informational, the client will try to
				// automatically recover.
				// But in this example we choose to terminate
				// the application if all brokers are down.
				fmt.Fprintf(os.Stderr, "%% Error: %v: %v\n", e.Code(), e)
				if e.Code() == kafka.ErrAllBrokersDown {
					run = false
				}
			default:
				fmt.Printf("Ignored **********%v\n", e)
			}
		}
	}

	fmt.Printf("Closing consumer\n")

	k.consumer.Close()
	fmt.Println("exit due to stopped consumption")
	defer wg.Done()
}

//Publish publishing message
func (k *KafkaPbusub) Publish(topic string, payload string) map[string]interface{} {
	err := k.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          []byte(payload),
	}, nil)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("%v,%v,%q", "sumbitted message to topic and val : ", topic, payload)
	//k.producer.Flush(10 * 1000)
	return nil
}

//Subscribe subscribe kakfa to a topic
func (k *KafkaPbusub) Subscribe(topic string, handler func(msg ...interface{})) {
	if k.subscriptions == nil {
		k.subscriptions = make(map[string]func(msg ...interface{}))
	}
	//TODO add validation for function
	k.subscriptions[topic] = handler
}

const componentName string = "KAFKAPUBSUB"

const SUBSCRIBE_TIMEOUT_SEC time.Duration = 10
const UNSUBSCRIBE_TIMEOUT_SEC time.Duration = 200

var startTime []time.Time

var diff []int

func main() {
	kafkaPubsub := &Kafka

	kafkaPubsub.Subscribe("test-topic-one", handler)
	kafkaPubsub.Init()

	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		if i == 0 {
			go consume(kafkaPubsub, &wg)
		} else {
			go heartBeat(kafkaPubsub, &wg)
		}

	}
	// wg.Add(1)

	// go heartBeat(kafkaPubsub, &wg)

	wg.Wait()

	fmt.Println(startTime)

	fmt.Println("difference array")
	fmt.Println(diff)
	sum := 0

	for j := 0; j < len(diff); j++ {
		sum += (diff[j])
	}

	avg := (float64(sum)) / (float64(len(diff)))
	fmt.Println("Sum = ", sum, "\nAverage = ", avg)
	//pubsub.Unsubscribe("topic/test")
}
func handler(e ...interface{}) {
	strindex := string(e[0].(*kafka.Message).Value)
	fmt.Printf("************received message:%v", strindex)
	fmt.Println()

	index, _ := strconv.Atoi(strindex)
	fmt.Printf("message Received:%v", index)
	diff = append(diff, int(time.Now().Sub(startTime[index])))
}

// func subscription(pubsub *KafkaPbusub, wg *sync.WaitGroup) {
// 	pubsub.Subscribe("topic/test", handler)
// }

func heartBeat(pubsub *KafkaPbusub, wg *sync.WaitGroup) {
	i := 0
	for range time.Tick(time.Second * 2) {
		if i == 1000 {
			break
		}
		startTime = append(startTime, time.Now())

		pubsub.Publish("test-topic-one", fmt.Sprintf("%v", i))

		fmt.Println()
		fmt.Printf("data sent %v", fmt.Sprintf("%v", i))
		i++
	}

	defer wg.Done()
}
