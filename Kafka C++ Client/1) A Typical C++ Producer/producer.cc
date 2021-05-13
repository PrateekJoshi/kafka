/*
 * producer.cc
 *
 *  Created on: 13-May-2021
 *      Author: prateek
 *
 *  1. Compile :
 *  g++ producer.cc  -o producer.o -lrdkafka++ -I${HOME}/.local/include -L${HOME}/.local/lib
 *
 *  2) Produce :
	$>./producer.o localhost:9092 prateek
	% Type message value and hit enter to produce message.
	Test message
	% Enqueued message (12 bytes) for topic prateek
	Kafka is awesome
	% Enqueued message (16 bytes) for topic prateek
	% Message delivered to topic prateek [0] at offset 14

	3) Verify produced messages using kafka consumer :
	>$ kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic prateek --from-beginning
 	 Test message
	Kafka is awesome
 *
 */
#include <iostream>
#include <string>
#include <cstdlib>
#include <cstdio>
#include <csignal>
#include <cstring>
#include <librdkafka/rdkafkacpp.h>


// Signal handler
// Set run atomic variable on receiving signal
static volatile sig_atomic_t run = 1;
static void sigterm (int sig) {
  run = 0;
}

// 1) Our class having method which will be used as a callback.
// 2) The dr_cb() will be called by producer as callback and will be called
//    once per message. This method from interface RdKafka::DeliveryReportCb needs to be overidden.
// 3) Here we can check the delivery status of the message which we sent earlier
class ExampleDeliveryReportCb : public RdKafka::DeliveryReportCb
{
public:
	void dr_cb(RdKafka::Message &message)
	{
		/*
		 * If message.err() is non zero the message delivery failed permanently
		 * for the message
		 */
		if( message.err() )
		{
			std::cerr<< "% Message delivery failed : "<< message.errstr() <<std::endl;
		}
		else
		{
			std::cerr << "% Message delivered to topic "
					<< message.topic_name () << " [" << message.partition ()
					<< "] at offset " << message.offset () << std::endl;
		}
	}
};


// ./producer <broker> <topic>
int main(int argc, char **argv)
{
	if ( argc != 3 )
	{
		std::cerr << "Usage: " << argv[0] << " <brokers> <topic>\n";
		exit (1);
	}

	// Store broker address and topic to produce to
	std::string brokers = argv[1];
	std::string topic = argv[2];

	// Create configuration object
	RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);

	std::string errstr;

	 /*
	  * 1) Set bootstrap broker(s) as a comma-separated list of
	 * host or host:port (default port 9092).
	 * librdkafka will use the bootstrap brokers to acquire the full
	 * set of brokers from the cluster.
	 *
	 * 2) use set() method to set various key value pair setting on the producer.
	 *
	 * 3) List of all configration is available at
	 * https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
	 */
	if( conf->set("bootstrap.servers", brokers, errstr) != RdKafka::Conf::CONF_OK )
	{
		std::cerr << errstr << std::endl;
		exit(1);
	}

	// Register signal handlers
	signal(SIGINT, sigterm);
	signal(SIGTERM, sigterm);


	/*
	 * 1) Set the delivery report callback
	 * 2) This callback will be called once per message to inform
	 * the application if delivery succeeded or failed.
	 * 3) See dr_msg_cb() above
	 * 4) The callback is only triggered from ::poll() and ::flush()
	 *
	 * IMPORTANT:
	 * Make sure the DeliveryReport instance outlives the Producer object, either by putting
	 * it on the heap or as in this case as a stack variable that will NOT go out of scope
	 * for the duration of the Producer object
	 */
	ExampleDeliveryReportCb exampleDeliveryCallback;
	if( conf->set("dr_cb", &exampleDeliveryCallback, errstr) != RdKafka::Conf::CONF_OK )
	{
		std::cerr << errstr << std::endl;
		exit(1);
	}

	// Create producer instance
	// Configuration is passed to producer
	RdKafka::Producer *producer = RdKafka::Producer::create(conf, errstr);
	if( !producer )
	{
		std::cerr << "Failed to create producer: "<< errstr <<std::endl;
		exit(1);
	}

	// Once producer is create , we can delete conf
	delete conf;

	/*
	 * Read the messages from stdin and producer to broker
	 */
	std::cout << "% Type message value and hit enter " << "to produce message."
			<< std::endl;

	for( std::string line ; run /* run will be 0 in case Signal received */ && std::getline(std::cin, line) ; )
	{
		if( line.empty() )
		{
			// call poll(0) , callbacks will be called for each message which we have producer earlier
			producer->poll(0);
			continue;
		}

		/*
		 * Produce/Send Message:
		 * 1) This is asynchronous call, on success it will only enqueue the message on the internal producer queue.
		 * 2) The actual delivery attempts to the broker are handled by background threads.
		 * 3) The previously registered delivery report callback is used to signal back to the application when the
		 * message has been delivered. ( or failed permanently after retries).
		 */
		retry:

		RdKafka::ErrorCode err = producer->produce(
													/* Topic name */
													topic,
													/*
													 * Any partition : The builtin partitioner will be used to assign
													 * the message to a topic based on the message key or random partition
													 * if key is not set
													 */
												   RdKafka::Topic::PARTITION_UA,			// partition where to send the message
												   /* Make a copy of the value */
												   RdKafka::Producer::RK_MSG_COPY,
												   /* Value */
												   const_cast<char*>(line.c_str()),
												   /* Value size */
												   line.size(),
												   /* Key */
												   NULL, 0,
												   /* Timestamp ( defaults to current time )*/
												   0,
												   /* Message headers, if any */
												   NULL,
												   /* Per message opaque value passed to delivery report */
												   NULL);

		if( err != RdKafka::ERR_NO_ERROR )
		{
			std::cerr << "% Failed to produce to topic " << topic << ": "
					<< RdKafka::err2str (err) << std::endl;

			if ( err == RdKafka::ERR__QUEUE_FULL )
			{
				/* If the internal queue is full, wait for
				 * messages to be delivered and then retry.
				 * The internal queue represents both
				 * messages to be sent and messages that have
				 * been sent or failed, awaiting their
				 * delivery report callback to be called.
				 *
				 * The internal queue is limited by the
				 * configuration property
				 * queue.buffering.max.messages */
				producer->poll (1000/*block for max 1000ms*/);
				goto retry;
			}
		}
		else
		{
			std::cerr << "% Enqueued message (" << line.size () << " bytes) "
					<< "for topic " << topic << std::endl;
		}

		/*
		 * 1) A producer application should continually serve the delivery report queue by calling poll()
		 * at frequent intervals.
		 *
		 * 2) Either put the poll call in your main loop , or in a dedicated thread , or call it after every
		 * produce() call.
		 *
		 * 3) Just make sure that poll() is still called during periods where you are not producing
		 * any messages to make sure previously produced messages have their delivery report callback served.
		 * (and any other callbacks you register)
		 */
		producer->poll(0);
	}

	/*
	 * 1) Wait for final messages to be delivered or fail.
	 * 2) flush() is an abstraction over poll() which waits for all messages to be delivered.
	 */
	std::cerr << "% Flushing final messages..." << std::endl;
	producer->flush(10*1000 /* wait for max 10 seconds */);


	// Get size of out queue. Messages waiting to be sent to OR acknowledged by broker
	if ( producer->outq_len () > 0 )
	{
		std::cerr << "% " << producer->outq_len ()
				<< " message(s) were not delivered" << std::endl;
	}

	// delete producer
	delete producer;

	return 0;
}
