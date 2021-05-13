/*
 * consumer.cc
 *
 *  Created on: 13-May-2021
 *      Author: prateek
 *
 *  Refer : https://github.com/edenhill/librdkafka/blob/master/examples/rdkafka_example.cpp
 *
 *  Example of a sync + async consumer
 *
 *  1) Compile
 *  g++ consumer.cc  -o consumer.o -lrdkafka++ -I${HOME}/.local/include -L${HOME}/.local/lib
 *
 *  2) Run producer
 *  $>./producer.o localhost:9092 prateek
 *
 *  3) Run Consumer
 *  ./consumer.o localhost:9092 prateek random  0 1
 */

#include <librdkafka/rdkafkacpp.h>
#include <cstdlib>
#include <csignal>
#include <string>
#include <iostream>
#include <cstdio>

// Signal handler
static volatile sig_atomic_t run = 1;
static void sigterm (int sig) {
  run = 0;
}

// flag to set if last message is received
static bool exit_eof = false;

// Function which will be called in consumer callback
// This function prints the info of the message received with the actual message value
void msg_consume(RdKafka::Message *message, void *opaque)
{
	const RdKafka::Headers *headers;

	switch(message->err())
	{
		case RdKafka::ERR__TIMED_OUT:
			break;
		case RdKafka::ERR_NO_ERROR:
			/* Real message */
			std::cout << "Read msg at offset "<< message->offset() << std::endl;
			if( message->key() )
			{
				std::cout<< "Key : "<< *message->key() << std::endl;
			}

			// Get message headers
			headers = message->headers();
			if( headers )
			{
				std::vector<RdKafka::Headers::Header> msg_headers = headers->get_all();	// store all headers in a vector

				// iterate through each header and print header key and value
				for( size_t i = 0 ; i < msg_headers.size(); i++ )
				{
					const RdKafka::Headers::Header header = msg_headers[i];

					if( header.value() != NULL )
					{
						printf(" Header: %s = \"%.*s\"\n", header.key().c_str(),
							   	   	   	   	   	   	   	   (int)header.value_size(),
														   (const char *)header.value());
					}
					else
					{
						printf(" Header:  %s = NULL\n", header.key().c_str());
					}
				}
			}

			// Print actual message payload and  length
			printf ("%.*s\n", static_cast<int> (message->len ()),
							  static_cast<const char*> (message->payload ()));
			break;
		case RdKafka::ERR__PARTITION_EOF:
			/* Last Message */
			if( exit_eof )
			{
				run = 0;	// stop polling the topic in broker
			}
			break;
		case RdKafka::ERR__UNKNOWN_TOPIC:
		case RdKafka::ERR__UNKNOWN_PARTITION:
			std::cerr << "Consume failed: " << message->errstr () << std::endl;
			run = 0;
			break;

		default:
			/* Errors */
			std::cerr << "Consume failed: " << message->errstr () << std::endl;
			run = 0;
	}
}

/*
 * 1) Consumer callback class
 * 2) The consume callback is used with RdKafka::Consumer::consume_callback() methods and will be
 * called for each consumed message.
   3) The callback interface is optional but provides increased performance.
   4) In opaque parameter you can find pointer to any structure or object and that will be passed as is.
 */
class ExampleConsumerCb : public RdKafka::ConsumeCb
{
public:
	void consume_cb(RdKafka::Message &msg, void *opaque)
	{
		 msg_consume(&msg, opaque);
	}
};


// ./consumer.o <broker> <topic> <partition> <offset> <Use consumer callback flag : 1 or 0  >
int main(int argc, char **argv)
{
	// Register signal handler
	signal(SIGINT, sigterm);
	signal(SIGTERM, sigterm);

	/* Create Configuration objects */
	RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);		// consumer configuration
	RdKafka::Conf *tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);		// topic configuration ( starting with topic.* )

	if( argc != 6 )
	{
		std::cout <<"Usage : ./consumer.o <brokers> <topic> <partition> <offset> <Use consumer callback flag : 1 or 0  >"<<std::endl;
		exit(1);
	}

	std::string errstr;
	std::string brokers = argv[1];
	std::string topic = argv[2];
	int32_t partition = RdKafka::Topic::PARTITION_UA;
	uint32_t offset = atoi(argv[4]);
	uint32_t isUseCallback = atoi(argv[5]);

	 // Get partition to use
	 if (strcmp(argv[3], "random"))
	 {
		 ;// default
	 }
	 else
	 {
		 partition = atoi(argv[3]);
	 }

	/* Set Configuration */
	conf->set("metadata.broker.list", brokers, errstr);
	//Emit RD_KAFKA_RESP_ERR__PARTITION_EOF event whenever the consumer reaches the end of a partition.
	conf->set("enable.partition.eof", "true", errstr);


	/*
	 * Create consumer using accumulated global configuration.
	 */
	RdKafka::Consumer *consumer = RdKafka::Consumer::create(conf, errstr);
	if( !consumer )
	{
		std::cerr << "Failed to create consumer: " << errstr << std::endl;
		exit(1);
	}
	std::cout << "% Created consumer " << consumer->name() << std::endl;

	/* Create topic handle. */
	RdKafka::Topic *topic_handle = RdKafka::Topic::create (consumer, topic, tconf, errstr);
	if ( !topic_handle )
	{
		std::cerr << "Failed to create topic: " << errstr << std::endl;
		exit (1);
	}

	/*
	 * Start consumer for topic + partition at start offset
	 * Fetch message from broker and place it in inyternal queue
	 */
	RdKafka::ErrorCode resp = consumer->start(topic_handle, partition, offset);
	if ( resp != RdKafka::ERR_NO_ERROR )
	{
		std::cerr << "Failed to start consumer: " << RdKafka::err2str (resp) << std::endl;
		exit (1);
	}

	ExampleConsumerCb ex_consume_cb;

	/* Start consuming messages from internal queue */
	while( run )
	{
		// Whether to use callback consumer or without
		if( isUseCallback )
		{
			// Non blocking
			consumer->consume_callback(topic_handle, partition, 1000, &ex_consume_cb, &isUseCallback);
		}
		else
		{
			// blocking
			RdKafka::Message *msg = consumer->consume(topic_handle, partition, 1000);
			msg_consume(msg, NULL);
			delete msg;
		}

		/*
		 * 1) A consumer application should continually serve the delivery report queue by calling poll()
		 * at frequent intervals.
		 *
		 * 2) Either put the poll call in your main loop , or in a dedicated thread , or call it after every
		 * consume() call.
		 *
		 * 3) Just make sure that poll() is still called during periods where you are not consuming
		 * any messages to make sure previously produced messages have their delivery report callback served.
		 * (and any other callbacks you register)
		 */
		consumer->poll(0);
	}


	 /*
	 * Stop consumer
	 */
	consumer->stop (topic_handle, partition);
	consumer->poll (1000);
	delete topic_handle;
	delete consumer;

	/*
	 * Wait for RdKafka to decommission.
	 * This is not strictly needed (when check outq_len() above), but
	 * allows RdKafka to clean up all its resources before the application
	 * exits so that memory profilers such as valgrind wont complain about
	 * memory leaks.
	 */
	RdKafka::wait_destroyed (5000);

	return 0;
}

























