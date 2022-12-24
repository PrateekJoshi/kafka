#include "librdkafka/rdkafkacpp.h"
#include "librdkafka/rdkafka.h"
#include <iostream>
#include <memory>
#include <vector>
#include <thread>

struct TopicConf
{
    uint32_t partitionCount;
    uint32_t replicationfactor;
    uint32_t minIsr;
    uint64_t maxMessageBytes;
    uint64_t maxRetensionMs;
};

void createTopics(const std::vector<std::string> &vTopics, const TopicConf &topicConf, const uint32_t timeoutMs = 1000)
{
    char errstr[512] = {};
    const auto BROKER = "localhost:9092";

    // allocate memory for conf object
    rd_kafka_conf_t *pConf{rd_kafka_conf_new()};
    auto err = RD_KAFKA_CONF_OK;

    // set bootstrap server config property
    err = rd_kafka_conf_set(pConf, "bootstrap.servers", BROKER, errstr, sizeof(errstr));
    if (err != RD_KAFKA_CONF_OK)
    {
        throw std::runtime_error{errstr};
    }

    // Creates a new Kafka handle and starts its operation according to the specified type (RD_KAFKA_CONSUMER or RD_KAFKA_PRODUCER)
    auto *pKafkaHandle = rd_kafka_new(RD_KAFKA_PRODUCER, pConf, errstr, sizeof(errstr));
    if (!pKafkaHandle)
    {
        throw std::runtime_error{errstr};
    }

    // Create a new message queue
    auto *pKafkaMsgQueue = rd_kafka_queue_new(pKafkaHandle);

    const auto topicCount = vTopics.size();

    // allocate memory for list of new topic to be created
    auto **pNewTopics = (rd_kafka_NewTopic_t **)malloc(topicCount * sizeof(rd_kafka_NewTopic_t *));
    // iterate through each topic handle and set parition count and replcation factor
    for (auto i = 0; i < topicCount; i++)
    {
        pNewTopics[i] = rd_kafka_NewTopic_new(vTopics[i].c_str(), topicConf.partitionCount, topicConf.replicationfactor, errstr, sizeof(errstr));
        if (!pNewTopics[i])
        {
            throw std::runtime_error{errstr};
        }
    }

    // create new admin option object
    auto *pAdminOptions = rd_kafka_AdminOptions_new(pKafkaHandle, RD_KAFKA_ADMIN_OP_CREATETOPICS);
    if (!pAdminOptions)
    {
        throw std::runtime_error{"allocation failed for rd_kafka_AdminOptions_new()"};
    }

    // set opertaion timeout
    auto opr_err = rd_kafka_AdminOptions_set_operation_timeout(pAdminOptions, timeoutMs, errstr, sizeof(errstr));
    if (opr_err != RD_KAFKA_RESP_ERR_NO_ERROR)
    {
        throw std::runtime_error{errstr};
    }

    // Set topic specific config
    for (auto i = 0; i < topicCount; i++)
    {
        opr_err = rd_kafka_NewTopic_set_config(pNewTopics[i], "retention.ms", std::to_string(topicConf.maxRetensionMs).c_str());
        if (opr_err != RD_KAFKA_RESP_ERR_NO_ERROR)
        {
            throw std::runtime_error{errstr};
        }

        opr_err = rd_kafka_NewTopic_set_config(pNewTopics[i], "max.message.bytes", std::to_string(topicConf.maxMessageBytes).c_str());
        if (opr_err != RD_KAFKA_RESP_ERR_NO_ERROR)
        {
            throw std::runtime_error{errstr};
        }

        opr_err = rd_kafka_NewTopic_set_config(pNewTopics[i], "min.insync.replicas", std::to_string(topicConf.minIsr).c_str());
        if (opr_err != RD_KAFKA_RESP_ERR_NO_ERROR)
        {
            throw std::runtime_error{errstr};
        }
    }

    // all set, Create the topics
    rd_kafka_CreateTopics(pKafkaHandle, pNewTopics, topicCount, pAdminOptions, pKafkaMsgQueue);

    // poll queue for incoming event
    auto pKafkaEvent = rd_kafka_queue_poll(pKafkaMsgQueue, timeoutMs);

    // get create topic result event
    const auto *pCreateTopicResult = rd_kafka_event_CreateTopics_result(pKafkaEvent);
    if (pCreateTopicResult == nullptr)
    {
        throw std::runtime_error{"received another event , expecting create topic result event"};
    }

    // Get result count
    uint64_t resultCount{};

    // Get an array of topic result
    const auto **pTopicResults = rd_kafka_CreateTopics_result_topics(pCreateTopicResult, &resultCount);
    if (topicCount != resultCount)
    {
        throw std::runtime_error{"Expected result for " + std::to_string(topicCount) + " but received " + std::to_string(resultCount)};
    }

    // Check err for each topic
    for (auto i = 0; i < topicCount; i++)
    {
        if (rd_kafka_topic_result_error(pTopicResults[i]))
        {
            throw std::runtime_error{rd_kafka_topic_result_error_string(pTopicResults[i])};
        }
    }

    auto cleanup = [&]()
    {
        if (pKafkaEvent)
            rd_kafka_event_destroy(pKafkaEvent);
        if (pAdminOptions)
            rd_kafka_AdminOptions_destroy(pAdminOptions);
        if (topicCount)
        {
            for (auto i = 0; i < topicCount; i++)
            {
                if (pNewTopics[i])
                    rd_kafka_NewTopic_destroy(pNewTopics[i]);
            }
        }
        if (pNewTopics)
            free(pNewTopics);
        if (pKafkaMsgQueue)
            rd_kafka_queue_destroy(pKafkaMsgQueue);
        if (pKafkaHandle)
            rd_kafka_destroy(pKafkaHandle);
    };

    // perform cleanup
    cleanup();
}

void deleteTopics(const std::vector<std::string> &vTopics, const uint32_t timeoutMs = 1000)
{
    char errstr[512] = {};
    const auto BROKER = "localhost:9092";

    // allocate memory for conf object
    rd_kafka_conf_t *pConf{rd_kafka_conf_new()};
    auto err = RD_KAFKA_CONF_OK;

    // set bootstrap server config property
    err = rd_kafka_conf_set(pConf, "bootstrap.servers", BROKER, errstr, sizeof(errstr));
    if (err != RD_KAFKA_CONF_OK)
    {
        throw std::runtime_error{errstr};
    }

    // Creates a new Kafka handle and starts its operation according to the specified type (RD_KAFKA_CONSUMER or RD_KAFKA_PRODUCER)
    auto *pKafkaHandle = rd_kafka_new(RD_KAFKA_PRODUCER, pConf, errstr, sizeof(errstr));
    if (!pKafkaHandle)
    {
        throw std::runtime_error{errstr};
    }

    // Create a new message queue
    auto *pKafkaMsgQueue = rd_kafka_queue_new(pKafkaHandle);

    const auto topicCount = vTopics.size();

    // allocate memory for list of new topic to be created
    auto **pDelTopics = (rd_kafka_DeleteTopic_t **)malloc(topicCount * sizeof(rd_kafka_DeleteTopic_t *));
    // iterate through each topic handle and set parition count and replcation factor
    for (auto i = 0; i < topicCount; i++)
    {
        pDelTopics[i] = rd_kafka_DeleteTopic_new(vTopics[i].c_str());
        if (!pDelTopics[i])
        {
            throw std::runtime_error{"allocation failed for rd_kafka_DeleteTopic_new()"};
        }
    }

    // create new admin option object
    auto *pAdminOptions = rd_kafka_AdminOptions_new(pKafkaHandle, RD_KAFKA_ADMIN_OP_DELETETOPICS);
    if (!pAdminOptions)
    {
        throw std::runtime_error{"allocation failed for rd_kafka_AdminOptions_new()"};
    }

    // set opertaion timeout
    auto opr_err = rd_kafka_AdminOptions_set_operation_timeout(pAdminOptions, timeoutMs, errstr, sizeof(errstr));
    if (opr_err != RD_KAFKA_RESP_ERR_NO_ERROR)
    {
        throw std::runtime_error{errstr};
    }

    // all set, delete the topics
    rd_kafka_DeleteTopics(pKafkaHandle, pDelTopics, topicCount, pAdminOptions, pKafkaMsgQueue);

    // poll queue for incoming event
    auto pKafkaEvent = rd_kafka_queue_poll(pKafkaMsgQueue, timeoutMs);

    // get create topic result event
    const auto *pCreateTopicResult = rd_kafka_event_DeleteTopics_result(pKafkaEvent);
    if (pCreateTopicResult == nullptr)
    {
        throw std::runtime_error{"received another event , expecting delete topic result event"};
    }

    // Get result count
    uint64_t resultCount{};

    // Get an array of topic result
    const auto **pTopicResults = rd_kafka_DeleteTopics_result_topics(pCreateTopicResult, &resultCount);
    if (topicCount != resultCount)
    {
        throw std::runtime_error{"Expected result for " + std::to_string(topicCount) + " but received " + std::to_string(resultCount)};
    }

    // Check err for each topic
    for (auto i = 0; i < topicCount; i++)
    {
        auto respErr = rd_kafka_topic_result_error(pTopicResults[i]);
        // if error occured and error is not unknown topic or parition ( ie topic already doesnt exists)
        if (respErr && respErr != RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART)
        {
            throw std::runtime_error{rd_kafka_topic_result_error_string(pTopicResults[i])};
        }
    }

    auto cleanup = [&]()
    {
        if (pKafkaEvent)
            rd_kafka_event_destroy(pKafkaEvent);
        if (pAdminOptions)
            rd_kafka_AdminOptions_destroy(pAdminOptions);
        if (topicCount)
        {
            for (auto i = 0; i < topicCount; i++)
            {
                if (pDelTopics[i])
                    rd_kafka_DeleteTopic_destroy(pDelTopics[i]);
            }
        }
        if (pDelTopics)
            free(pDelTopics);
        if (pKafkaMsgQueue)
            rd_kafka_queue_destroy(pKafkaMsgQueue);
        if (pKafkaHandle)
            rd_kafka_destroy(pKafkaHandle);
    };

    // perform cleanup
    cleanup();
}

void GetTopicList(std::vector<std::string> &vTopicList, const uint32_t timeoutMs)
{
    char errstr[512] = {};
    const auto BROKER = "localhost:9092";

    // allocate memory for conf object
    rd_kafka_conf_t *pConf{rd_kafka_conf_new()};
    auto err = RD_KAFKA_CONF_OK;

    // set bootstrap server config property
    err = rd_kafka_conf_set(pConf, "bootstrap.servers", BROKER, errstr, sizeof(errstr));
    if (err != RD_KAFKA_CONF_OK)
    {
        throw std::runtime_error{errstr};
    }

    // Creates a new Kafka handle and starts its operation according to the specified type (RD_KAFKA_CONSUMER or RD_KAFKA_PRODUCER)
    auto *pKafkaHandle = rd_kafka_new(RD_KAFKA_PRODUCER, pConf, errstr, sizeof(errstr));
    if (!pKafkaHandle)
    {
        throw std::runtime_error{errstr};
    }

    const rd_kafka_metadata_t* pMetdata{nullptr};
    auto respErr = rd_kafka_metadata(pKafkaHandle, 1, nullptr, &pMetdata, timeoutMs);
    if( respErr )
    {
        throw std::runtime_error{"rd_kafka_metadata failed"};
    }

    // fill topic names
    for( auto i = 0 ; i < pMetdata->topic_cnt ; i++ )
    {
        vTopicList.emplace_back(pMetdata->topics[i].topic);
    }

    auto cleanup = [&](){
        if( pMetdata ) rd_kafka_metadata_destroy(pMetdata);
        if( pKafkaHandle ) rd_kafka_destroy(pKafkaHandle);
    };

    cleanup();
}

int main(int argc, char const *argv[])
{
    std::vector<std::string> vTopic{"test_topic_1", "test_topic_2"};
    TopicConf topicConf{};
    topicConf.maxMessageBytes = 1000000;
    topicConf.maxRetensionMs = 1000 * 60 * 60; // 1 hr
    topicConf.minIsr = 2;
    topicConf.partitionCount = 3;
    topicConf.replicationfactor = 1;

    // start from clean slate : delete topic
    deleteTopics(vTopic,1000);

    // create topic
    createTopics(vTopic, topicConf);

    // sleep for 5 seconds to topic to get created
    // using namespace std::literals;
    // std::this_thread::sleep_for(5s);

    // fetch list of topics
    std::vector<std::string> outTopics{};
    GetTopicList(outTopics,2000);
    for( const auto& topic : outTopics )
    {
        std::cout<<topic<<std::endl;
    }

    // delete topic
    deleteTopics(vTopic);
    return 0;
}
