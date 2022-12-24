find_package(RdKafka CONFIG REQUIRED)

add_executable(create_topic create_topic.cpp)
target_link_libraries(create_topic PRIVATE RdKafka::rdkafka RdKafka::rdkafka++)
