## Introduction to Apache Kafka
Apache Kafka is a *message broker* and *stream processor*. Kafka is used to handle *real-time data feeds*.

In a data project we can differentiate between consumers and producers:
- **Consumers** are those that consume the data: web pages, micro services, apps, etc.
- **Producers** are those who supply the data to consumers.

Connecting consumers to producers directly can lead to an amorphous and hard to maintain architecture. Kafka solves this issue by becoming an intermediary that all other components connect to. Kafka works by allowing producers to send *messages* which are then pushed in real time by Kafka to consumers.

![alt text](images/what_is_kafka_and_how_does_it_work.png)

### Kafka Advantages
#### **Robustness**
Kafka replicates data across multiple brokers, making it resilient to node failures. If one broker goes down, others take over seamlessly.
- **Flexibility**
#### **Scalability and High Throughput**
Kafka is designed for handling massive data volumes. It scales horizontally by adding more broker nodes, ensuring high throughput and accommodating growing workloads.

### Basic Kafka components
#### **Message**
The basic communication abstraction used by producers and consumers in order to share information in Kafka is called a message. Messages have 3 main components:
- **Key**: used to identify the message and for additional Kafka stuff such as partitions.
- **Value**: the actual information that producers push and consumers are interested in.
- **Timestamp**: used for logging.

#### **Topic**
A topic is an abstraction of a concept. Concepts can be anything that makes sense in the context of the project, such as "sales data", "new members", "clicks on banner", etc. A producer pushes a message to a topic, which is then consumed by a consumer subscribed to that topic.

#### **Broker**
A Kafka broker is a machine (physical or virtualized) on which Kafka is running. 

#### **Cluster**
A Kafka cluster is a collection of brokers (nodes) working together.

#### **Logs**
In Kafka, logs are data segments present on a storage disk. In other words, they're physical representations of data.

Logs store messages in an ordered fashion. Kafka assigns a sequence ID in order to each new message and then stores it in logs.


Intermission: visualizing the concepts so far

Here's how a producer and a consumer would talk to the same Kafka broker to send and receive messages.

    Producer sending messages to Kafka.