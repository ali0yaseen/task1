1) Start Zookeeper (Terminal 1)

Zookeeper is required to manage Kafka brokers.

bin\windows\zookeeper-server-start.bat config\zookeeper.properties


Keep this terminal running.

2) Start Kafka Broker (Terminal 2)

After starting Zookeeper, start the Kafka broker.

bin\windows\kafka-server-start.bat config\server.properties


Keep this terminal running.

3) Create Kafka Topic

A topic named first-topic is created to stream inventory data.

bin\windows\kafka-topics.bat --create ^
--topic first-topic ^
--bootstrap-server localhost:9092 ^
--partitions 1 ^
--replication-factor 1

4) List Available Topics

Verify that the topic was created successfully.

bin\windows\kafka-topics.bat --list --bootstrap-server localhost:9092


Expected output:

first-topic

5) Start Kafka Console Consumer (Test) – Terminal 3

Used to verify that messages are received correctly.

bin\windows\kafka-console-consumer.bat ^
--bootstrap-server localhost:9092 ^
--topic first-topic ^
--from-beginning


Keep this terminal open to monitor incoming messages.

6) Start Kafka Console Producer (Test) – Terminal 4

Used to manually send test messages.

bin\windows\kafka-console-producer.bat ^
--broker-list localhost:9092 ^
--topic first-topic


Type any message and press Enter.
The message should appear in the Consumer terminal.

7) Project Kafka Producer (CSV → Kafka)

In this repository:

CSV data file:

data/Items-bigdata.csv


Kafka producer script:

kafka/producer.py


The producer reads inventory data from the CSV file and sends each row as a JSON message to Kafka.

Run the Producer

From the project root directory:

cd C:\Retail-Sales-Inventory-Analytics
python kafka\producer.py


Expected behavior:

Each CSV row is sent to Kafka

Messages appear in the Kafka Consumer terminal in real time