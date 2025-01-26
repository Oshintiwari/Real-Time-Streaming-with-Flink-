from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy


def main():
    # Create the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()

    # Kafka Consumer properties
    kafka_consumer_properties = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'flink-consumer-group'
    }

    # Kafka Producer properties
    kafka_producer_properties = {
        'bootstrap.servers': 'localhost:9092'
    }

    # Define Kafka Consumer
    kafka_consumer = FlinkKafkaConsumer(
        topics='input-topic',
        deserialization_schema=SimpleStringSchema(),
        properties=kafka_consumer_properties
    )

    # Define Kafka Producer
    kafka_producer = FlinkKafkaProducer(
        topic='output-topic',
        serialization_schema=SimpleStringSchema(),
        producer_config=kafka_producer_properties
    )

    # Source: Read from Kafka
    input_stream = env.add_source(kafka_consumer)

    # Transformation: Add prefix "Processed:" to each event
    processed_stream = input_stream.map(lambda value: f"Processed: {value}")

    # Sink: Write processed data to Kafka
    processed_stream.add_sink(kafka_producer)

    # Execute the Flink job
    env.execute("Real-Time Streaming with Apache Flink")


if __name__ == '__main__':
    main()
