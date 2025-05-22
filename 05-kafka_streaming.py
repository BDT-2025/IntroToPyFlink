import os

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.common.typeinfo import Types
import json

def main():
    # 1. Setup environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # Download the Kafka connector jar file from here https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/connectors/datastream/kafka/
    # and place it in the same directory as this script

    jar_absolute_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'flink-sql-connector-kafka-3.3.0-1.20.jar'))
    env.add_jars(f"file://{jar_absolute_path}")



    # 2. Configure Kafka consumer
    kafka_props = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'log-consumer-group',
        'auto.offset.reset': 'earliest'
    }

    kafka_consumer = FlinkKafkaConsumer(
        topics='logs',
        deserialization_schema=SimpleStringSchema(),
        properties=kafka_props
    )

    # 3. Add Kafka source
    stream = env.add_source(kafka_consumer).name("Kafka Source")

    # 4. Parse JSON and extract log level
    parsed_stream = stream \
        .map(lambda x: json.loads(x), output_type=Types.MAP(Types.STRING(), Types.STRING())) \
        .map(lambda d: d.get("level", "UNKNOWN"), output_type=Types.STRING())

    # 5. Count by log level using keyBy + reduce
    counts = parsed_stream \
        .map(lambda level: (level, 1), output_type=Types.TUPLE([Types.STRING(), Types.INT()])) \
        .key_by(lambda x: x[0]) \
        .reduce(lambda a, b: (a[0], a[1] + b[1]))

    # 6. Print the result
    counts.print()

    # 7. Execute
    env.execute("Kafka Log Level Counter")

if __name__ == "__main__":
    main()