import os

from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.table.expressions import col

env_settings = EnvironmentSettings.in_streaming_mode()
table_env = StreamTableEnvironment.create(environment_settings=env_settings)

jar_absolute_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'flink-sql-connector-kafka-3.3.0-1.20.jar'))

table_env.get_config().get_configuration().set_string(
    "pipeline.jars",
    f"file://{jar_absolute_path}"
)

# Define Kafka source table
table_env.execute_sql("""
CREATE TABLE input_table (
    word_line STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'sentences',
  'properties.bootstrap.servers' = 'localhost:9092',
  'properties.group.id' = 'testGroup',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'raw',
  'raw.charset' = 'UTF-8'
)
""")

# # Define Kafka sink table
# table_env.execute_sql("""
# CREATE TABLE output_table (
#     word STRING,
#     word_count BIGINT
# ) WITH (
#   'connector' = 'kafka',
#   'topic' = 'output_topic',
#   'properties.bootstrap.servers' = 'localhost:9092',
#   'format' = 'json'
# )
# """)

# Define console sink table
table_env.execute_sql("""
CREATE TABLE output_table (
    word STRING,
    word_count BIGINT
) WITH (
  'connector' = 'print'
)
""")


# Transform: split lines into words and count them
res = table_env.execute_sql("""
INSERT INTO output_table
SELECT word, COUNT(*) as word_count
FROM (
    SELECT word
    FROM input_table,
         UNNEST(split(word_line, ' ')) AS t(word)
)
GROUP BY word
""")

print("Job submitted. Waiting for completion...")

res.wait()  # Wait for the job to finish
