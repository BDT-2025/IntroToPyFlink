from pyflink.table import EnvironmentSettings, TableEnvironment, DataTypes
from pyflink.table.expressions import col
from pyflink.table.udf import ScalarFunction


env_settings = EnvironmentSettings.in_batch_mode() # can be used only when we input a finite dataset
# env_settings = EnvironmentSettings.in_streaming_mode()
t_env = TableEnvironment.create(environment_settings=env_settings)

# Create table from in-memory data
data = [
    ("Alice", 12),
    ("Bob", 15),
    ("Alice", 8)
]

schema = DataTypes.ROW([
    DataTypes.FIELD("name", DataTypes.STRING()),
    DataTypes.FIELD("score", DataTypes.INT())
])

table = t_env.from_elements(data, schema)

# Perform a Table API operation
result = table.group_by(col("name")).select(
    col("name"), col("score").avg.alias("avg_score")
)

result.execute().print()
