from pyflink.table import EnvironmentSettings, TableEnvironment
import os


env_settings = EnvironmentSettings.in_batch_mode() # can be used only when we input a finite dataset

t_env = TableEnvironment.create(env_settings)

# Reading CSV data
users_csv_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'data/users.csv')) # converting relative path to absolute path

# Reading a folder of CSV files
# users_csv_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'data/users_folder')) # converting relative path to absolute path

print(f"Loading CSV data from: {users_csv_path}")

t_env.execute_sql(f"""
    CREATE TEMPORARY TABLE users (
        name STRING,
        age INT
    ) WITH (
        'connector' = 'filesystem',
        'path' = 'file://{users_csv_path}',
        'format' = 'csv'
    )
""")

result = t_env.sql_query("SELECT name, age FROM users WHERE age > 18")
result.execute().print()
