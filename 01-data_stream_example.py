from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.typeinfo import Types

env = StreamExecutionEnvironment.get_execution_environment()

data = env.from_collection([
    ("apple", 2),
    ("banana", 3),
    ("apple", 5),
    ("banana", 1),
    ("orange", 4),
    ("apple", 1)
], type_info=Types.TUPLE([Types.STRING(), Types.INT()]))

# Reduce by key
result = data.key_by(lambda x: x[0]).reduce(lambda a, b: (a[0], a[1] + b[1]))
result.print()

env.execute("Tuple Stream Example")
