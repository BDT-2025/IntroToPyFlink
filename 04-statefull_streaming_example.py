from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor

class EventCountFunction(KeyedProcessFunction):

    def open(self, runtime_context: RuntimeContext):
        # Define state: count per key
        descriptor = ValueStateDescriptor("count", Types.INT())
        self.count_state = runtime_context.get_state(descriptor)

    def process_element(self, value, ctx):
        # Get current count or initialize
        count = self.count_state.value()
        if count is None:
            count = 0
        count += 1

        # Update state
        self.count_state.update(count)

        if count >= 3:
            # Emit user and count
            yield f"User {value[0]} reached {count} actions!"
            # Reset state
            self.count_state.clear()

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # Example input: list of tuples (user, action)
    input_data = [
        ("alice", "click"),
        ("alice", "click"),
        ("bob", "scroll"),
        ("alice", "click"),
        ("bob", "click"),
        ("bob", "scroll"),
        ("bob", "click"),
        ("alice", "scroll"),
        ("alice", "click"),
        ("bob", "click"),
        ("alice", "click"),
        ("bob", "scroll"),
        ("bob", "click"),
        ("alice", "scroll"),
        ("alice", "click"),
        ("bob", "click")
    ]

    ds = env.from_collection(input_data, type_info=Types.TUPLE([Types.STRING(), Types.STRING()]))

    ds \
        .key_by(lambda x: x[0]) \
        .process(EventCountFunction(), output_type=Types.STRING()) \
        .print()

    env.execute("Stateful User Action Counter")

if __name__ == "__main__":
    main()