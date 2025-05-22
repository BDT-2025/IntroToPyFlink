from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.typeinfo import Types

env = StreamExecutionEnvironment.get_execution_environment()


data = env.from_collection([
    ("2025-05-14T10:15:00Z", "INFO", "auth-service", "User login succeeded"),
    ("2025-05-14T10:15:10Z", "ERROR", "auth-service", "Invalid password"),
    ("2025-05-14T10:15:10Z", "ERROR", "auth-service", "Invalid password"),
    ("2025-05-14T10:15:30Z", "INFO", "payment-service", "Payment completed"),
    ("2025-05-14T10:16:00Z", "WARN", "auth-service", "Password attempt warning"),
    ("2025-05-14T10:16:10Z", "INFO", "user-service", "Profile updated")
], type_info=Types.TUPLE([Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING()]))

# # TODO complete the code


result.print()
env.execute("Es1")
