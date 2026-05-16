from kafka import KafkaConsumer
import json
from config import setting
from database import driver


def create_user_in_db(username: str, email: str):
    """Create a user in the Neo4j database."""
    with driver.session() as session:
        session.run(
            """
            CREATE (u:User {username: $username, email: $email})
            """,
            username=username,
            email=email
        )
    print(f"User created: {username} ({email})")


def consume_messages():
    """Consume messages from Kafka and create users."""
    consumer = KafkaConsumer(
        'create_db_user',
        group_id="notify.email.group",
        bootstrap_servers=setting.KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        auto_offset_reset='earliest',
        enable_auto_commit=True
    )

    print("Kafka consumer started. Waiting for messages...")

    for message in consumer:
        try:
            data = message.value
            username = data.get('username')
            email = data.get('email')

            if username and email:
                create_user_in_db(username, email)
            else:
                print(f"Invalid message data: {data}")
        except Exception as e:
            print(f"Error processing message: {e}")


 