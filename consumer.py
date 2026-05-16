from kafka import KafkaConsumer
import json
from config import setting
from database import driver


def create_user_in_db(username: str, email: str, user_id: int = None):
    """Create a user in the Neo4j database."""
    with driver.session() as session:
        if user_id is not None:
            session.run(
                """
                MERGE (u:User {id: $user_id})
                SET u.username = $username, u.email = $email
                """,
                username=username,
                email=email,
                user_id=user_id
            )
         
    print(f"User created: {username} ({email}) with id {user_id}")


def follow_user_in_db(follower_id: int, following_id: int):
    """Follow a user in Neo4j."""
    with driver.session() as session:
        session.run("""
            MERGE (a:User {id: $follower_id})
            MERGE (b:User {id: $following_id})
            MERGE (a)-[:FOLLOWS]->(b)
        """, follower_id=follower_id, following_id=following_id)
    print(f"User {follower_id} now follows {following_id}")


def unfollow_user_in_db(follower_id: int, following_id: int):
    """Unfollow a user in Neo4j."""
    with driver.session() as session:
        session.run("""
            MATCH (a:User {id: $follower_id})-[r:FOLLOWS]->(b:User {id: $following_id})
            DELETE r
        """, follower_id=follower_id, following_id=following_id)
    print(f"User {follower_id} unfollowed {following_id}")


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

    print("Kafka consumer started. Waiting for messages...",flush=True)

    for message in consumer:
        try:
            data = message.value
            action = data.get('action')

            if action == 'create_user':
                username = data.get('username')
                email = data.get('email')
                user_id = data.get('user_id')
                if username and email:
                    create_user_in_db(username, email, user_id)
                    print(f"IUSER CRETED: {data}",flush=True)
                else:
                    print(f"Invalid message data: {data}")

            elif action == 'follow':
                follower_id = data.get('follower_id')
                following_id = data.get('following_id')
                if follower_id and following_id:
                    follow_user_in_db(follower_id, following_id)
                else:
                    print(f"Invalid follow data: {data}")

            elif action == 'unfollow':
                follower_id = data.get('follower_id')
                following_id = data.get('following_id')
                if follower_id and following_id:
                    unfollow_user_in_db(follower_id, following_id)
                else:
                    print(f"Invalid unfollow data: {data}")

            else:
                print(f"Unknown action: {action}")
        except Exception as e:
            print(f"Error processing message: {e}")


 