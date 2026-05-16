import json
import logging
from kafka import KafkaConsumer
from config import setting
from database import driver

# Joins the Uvicorn log stream perfectly
logger = logging.getLogger("uvicorn.error") 

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
                user_id=int(user_id)  # Forced integer type safety for Neo4j
            )
    logger.info(f"[NEO4J SUCCESS] User created/merged: {username} ({email}) with id {user_id}")


def follow_user_in_db(follower_id: int, following_id: int):
    """Follow a user in Neo4j."""
    with driver.session() as session:
        session.run("""
            MERGE (a:User {id: $follower_id})
            MERGE (b:User {id: $following_id})
            MERGE (a)-[:FOLLOWS]->(b)
        """, follower_id=int(follower_id), following_id=int(following_id))
    logger.info(f"[NEO4J SUCCESS] User {follower_id} now follows {following_id}")


def unfollow_user_in_db(follower_id: int, following_id: int):
    """Unfollow a user in Neo4j."""
    with driver.session() as session:
        session.run("""
            MATCH (a:User {id: $follower_id})-[r:FOLLOWS]->(b:User {id: $following_id})
            DELETE r
        """, follower_id=int(follower_id), following_id=int(following_id))
    logger.info(f"[NEO4J SUCCESS] User {follower_id} unfollowed {following_id}")


def consume_messages():
    """Consume messages from Kafka and manage Neo4j records."""
    logger.info(f"[KAFKA INIT] Attempting to connect to brokers: {setting.KAFKA_BOOTSTRAP_SERVERS}")
    
    try:
        consumer = KafkaConsumer(
            'create_db_user',
            group_id="notify.email.group",
            bootstrap_servers=setting.KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            auto_offset_reset='earliest',
            enable_auto_commit=True
        )
        logger.info("[KAFKA INIT] Consumer connected successfully. Waiting for messages...")
    except Exception as init_err:
        logger.error(f"[KAFKA CRITICAL] Failed to initialize consumer: {init_err}")
        return

    for message in consumer:
        try:
            data = message.value
            logger.info(f"[KAFKA INBOUND] Received message payload: {data}")
            action = data.get('action')

            if action == 'create_user':
                username = data.get('username')
                email = data.get('email')
                user_id = data.get('user_id')
                if username and email:
                    create_user_in_db(username, email, user_id)
                else:
                    logger.warning(f"[KAFKA VALIDATION ERROR] Missing fields for create_user: {data}")

            elif action == 'follow':
                follower_id = data.get('follower_id')
                following_id = data.get('following_id')
                if follower_id and following_id:
                    follow_user_in_db(follower_id, following_id)
                else:
                    logger.warning(f"[KAFKA VALIDATION ERROR] Missing fields for follow: {data}")

            elif action == 'unfollow':
                follower_id = data.get('follower_id')
                following_id = data.get('following_id')
                if follower_id and following_id:
                    unfollow_user_in_db(follower_id, following_id)
                else:
                    logger.warning(f"[KAFKA VALIDATION ERROR] Missing fields for unfollow: {data}")

            else:
                logger.warning(f"[KAFKA ERROR] Unknown action requested: '{action}'")
                
        except Exception as e:
            logger.error(f"[KAFKA RUNTIME ERROR] Failed to process message loop: {e}", exc_info=True)