
import threading
from fastapi import FastAPI,Form


from fastapi.middleware.cors import CORSMiddleware

from fastapi.responses import JSONResponse
from database import driver

from pydantic import BaseModel
from consumer import consume_messages

class UserCreate(BaseModel):
    username: str
    email: str


router=FastAPI()

@router.on_event("startup")
def start_kafka_consumer():
    consumer_thread = threading.Thread(target=consume_messages, daemon=True)
    consumer_thread.start()
    print("Kafka consumer started in background thread")

router.add_middleware(
    CORSMiddleware,
    allow_origins=["*",
         
    ],  # your frontend origin
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@router.get("/")
def root():
    return {"Connection":"Successful"}
     

@router.post("/following/unfollow")
def unfollow_user(follower_id: int = Form(...), following_id: int = Form(...)):
    with driver.session() as session:
        session.run("""
            MATCH (a:User {id: $follower_id})-[r:FOLLOWS]->(b:User {id: $following_id})
            DELETE r
        """, follower_id=follower_id, following_id=following_id)
    return {"message": "Unfollowed"}


@router.get("/following/is-following")
def is_following(follower_id: int, following_id: int):
    with driver.session() as session:
        result = session.run("""
            MATCH (a:User {id: $follower_id})-[:FOLLOWS]->(b:User {id: $following_id})
            RETURN count(*) > 0 AS is_following
        """, follower_id=follower_id, following_id=following_id)
        return {"is_following": result.single()["is_following"]}

@router.post("/following/create_user_no_sql")
def create_user(user: UserCreate):
    with driver.session() as session:
        session.run(
            """
            CREATE (u:User {username: $username, email: $email})
            """,
            username=user.username,
            email=user.email
        )
    return {"message": "User created successfully"}


 

@router.post("/following/follow")
def follow_user(follower_id: int = Form(...), following_id: int = Form(...)):
    with driver.session() as session:
        session.run("""
            MERGE (a:User {id: $follower_id})
            MERGE (b:User {id: $following_id})
            MERGE (a)-[:FOLLOWS]->(b)
        """, follower_id=follower_id, following_id=following_id)

    return JSONResponse(
        status_code=201,
        content={"message": f"User {follower_id} now follows {following_id}"}
    )

@router.get("/following/followers/{user_id}")
def get_followers(user_id: int):
    with driver.session() as session:
        # Match any user (f) who has an outgoing FOLLOWS relationship to the targeted user_id node
        result = session.run("""
            MATCH (f:User)-[:FOLLOWS]->(:User {id: $user_id})
            RETURN f.id AS follower_id, f.username AS username, f.email AS email
        """, user_id=user_id)
        
        # Parse out the data array safely from the transaction records
        followers_list = []
        for record in result:
            followers_list.append({
                "follower_id": record["follower_id"],
                "username": record.get("username") or "Unknown",
                "email": record.get("email") or "No Email"
            })
            
        return {"followers": followers_list}

@router.get("following/followings/{user_id}")
def get_followings(user_id: int):
    with driver.session() as session:
        result = session.run("""
            MATCH (:User {id: $user_id})-[:FOLLOWS]->(f:User)
            RETURN f.id AS following_id
        """, user_id=user_id)
        return {"followings": [record["following_id"] for record in result]}

@router.get("/following/suggestions/{user_id}")
def suggest_friends(user_id: int):
    with driver.session() as session:
        result = session.run("""
            MATCH (:User {id: $user_id})-[:FOLLOWS]->(:User)-[:FOLLOWS]->(suggested:User)
            WHERE NOT (:User {id: $user_id})-[:FOLLOWS]->(suggested)
              AND suggested.id <> $user_id
            RETURN DISTINCT suggested.id AS suggestion
        """, user_id=user_id)
        return {"suggestions": [record["suggestion"] for record in result]}
