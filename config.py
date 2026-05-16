
from pydantic_settings import BaseSettings
 
class Settings(BaseSettings):

    NEO4J_AUTH:str
    NEO4J_PASSWORD:str
    NEO4J_URI:str
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
    


     
    class Config:
        env_file=".env"


setting=Settings()

