import time
from redis import Redis
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    
    redis_host: str = "redis" 
    redis_port: int = 6379

settings = Settings()


try:
    redis = Redis(host=settings.redis_host, port=settings.redis_port, decode_responses=True)
    print("Notification servis je pokrenut i čeka događaje...")
except Exception as e:
    print(f"Greška pri povezivanju na Redis: {e}")

while True:
    try:
       
        results = redis.xread({"order_completed": "$", "refund_order": "$"}, block=0)
        if results:
            for stream, messages in results:
                for message_id, data in messages:
                    if stream == "order_completed":
                        print(f"Obaveštenje: Porudžbina {data.get('id', 'N/A')} je uspešno kreirana.")
                    elif stream == "refund_order":
                        print(f"Obaveštenje: Refundacija za porudžbinu {data.get('id', 'N/A')}.")
    except Exception as e:
        print(f"Greška u loop-u: {e}")
        time.sleep(2) 