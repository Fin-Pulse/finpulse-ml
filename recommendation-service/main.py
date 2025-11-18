from fastapi import FastAPI, HTTPException
import logging
import uuid
from threading import Thread

from database import db_manager
from kafka_consumer import consumer

logging.basicConfig(
    level='INFO',
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Recommendation Service",
    description="Сервис рекомендаций финансовых продуктов",
    version="1.0.0"
)

@app.on_event("startup")
def startup():
    """Запуск сервиса"""
    logger.info("Запуск Recommendation Service")
    
    # Создаем таблицу если нет
    db_manager.ensure_table_exists()
    
    # Запускаем Kafka consumer в фоне
    def start_consumer():
        consumer.start_consuming()
    
    thread = Thread(target=start_consumer, daemon=True)
    thread.start()

@app.on_event("shutdown")
def shutdown():
    """Остановка сервиса"""
    consumer.stop()

@app.get("/health")
def health():
    """Health check"""
    return {"status": "healthy", "service": "recommendation"}

@app.get("/recommendations/{user_id}")
def get_recommendations(user_id: uuid.UUID):
    """Получить рекомендации пользователя"""
    result = db_manager.get_recommendations(user_id)
    if not result:
        raise HTTPException(status_code=404, detail="Рекомендации не найдены")
    
    return {
        "userId": user_id,
        "recommendations": result[0],
        "createdAt": result[1].isoformat()
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000)