from kafka import KafkaProducer
import json
import logging
from config import KAFKA_CONFIG, KAFKA_TOPICS
from datetime import datetime

logger = logging.getLogger(__name__)


class RecommendationProducer:
    def __init__(self):
        self.producer = None
        self._init_producer()
    
    def _init_producer(self):
        """Инициализирует Kafka producer"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=KAFKA_CONFIG["bootstrap_servers"],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: str(k).encode('utf-8'),
                acks='all'
            )
            logger.info("RecommendationProducer успешно инициализирован")
        except Exception as e:
            logger.error(f"Ошибка инициализации Kafka producer: {e}")
            self.producer = None

    def send_recommendations_ready(self, user_id: str):
        """
        Отправляет событие о том, что рекомендации готовы.
        САМИ ДАННЫЕ не отправляются!
        """
        try:
            if self.producer is None:
                logger.error("Producer is None — событие не отправлено")
                return
            
            event = {
                "userId": str(user_id),
                "recommendationsReady": True,
                "timestamp": int(datetime.utcnow().timestamp() * 1000)
            }

            future = self.producer.send(
                KAFKA_TOPICS["recommendations"],
                key=str(user_id),
                value=event
            )

            future.get(timeout=5)
            logger.info(f"Событие recommendations_ready отправлено для {user_id}")

        except Exception as e:
            logger.error(f"Ошибка отправки события recommendations_ready: {e}")


# Singleton instance
producer = RecommendationProducer()
