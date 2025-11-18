from kafka import KafkaProducer
import json
import logging
from config import KAFKA_CONFIG, KAFKA_TOPICS

logger = logging.getLogger(__name__)

class RecommendationProducer:
    def __init__(self):
        self.producer = None
        self._init_producer()
    
    def _init_producer(self):
        """Инициализирует продюсер"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=KAFKA_CONFIG['bootstrap_servers'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: str(k).encode('utf-8')
            )
        except Exception as e:
            logger.error(f"Ошибка producer: {e}")
            self.producer = None
    
    def send_recommendation(self, user_id, recommendations):
        """Отправляет рекомендации в Kafka"""
        try:
            if self.producer is None:
                return
            
            message = {
                'userId': str(user_id),
                'recommendations': recommendations
            }
            
            future = self.producer.send(
                KAFKA_TOPICS['recommendations'],
                key=str(user_id),
                value=message
            )
            
            future.get(timeout=10)
            logger.info(f"Рекомендации отправлены для {user_id}")
            
        except Exception as e:
            logger.error(f"Ошибка отправки: {e}")

producer = RecommendationProducer()