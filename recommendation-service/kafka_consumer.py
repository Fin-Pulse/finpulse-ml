from kafka import KafkaConsumer
import json
import logging
from threading import Thread
from concurrent.futures import ThreadPoolExecutor
from config import KAFKA_CONFIG, KAFKA_TOPICS
from database import db_manager
from recommender import recommender
from kafka_producer import producer
from redis_product_client import redis_product_client
import time

logger = logging.getLogger(__name__)

class ForecastConsumer:
    def __init__(self):
        self.consumer = None
        self.executor = ThreadPoolExecutor(max_workers=4)
        self.running = False
        self.last_redis_check = 0
        self.redis_check_interval = 300
    
    def _check_redis_connection(self):
        """Периодически проверяет доступность Redis"""
        current_time = time.time()
        if current_time - self.last_redis_check > self.redis_check_interval:
            redis_product_client.test_redis_connection()
            self.last_redis_check = current_time
    
    def _extract_forecast_data(self, event):
        """Извлекает данные прогноза из Kafka сообщения"""
        try:
            if not event.get('forecastReady', False) or 'result' not in event:
                logger.warning("Прогноз не готов или отсутствует результат")
                return None
            
            result = event['result']
            forecast_result = result['forecast']
            analysis = result['analysis']
            meta = result['metadata']
            
            # Преобразуем в формат для рекомендатора
            forecast_data = {
                "userId": event['userId'],
                "result": {
                    "forecast": forecast_result['forecast'],  # значение прогноза
                    "analysis": {
                        "statistics": {
                            "mean": analysis.get('mean', 0),  # среднее значение
                            "std": analysis.get('std', 0)     # стандартное отклонение
                        },
                        "volatility": analysis.get('volatility', 0),
                        "trends": {
                            "long_term_trend": analysis.get('trends', {}).get('long_term_trend', 0)
                        }
                    },
                    "recommendations": {
                        "insights": result['recommendations']['insights']  # можно добавить аномалии из analysis если есть
                    },
                    "re": {
                        "change_pct": forecast_result['change_pct']
                    }
                }
            }
            
            return forecast_data
            
        except Exception as e:
            logger.error(f"Ошибка извлечения данных прогноза: {e}")
            return None
    
    def process_message(self, message):
        """Обрабатывает сообщение с прогнозом"""
        try:
            event = message.value
            user_id = event['userId']
            
            logger.info(f"Получен прогноз для {user_id}")
            
            # Извлекаем данные прогноза из Kafka сообщения
            forecast_data = self._extract_forecast_data(event)
            if not forecast_data:
                logger.error(f"Не удалось извлечь данные прогноза для {user_id}")
                return
            
            # Проверяем доступность Redis
            self._check_redis_connection()
            
            # Получаем продукты (из Redis или БД)
            products = redis_product_client.get_all_products()
            if not products:
                logger.error("Не удалось получить продукты ни из Redis, ни из БД")
                return
            
            # Генерируем рекомендации
            recommendations = recommender.recommend(forecast_data, products)
            
            # Сохраняем в БД
            db_manager.save_recommendations(user_id, recommendations)
            
            # Отправляем в Kafka
            producer.send_recommendations_ready(user_id)
            
            logger.info(f"Обработан прогноз для {user_id}")
            
        except Exception as e:
            logger.error(f"Ошибка обработки: {e}")
    
    def start_consuming(self):
        """Запускает потребитель Kafka"""
        try:
            self.consumer = KafkaConsumer(
                KAFKA_TOPICS['forecasts'],
                value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None,
                **KAFKA_CONFIG
            )
            
            self.running = True
            logger.info(f"Ожидаем сообщения в {KAFKA_TOPICS['forecasts']}")
            
            for message in self.consumer:
                if not self.running:
                    break
                    
                if message.value is None:
                    continue
                
                # Обрабатываем в отдельном потоке
                self.executor.submit(self.process_message, message)
                self.consumer.commit()
                        
        except Exception as e:
            logger.error(f"Ошибка consumer: {e}")
            raise
    
    def stop(self):
        """Останавливает потребитель"""
        self.running = False
        if self.consumer:
            self.consumer.close()
        self.executor.shutdown()

consumer = ForecastConsumer()