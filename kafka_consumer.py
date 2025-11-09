from kafka import KafkaConsumer, KafkaProducer
import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from config import KAFKA_CONFIG, KAFKA_TOPIC, KAFKA_FORECAST_READY_TOPIC, ML_CONFIG

logger = logging.getLogger(__name__)

class KafkaForecastConsumer:
    def __init__(self, db_manager, ml_processor):
        self.db_manager = db_manager
        self.ml_processor = ml_processor
        self.consumer = None
        self.producer = None
        self.executor = ThreadPoolExecutor(max_workers=ML_CONFIG['max_workers'])
        self._init_producer()
        
    def _init_producer(self):
        try:
            producer_config = {
                'bootstrap_servers': KAFKA_CONFIG['bootstrap_servers'],
                'value_serializer': lambda v: json.dumps(v).encode('utf-8'),
                'key_serializer': lambda k: str(k).encode('utf-8') if k else None
            }
            self.producer = KafkaProducer(**producer_config)
        except Exception as e:
            logger.error(f"Ошибка инициализации Kafka producer: {e}")
            self.producer = None
    
    def safe_deserialize(self, message):
        try:
            if message is None:
                return None
            return json.loads(message.decode('utf-8'))
        except Exception as e:
            logger.warning(f"Некорректное сообщение в Kafka: {e}")
            return None
    
    def publish_forecast_ready(self, user_id, bank_client_id, success=True, error=None):
        try:
            if self.producer is None:
                logger.warning("Kafka producer не инициализирован, пропускаем публикацию")
                return
            
            event = {
                'userId': str(user_id),
                'bankClientId': bank_client_id,
                'forecastReady': success,
                'timestamp': int(time.time() * 1000)
            }
            
            if not success and error:
                event['error'] = error
            
            future = self.producer.send(
                KAFKA_FORECAST_READY_TOPIC,
                key=str(user_id),
                value=event
            )
            
            future.get(timeout=10)
            
        except Exception as e:
            logger.error(f"Ошибка публикации события о готовности прогноза: {e}")

    def start_consuming(self):
        try:
            self.consumer = KafkaConsumer(
                KAFKA_TOPIC,
                **KAFKA_CONFIG,
                value_deserializer=lambda m: self.safe_deserialize(m)
            )
            
            logger.info(f"ML Service запущен. Ожидаем сообщения в топике {KAFKA_TOPIC}...")
            
            for message in self.consumer:
                if message.value is None:
                    self.consumer.commit()
                    continue
                    
                try:
                    
                    future = self.executor.submit(self.process_message, message)
                    future.add_done_callback(lambda f: self.consumer.commit())
                    
                except Exception as e:
                    logger.error(f"Ошибка обработки сообщения: {e}")
                    self.consumer.commit()
                        
        except Exception as e:
            logger.error(f"Ошибка запуска Kafka consumer: {e}")
            raise
    
    def process_message(self, message):

        try:
            event_data = message.value
            user_id = event_data.get('userId')
            bank_client_id = event_data.get('bankClientId')
            analysis_type = event_data.get('analysisType', 'WEEKLY')
            
            if not bank_client_id:
                bank_client_id = self.db_manager.get_bank_client_id(user_id)
                if not bank_client_id:
                    return
            
            result = self.ml_processor.process_user_forecast(user_id, bank_client_id, self.db_manager, self)
            
            if result.get('success'):
                self.publish_forecast_ready(user_id, bank_client_id, success=True)
            else:
                logger.warning(f"Прогноз не создан для {user_id}: {result.get('error')}")
                self.publish_forecast_ready(user_id, bank_client_id, success=False, error=result.get('error'))
                
        except Exception as e:
            logger.error(f"Ошибка обработки сообщения Kafka: {e}")
            try:
                event_data = message.value
                user_id = event_data.get('userId')
                bank_client_id = event_data.get('bankClientId')
                if user_id:
                    self.publish_forecast_ready(user_id, bank_client_id, success=False, error=str(e))
            except:
                pass