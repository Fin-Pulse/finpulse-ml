import logging
import signal
import sys
from config import LOG_CONFIG
from database import DatabaseManager
from ml_processor import MLProcessor
from kafka_consumer import KafkaForecastConsumer

# Настройка логирования
logging.basicConfig(**LOG_CONFIG)
logger = logging.getLogger(__name__)

class MLForecastService:
    def __init__(self):
        self.db_manager = DatabaseManager()
        self.ml_processor = MLProcessor()
        self.consumer = KafkaForecastConsumer(self.db_manager, self.ml_processor)
        self.is_running = True
        
    def start(self):
        try:
            logger.info("Запуск ML Forecast Service...")
            
            signal.signal(signal.SIGINT, self.signal_handler)
            signal.signal(signal.SIGTERM, self.signal_handler)

            self.consumer.start_consuming()
            
        except Exception as e:
            logger.error(f"Критическая ошибка при запуске сервиса: {e}")
            sys.exit(1)
    
    def signal_handler(self, signum, frame):
        logger.info(f"Получен сигнал {signum}. Останавливаем сервис...")
        self.is_running = False
        if self.consumer:
            self.consumer.executor.shutdown(wait=True)
            # Закрываем Kafka producer
            if self.consumer.producer:
                self.consumer.producer.close()
        sys.exit(0)

if __name__ == "__main__":
    service = MLForecastService()
    service.start()