import os

# Kafka настройки
KAFKA_CONFIG = {
    'bootstrap_servers': ['kafka:9092'],
    'group_id': 'recommendation-service',
    'auto_offset_reset': 'earliest',
    'enable_auto_commit': False,
    'max_poll_records': 100,
    'session_timeout_ms': 30000
}

KAFKA_TOPICS = {
    'forecasts': 'forecast_ready',           # слушаем этот топик
    'recommendations': 'recommendations_ready' # отправляем в этот топик
}

# PostgreSQL настройки
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'postgres'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'finpulse'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'password')
}

# Redis настройки
REDIS_CONFIG = {
    'host': os.getenv('REDIS_HOST', 'redis'),
    'port': int(os.getenv('REDIS_PORT', '6379')),
    'db': int(os.getenv('REDIS_DB', '0')),
    'password': os.getenv('REDIS_PASSWORD', None),
    'decode_responses': False  # важно для бинарных данных
}

# Application settings
APP_CONFIG = {
    'host': '0.0.0.0',
    'port': 8006,
    'debug': os.getenv('DEBUG', 'False').lower() == 'true'
}

# Logging
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')