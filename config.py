import os

MINIO_CONFIG = {
    'endpoint': os.getenv('MINIO_URL', 'http://minio:9000'),
    'access_key': os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
    'secret_key': os.getenv('MINIO_SECRET_KEY', 'minioadmin'),
    'bucket_name': 'ml-charts',
    'public_url': os.getenv('MINIO_PUBLIC_URL', 'http://localhost:9000/ml-charts')
}

# Kafka настройки
KAFKA_CONFIG = {
    'bootstrap_servers': ['kafka:9092'],
    'group_id': 'ml-forecast-service',
    'auto_offset_reset': 'earliest',
    'enable_auto_commit': False
}

KAFKA_TOPIC = 'user_forecast_update'
KAFKA_FORECAST_READY_TOPIC = 'forecast_ready'

# PostgreSQL настройки
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'postgres'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'finpulse'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'password')
}

# ML настройки
ML_CONFIG = {
    'use_prophet': True,
    'batch_size': 10,
    'max_workers': 4
}

# Логирование
LOG_CONFIG = {
    'level': 'INFO',
    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
}