import psycopg2
import json
import pandas as pd
import logging
from config import DB_CONFIG

logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self):
        self.db_config = DB_CONFIG
    
    def get_connection(self):
        try:
            conn = psycopg2.connect(**self.db_config)
            return conn
        except Exception as e:
            logger.error(f"Ошибка подключения к БД: {e}")
            return None
    
    def get_user_transactions(self, user_id, bank_client_id):
        conn = self.get_connection()
        if not conn:
            return None
            
        try:
            query = """
                SELECT 
                    t.booking_date as date,
                    t.amount,
                    t.absolute_amount,
                    t.is_expense,
                    t.transaction_information as description,
                    t.credit_debit_indicator,
                    t.category
                FROM transactions t
                WHERE t.bank_client_id = %s
                ORDER BY t.booking_date DESC
                LIMIT 1000
            """
            
            df = pd.read_sql_query(query, conn, params=[bank_client_id])
            
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            df = df.dropna(subset=['date'])
            

            df = df.sort_values('date')
            

            expenses_count = df[df['is_expense'] == True].shape[0]
            income_count = df[df['is_expense'] == False].shape[0]
            
            return df
            
        except Exception as e:
            logger.error(f"Ошибка получения транзакций для {bank_client_id}: {e}")
            return None
        finally:
            if conn:
                conn.close()

    def save_forecast(self, user_id, bank_client_id, forecast_data):
        conn = self.get_connection()
        if not conn:
            return False
            
        try:
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'user_forecasts'
                );
            """)
            table_exists = cursor.fetchone()[0]
            
            if not table_exists:
                logger.warning("Таблица user_forecasts не существует, создаем...")
                self._create_forecasts_table(cursor)
            else:
                self._ensure_charts_column_exists(cursor)
            
            forecast_result = forecast_data['forecast']
            charts_data = forecast_data.get('charts', {})
            
            from datetime import datetime, timedelta
            next_monday = (datetime.now() + timedelta(days=(7 - datetime.now().weekday()))).date()
            
            cursor.execute("""
                INSERT INTO user_forecasts 
                (user_id, forecast_amount, confidence_min, confidence_max, 
                 change_percentage, last_week_amount, forecast_method,
                 forecast_week_start, full_forecast_data, chart_urls)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (user_id, forecast_week_start) 
                DO UPDATE SET 
                    forecast_amount = EXCLUDED.forecast_amount,
                    confidence_min = EXCLUDED.confidence_min,
                    confidence_max = EXCLUDED.confidence_max,
                    change_percentage = EXCLUDED.change_percentage,
                    last_week_amount = EXCLUDED.last_week_amount,
                    forecast_method = EXCLUDED.forecast_method,
                    full_forecast_data = EXCLUDED.full_forecast_data,
                    chart_urls = EXCLUDED.chart_urls,
                    updated_at = CURRENT_TIMESTAMP
            """, (
                user_id,
                forecast_result['forecast'],
                forecast_result['confidence_interval'][0],
                forecast_result['confidence_interval'][1],
                forecast_result['change_pct'],
                forecast_result['last_week'],
                forecast_result['method'],
                next_monday,
                json.dumps(forecast_data),
                json.dumps(charts_data)
            ))
            
            conn.commit()

            return True
            
        except Exception as e:
            logger.error(f"Ошибка сохранения прогноза для {user_id}: {e}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                conn.close()

    def _create_forecasts_table(self, cursor):
        try:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS user_forecasts (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    user_id UUID NOT NULL REFERENCES users(id),
                    forecast_amount DECIMAL(15,2) NOT NULL,
                    confidence_min DECIMAL(15,2),
                    confidence_max DECIMAL(15,2),
                    change_percentage DECIMAL(5,2),
                    last_week_amount DECIMAL(15,2),
                    forecast_method VARCHAR(50),
                    forecast_week_start DATE NOT NULL,
                    full_forecast_data JSONB,
                    chart_urls JSONB,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(user_id, forecast_week_start)
                );
                
                CREATE INDEX IF NOT EXISTS idx_user_forecasts_user_id ON user_forecasts(user_id);
                CREATE INDEX IF NOT EXISTS idx_user_forecasts_week_start ON user_forecasts(forecast_week_start);
            """)
        except Exception as e:
            logger.error(f"Ошибка создания таблицы user_forecasts: {e}")
            raise

    def _ensure_charts_column_exists(self, cursor):
        try:
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name='user_forecasts' AND column_name='chart_urls'
            """)
            if not cursor.fetchone():
                cursor.execute("ALTER TABLE user_forecasts ADD COLUMN chart_urls JSONB")
        except Exception as e:
            logger.error(f"Ошибка проверки колонки chart_urls: {e}")