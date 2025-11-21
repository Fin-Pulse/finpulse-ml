import psycopg2
import json
import logging
from config import DB_CONFIG

logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self):
        self.db_config = DB_CONFIG
    
    def get_connection(self):
        try:
            return psycopg2.connect(**self.db_config)
        except Exception as e:
            logger.error(f"Ошибка подключения к БД: {e}")
            return None
    
    def ensure_table_exists(self):
        """Создает таблицу если ее нет"""
        conn = self.get_connection()
        if not conn:
            return False
            
        try:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS user_recommendations (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    user_id UUID NOT NULL,
                    recommendations JSONB NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                
                CREATE INDEX IF NOT EXISTS idx_user_recommendations_user_id 
                ON user_recommendations(user_id);
            """)
            conn.commit()
            return True
        except Exception as e:
            logger.error(f"Ошибка создания таблицы: {e}")
            return False
        finally:
            if conn:
                conn.close()
    
    def get_products(self):
        """Получает все активные продукты из bank_products"""
        conn = self.get_connection()
        if not conn:
            return []
            
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    bank_id,
                    product_id,
                    product_type, 
                    product_name,
                    description,
                    interest_rate,
                    min_amount,
                    max_amount,
                    term_months
                FROM bank_products 
                WHERE is_active = TRUE
            """)
            
            products = []
            for row in cursor.fetchall():
                products.append({
                    'bank_id': row[0],
                    'productId': row[1],           # product_id
                    'productType': row[2],         # product_type
                    'productName': row[3],         # product_name
                    'description': row[4],         # description
                    'interestRate': str(row[5]) if row[5] is not None else None,  # interest_rate
                    'minAmount': str(row[6]) if row[6] is not None else None,     # min_amount
                    'maxAmount': str(row[7]) if row[7] is not None else None,     # max_amount
                    'termMonths': row[8]           # term_months
                })
            
            logger.info(f"Загружено {len(products)} активных продуктов из bank_products")
            return products
        except Exception as e:
            logger.error(f"Ошибка получения продуктов: {e}")
            return []
        finally:
            if conn:
                conn.close()
    
    def save_recommendations(self, user_id, recommendations):
        """Сохраняет рекомендации в БД - ИСПРАВЛЕНА СЕРИАЛИЗАЦИЯ"""
        conn = self.get_connection()
        if not conn:
            return False
            
        try:
            # Преобразуем объекты BankServiceRecommendation в словари
            recommendations_dicts = [rec.to_dict() for rec in recommendations]
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO user_recommendations (user_id, recommendations)
                VALUES (%s, %s)
            """, (user_id, json.dumps(recommendations_dicts)))
            conn.commit()
            logger.info(f"Сохранены рекомендации для пользователя {user_id}")
            return True
        except Exception as e:
            logger.error(f"Ошибка сохранения рекомендаций: {e}")
            return False
        finally:
            if conn:
                conn.close()
    
    def get_recommendations(self, user_id):
        """Получает рекомендации пользователя"""
        conn = self.get_connection()
        if not conn:
            return None
            
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT recommendations, created_at
                FROM user_recommendations 
                WHERE user_id = %s
                ORDER BY created_at DESC
                LIMIT 1
            """, (user_id,))
            
            result = cursor.fetchone()
            return result
        except Exception as e:
            logger.error(f"Ошибка получения рекомендаций: {e}")
            return None
        finally:
            if conn:
                conn.close()

db_manager = DatabaseManager()