import json
import logging
import redis
from config import REDIS_CONFIG
from database import db_manager

logger = logging.getLogger(__name__)

class RedisProductClient:
    def __init__(self):
        self.redis_client = redis.Redis(**REDIS_CONFIG)
        self.key_pattern = "bank_products:*"
        self.use_redis = True
    
    def get_all_products(self):
        """Получает все активные продукты из Redis с fallback на БД"""
        all_products = []
        
        if self.use_redis:
            try:
                self.redis_client.ping()
                keys = self.redis_client.keys(self.key_pattern)
                
                if not keys:
                    logger.warning("В Redis нет ключей с продуктами, используем БД")
                    return self._get_products_from_db()
                
                for key in keys:
                    try:
                        data = self.redis_client.get(key)
                        if data:
                            products_list = json.loads(data.decode('utf-8'))
                            
                            for product in products_list:
                                if product.get('isActive', True):
                                    all_products.append({
                                        'productId': product.get('productId'),
                                        'productType': product.get('productType'),
                                        'productName': product.get('productName'),
                                        'description': product.get('description'),
                                        'interestRate': str(product.get('interestRate')) if product.get('interestRate') is not None else None,
                                        'minAmount': str(product.get('minAmount')) if product.get('minAmount') is not None else None,
                                        'maxAmount': str(product.get('maxAmount')) if product.get('maxAmount') is not None else None,
                                        'termMonths': product.get('termMonths')
                                    })
                                        
                    except Exception as e:
                        logger.error(f"Ошибка обработки ключа {key}: {e}")
                        continue
                
                if all_products:
                    logger.info(f"Загружено {len(all_products)} активных продуктов из Redis")
                    return all_products
                else:
                    logger.warning("Redis доступен, но продукты не найдены, используем БД")
                    return self._get_products_from_db()
                        
            except Exception as e:
                logger.error(f"Redis недоступен, используем БД: {e}")
                self.use_redis = False
                return self._get_products_from_db()
        
        return self._get_products_from_db()
    
    def _get_products_from_db(self):
        """Fallback - получает продукты из БД"""
        try:
            logger.info("Загрузка продуктов из БД (fallback)")
            products = db_manager.get_products()
            logger.info(f"Загружено {len(products)} продуктов из БД")
            return products
        except Exception as e:
            logger.error(f"Ошибка загрузки продуктов из БД: {e}")
            return []
    
    def test_redis_connection(self):
        """Проверяет доступность Redis и включает его обратно если доступен"""
        try:
            self.redis_client.ping()
            if not self.use_redis:
                logger.info("Redis снова доступен, переключаемся на него")
                self.use_redis = True
            return True
        except:
            self.use_redis = False
            return False

redis_product_client = RedisProductClient()