import logging
import numpy as np
from typing import List, Dict, Any

logger = logging.getLogger(__name__)

class ProductRecommender:
    """
    Класс для генерации рекомендаций финансовых продуктов.
    Теперь сохраняет полные данные о продуктах для отображения пользователю.
    """
    
    def __init__(self):
        # Веса для разных типов продуктов
        self.product_weights = {
            'loan': {
                'forecast_amount': 0.3,
                'volatility': -0.2,
                'trend': 0.4,
                'min_amount_match': 0.2
            },
            'deposit': {
                'forecast_amount': 0.4,
                'volatility': 0.3,
                'trend': -0.2,
                'min_amount_match': 0.1
            },
            'card': {
                'forecast_amount': 0.2,
                'volatility': -0.1,
                'trend': 0.3,
                'min_amount_match': 0.15
            }
        }
    
    def score_product(self, forecast: Dict[str, Any], product: Dict[str, Any]) -> float:
        """
        Вычисляет скор для одного продукта на основе прогноза пользователя.
        """
        try:
            # Извлекаем данные прогноза
            forecast_amount = forecast['forecastAmount']
            volatility = forecast['analytics']['volatility']
            trend = forecast['analytics']['long_term_trend']
            change_pct = forecast['analytics']['change_pct']
            
            # Извлекаем данные продукта
            product_type = product['productType']
            interest_rate = float(product['interestRate']) if product['interestRate'] else 0.0
            min_amount = float(product['minAmount']) if product['minAmount'] else 0.0
            max_amount = float(product['maxAmount']) if product['maxAmount'] else float('inf')
            
            # Получаем веса для типа продукта
            weights = self.product_weights.get(product_type, {})
            if not weights:
                return 0.0
            
            score = 0.0
            features_used = 0
            
            # 1. Учет суммы прогноза
            if 'forecast_amount' in weights:
                amount_score = min(1.0, forecast_amount / 100000)
                score += amount_score * weights['forecast_amount']
                features_used += 1
            
            # 2. Учет волатильности
            if 'volatility' in weights:
                vol_score = 1.0 - min(1.0, volatility / 200)
                score += vol_score * weights['volatility']
                features_used += 1
            
            # 3. Учет тренда
            if 'trend' in weights:
                trend_score = trend / 1000
                score += trend_score * weights['trend']
                features_used += 1
            
            # 4. Проверка минимальной суммы
            if 'min_amount_match' in weights and min_amount > 0:
                if forecast_amount >= min_amount:
                    score += weights['min_amount_match']
                    features_used += 1
            
            # 5. Учет процентной ставки
            if interest_rate > 0:
                if product_type == 'loan':
                    rate_score = 1.0 / (1.0 + interest_rate / 100)
                else:
                    rate_score = interest_rate / 100
                score += rate_score * 0.1
                features_used += 1
            
            # Нормализуем итоговый скор
            if features_used > 0:
                final_score = max(0.0, min(1.0, score))
                return round(final_score, 4)
            else:
                return 0.0
                
        except Exception as e:
            logger.error(f"Ошибка в score_product: {e}")
            return 0.0
    
    def recommend(self, forecast: Dict[str, Any], products: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Основной метод генерации рекомендаций.
        Теперь возвращает полные данные о продуктах.
        """
        try:
            # Шаг 1: Вычисляем скор для каждого продукта и сохраняем полные данные
            scored_products = []
            for product in products:
                score = self.score_product(forecast, product)
                if score > 0.1:  # минимальный порог
                    # Сохраняем полные данные о продукте + скор
                    product_recommendation = {
                        'productId': product['productId'],
                        'score': score,
                        'productType': product['productType'],
                        'productName': product['productName'],
                        'description': product['description'],
                        'interestRate': product['interestRate'],
                        'minAmount': product['minAmount'],
                        'maxAmount': product['maxAmount'],
                        'termMonths': product['termMonths'],
                        # Дополнительные поля для UI
                        'matchReason': self._get_match_reason(score, forecast, product),
                        'forecastAmount': forecast['forecastAmount']  # для контекста
                    }
                    scored_products.append(product_recommendation)
            
            # Шаг 2: Убираем дубликаты по productId и сортируем по скору
            unique_products = {}
            for product in scored_products:
                product_id = product['productId']
                if product_id not in unique_products or product['score'] > unique_products[product_id]['score']:
                    unique_products[product_id] = product
            
            # Шаг 3: Сортируем по скору (убывание) и берем топ-10
            sorted_products = sorted(unique_products.values(), key=lambda x: x['score'], reverse=True)
            top_recommendations = sorted_products[:10]
            
            logger.info(f"Сгенерировано {len(top_recommendations)} рекомендаций для {forecast['userId']}")
            
            return top_recommendations
            
        except Exception as e:
            logger.error(f"Ошибка в recommend: {e}")
            return []
    
    def _get_match_reason(self, score: float, forecast: Dict[str, Any], product: Dict[str, Any]) -> str:
        """
        Генерирует текстовое объяснение почему продукт рекомендован.
        """
        reasons = []
        forecast_amount = forecast['forecastAmount']
        product_type = product['productType']
        
        # Анализ суммы
        min_amount = float(product['minAmount']) if product['minAmount'] else 0
        if min_amount > 0 and forecast_amount >= min_amount:
            reasons.append("подходит по минимальной сумме")
        
        # Анализ типа продукта и финансового профиля
        if product_type == 'loan':
            if forecast['analytics']['volatility'] < 50:
                reasons.append("стабильные доходы подходят для кредита")
            if forecast['analytics']['long_term_trend'] > 0:
                reasons.append("положительная динамика доходов")
                
        elif product_type == 'deposit':
            if forecast['analytics']['volatility'] > 70:
                reasons.append("подходит для сбережений при нестабильных тратах")
            if forecast_amount > 50000:
                reasons.append("достаточная сумма для открытия вклада")
        
        return ", ".join(reasons) if reasons else "подходит под ваш финансовый профиль"

# Глобальный экземпляр
recommender = ProductRecommender()