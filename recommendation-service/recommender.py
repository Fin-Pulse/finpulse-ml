import logging
import numpy as np
from typing import List, Dict, Any
from dataclasses import dataclass, asdict
import json

logger = logging.getLogger(__name__)

@dataclass
class BankServiceRecommendation:
    product: Dict[str, Any]
    score: float
    reasons: List[str]
    suitability: str
    product_id: str
    
    def to_dict(self):
        """Преобразование в словарь для сериализации"""
        return {
            'product': self.product,
            'score': self.score,
            'reasons': self.reasons,
            'suitability': self.suitability,
            'product_id': self.product_id
        }

class ProductRecommender:
    def __init__(self):
        self.z_score_thresholds = {'low_risk': -1.0, 'medium_risk': 1.0, 'high_risk': 2.0}
    
    def _calculate_z_score(self, forecast_data: Dict[str, Any]) -> float:
        """Вычисляем Z-score"""
        # ИСПРАВЛЕНИЕ: правильное получение forecast значения
        forecast_val = forecast_data['forecast']  # Получаем числовое значение прогноза
        mean = forecast_data['analysis']['statistics']['mean'] 
        std = forecast_data['analysis']['statistics']['std'] 
        
        if std and std > 0:
            return (forecast_val - mean) / std
        return 0.0
    
    def _safe_lower(self, text: Any) -> str:
        """Безопасное преобразование в нижний регистр"""
        try:
            if text is None:
                return ""
            return str(text).lower()
        except:
            return ""
    
    def _evaluate_product(self, product: Dict[str, Any], characteristics: Dict[str, Any]) -> Dict[str, Any]:
        """Оцениваем один продукт"""
        score = 0.0
        reasons = []
        
        product_type = product['productType'] 
        product_name = product['productName'] 
        description = product['description'] 
        
        z_score = characteristics['z_score'] 
        is_high_volatility = characteristics['is_high_volatility'] 
        is_significant_increase = characteristics['is_significant_increase'] 
        has_anomaly = characteristics['has_anomaly']
        
        # Базовые баллы за тип продукта
        if any(word in product_type for word in ['deposit', 'вклад']):
            score += 1.0
            reasons.append("Сберегательный продукт")
            
        elif any(word in product_type for word in ['credit', 'loan', 'кредит']):
            score += 1.0
            reasons.append("Кредитный продукт")
            
        elif any(word in product_type for word in ['debit', 'card', 'дебет']):
            score += 1.0
            reasons.append("Платежный продукт")
        
        # Дополнительные баллы на основе Z-score
        if z_score > 2.0:  # Очень высокие траты
            if any(word in product_type for word in ['credit', 'loan']):
                score += 2.0
                reasons.append("Поможет с высокими расходами")
                
        elif z_score < -1.0:  # Низкие траты
            if any(word in product_type for word in ['deposit', 'вклад']):
                score += 2.0
                reasons.append("Идеален для накоплений")
                
        elif -1.0 <= z_score <= 1.0:  # Нормальные траты
            if any(word in product_type for word in ['debit', 'card']):
                score += 1.5
                reasons.append("Подходит для регулярных трат")
        
        # Учитываем дополнительные факторы
        if is_high_volatility and any(word in product_type for word in ['debit', 'card']):
            score += 1.0
            reasons.append("Гибкое управление при нестабильности")
            
        if is_significant_increase and any(word in product_type for word in ['credit', 'loan']):
            score += 1.5
            reasons.append("Покрывает рост расходов")
            
        if has_anomaly and any(word in product_type for word in ['credit']):
            score += 1.0
            reasons.append("Резерв на непредвиденное")
                
        return {'score': score, 'reasons': reasons}
    
    def _analyze_forecast_characteristics(self, forecast_data: Dict[str, Any]) -> Dict[str, Any]:
        """Анализируем характеристики прогноза с улучшенной обработкой"""
        
        z_score = self._calculate_z_score(forecast_data)
        
        insights = forecast_data['recommendations']['insights']
        
        has_anomaly = False
        has_high_volatility = False
        has_increase = False
        
        for insight in insights:
            insight_type = self._safe_lower(insight.get('type', ''))
            if 'anomalous' in insight_type or 'аномал' in insight_type:
                has_anomaly = True
            if 'high_volatility' in insight_type or 'волатильность' in insight_type:
                has_high_volatility = True
            if 'significant_increase' in insight_type or 'рост' in insight_type:
                has_increase = True
        
        # ИСПРАВЛЕНИЕ: правильное получение change_pct
        change_pct = forecast_data['re']['change_pct']
        forecast_amount = forecast_data['forecast']
        volatility = forecast_data['analysis']['volatility']

        return {
            'z_score': z_score,
            'is_high_volatility': has_high_volatility,
            'is_significant_increase': has_increase or (change_pct and change_pct > 100),
            'has_anomaly': has_anomaly,
            'forecast_amount': forecast_amount,
            'change_percentage': change_pct,
            'volatility': volatility
        }
    
    def recommend(self, forecast_data: Dict[str, Any], products: List[Dict[str, Any]]) -> List[BankServiceRecommendation]:
        """Основной метод рекомендаций"""
        try:
            characteristics = self._analyze_forecast_characteristics(forecast_data['result'])
            recommendations = []
            
            for i, product in enumerate(products):
                try:
                    evaluation = self._evaluate_product(product, characteristics)
                    recommendations.append(BankServiceRecommendation(
                        product=product,
                        score=evaluation['score'],
                        reasons=evaluation['reasons'],
                        suitability='medium',
                        product_id=product['productId'] 
                    ))
                except Exception as e:
                    print(f"⚠️ Ошибка при оценке продукта {i}: {e}")
                    continue
            
            if recommendations:
                scores = [r.score for r in recommendations if r.score > 0]
                if scores:
                    max_score = max(scores)
                    for rec in recommendations:
                        ratio = rec.score / max_score if max_score > 0 else 0
                        if ratio >= 0.7:
                            rec.suitability = 'high'
                        elif ratio >= 0.4:
                            rec.suitability = 'medium'
                        else:
                            rec.suitability = 'low'
            
            # Сортируем по убыванию score
            result = sorted(recommendations, key=lambda x: x.score, reverse=True)
            
            # ФИЛЬТРАЦИЯ: оставляем только лучший продукт каждого типа
            best_by_type = {}
            for rec in result:
                product_type = rec.product['productType']
                # Если тип еще не встречался или текущий продукт имеет больший score
                if product_type not in best_by_type or rec.score > best_by_type[product_type].score:
                    best_by_type[product_type] = rec
            
            # Возвращаем только лучшие продукты каждого типа
            filtered_result = list(best_by_type.values())
            
            # Сортируем результат по score (на всякий случай)
            filtered_result = sorted(filtered_result, key=lambda x: x.score, reverse=True)
            
            print(f"Сформировано {len(filtered_result)} рекомендаций после фильтрации (было {len(result)})")
            return filtered_result
            
        except Exception as e:
            print(f"Критическая ошибка: {e}")
            import traceback
            traceback.print_exc()
            return []


recommender = ProductRecommender()