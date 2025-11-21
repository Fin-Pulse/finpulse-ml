import pandas as pd
import numpy as np
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union

class WeeklySpendingForecaster:
    
    def __init__(self, min_weeks: int = 4, outlier_threshold: float = 0.95):
        self.min_weeks = min_weeks
        self.outlier_threshold = outlier_threshold
    
    def create_weekly_data(self, transactions_df: pd.DataFrame) -> pd.DataFrame:
        df = transactions_df.copy()
        
        self._validate_transactions_data(df)
        
        df = self._prepare_amounts(df)
        
        expenses = self._filter_expenses(df)
        
        if len(expenses) == 0:
            raise ValueError("Не найдено транзакций с расходами")
        
        weekly = self._create_weekly_aggregates(expenses)
        
        weekly = self._clean_weekly_data(weekly)
        
        return weekly
    
    def _validate_transactions_data(self, df: pd.DataFrame) -> None:
        if 'date' not in df.columns:
            raise ValueError("Данные транзакций должны содержать колонку 'date'")
        
        if 'absolute_amount' not in df.columns and 'amount' not in df.columns:
            raise ValueError("Данные транзакций должны содержать колонку 'amount' или 'absolute_amount'")
    
    def _prepare_amounts(self, df: pd.DataFrame) -> pd.DataFrame:
        if 'absolute_amount' not in df.columns:
            df['absolute_amount'] = df['amount'].abs()
        
        df['date'] = pd.to_datetime(df['date']).dt.normalize()
        return df
    
    def _filter_expenses(self, df: pd.DataFrame) -> pd.DataFrame:
        if 'is_expense' in df.columns:
            return df[df['is_expense'] == True].copy()
        else:
            return df[df['amount'] < 0].copy()
    
    def _create_weekly_aggregates(self, expenses: pd.DataFrame) -> pd.DataFrame:
        expenses['week_start'] = expenses['date'] - pd.to_timedelta(
            expenses['date'].dt.dayofweek, unit='D'
        )
        
        weekly = expenses.groupby('week_start', as_index=False).agg({
            'absolute_amount': 'sum'
        })
        weekly.columns = ['week_start', 'expenses_total']
        
        return weekly
    
    def _clean_weekly_data(self, weekly: pd.DataFrame) -> pd.DataFrame:
        weekly = weekly[weekly['expenses_total'] > 100].copy()
        
        if len(weekly) > 4:
            weekly = self._handle_outliers_iqr(weekly)
        
        return weekly.sort_values('week_start').reset_index(drop=True)
    
    def _handle_outliers_iqr(self, weekly: pd.DataFrame) -> pd.DataFrame:
        Q1 = weekly['expenses_total'].quantile(0.25)
        Q3 = weekly['expenses_total'].quantile(0.75)
        IQR = Q3 - Q1
        
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        
        weekly['expenses_total'] = weekly['expenses_total'].clip(
            lower=lower_bound, 
            upper=upper_bound
        )
        
        return weekly
    
    def forecast(self, weekly_data: pd.DataFrame, method: str = "robust_ma") -> Dict:
        values = weekly_data['expenses_total'].values
        
        if method == "robust_ma":
            return self._robust_moving_average(values)
        elif method == "weighted_ma":
            return self._weighted_moving_average(values)
        else:
            return self._robust_moving_average(values)
    
    def _robust_moving_average(self, values: np.ndarray, weeks: int = 4) -> Dict:
        recent_values = values[-weeks:]
        forecast = np.median(recent_values)
        
        return self._calculate_forecast_metrics(values, forecast, "robust_moving_average")
    
    def _weighted_moving_average(self, values: np.ndarray, weeks: int = 4) -> Dict:
        if len(values) < weeks:
            weeks = len(values)
        
        weights = np.linspace(1, 2, weeks)
        recent_values = values[-weeks:]
        
        forecast = np.average(recent_values, weights=weights)
        
        return self._calculate_forecast_metrics(values, forecast, "weighted_moving_average")
    
    def _calculate_forecast_metrics(self, values: np.ndarray, forecast: float, method: str) -> Dict:
        recent_std = np.std(values[-min(8, len(values)):])
        mean_value = np.mean(values)
        
        volatility = recent_std / mean_value if mean_value > 0 else 0.5
        
        if volatility > 0.5:
            lower = max(forecast * 0.3, forecast - 1.5 * recent_std)
            upper = min(forecast * 2.5, forecast + 1.5 * recent_std)
        else:
            lower = max(forecast * 0.7, forecast - recent_std)
            upper = min(forecast * 1.5, forecast + recent_std)
        
        last_week = values[-1]
        change = forecast - last_week
        change_pct = (change / last_week) * 100 if last_week != 0 else 0
        
        return {
            'forecast': float(forecast),
            'confidence_interval': [float(lower), float(upper)],
            'last_week': float(last_week),
            'change': float(change),
            'change_pct': float(change_pct),
            'method': method
        }
    
    def analyze_patterns(self, weekly_data: pd.DataFrame) -> Dict:
        values = weekly_data['expenses_total'].values
        
        stats = self._calculate_statistics(values)
        trends = self._calculate_trends(values)
        seasonality = self._detect_seasonality(weekly_data)
        
        return {
            'statistics': stats,
            'trends': trends,
            'seasonality': seasonality,
            'volatility': self._calculate_volatility(values),
            'last_week_deviation': self._calculate_last_week_deviation(values)
        }
    
    def _calculate_statistics(self, values: np.ndarray) -> Dict:
        return {
            'period_weeks': len(values),
            'mean': float(np.mean(values)),
            'median': float(np.median(values)),
            'std': float(np.std(values)),
            'min': float(np.min(values)),
            'max': float(np.max(values)),
            'q25': float(np.percentile(values, 25)),
            'q75': float(np.percentile(values, 75))
        }
    
    def _calculate_trends(self, values: np.ndarray) -> Dict:
        trends = {}
        
        if len(values) >= 4:
            recent_avg = np.median(values[-4:])
            previous_avg = np.median(values[-8:-4]) if len(values) >= 8 else np.median(values[:-4])
            
            if previous_avg > 0:
                trends['short_term_trend_pct'] = float(((recent_avg - previous_avg) / previous_avg) * 100)
            
            if len(values) >= 8:
                x = np.arange(len(values))
                slope = np.polyfit(x, values, 1)[0]
                trends['long_term_trend'] = float(slope)
        
        return trends
    
    def _detect_seasonality(self, weekly_data: pd.DataFrame) -> Dict:

        if len(weekly_data) < 12:
            return {'detected': False, 'message': 'Недостаточно данных для анализа сезонности'}
        
        weekly_data['month'] = weekly_data['week_start'].dt.month
        monthly_avg = weekly_data.groupby('month')['expenses_total'].mean()
        
        return {
            'detected': len(monthly_avg.unique()) > 1,
            'monthly_pattern': monthly_avg.to_dict(),
            'peak_month': int(monthly_avg.idxmax()) if not monthly_avg.empty else None
        }
    
    def _calculate_volatility(self, values: np.ndarray) -> float:
        return float((np.std(values) / np.mean(values)) * 100) if np.mean(values) > 0 else 0.0
    
    def _calculate_last_week_deviation(self, values: np.ndarray) -> float:
        return float(((values[-1] - np.mean(values)) / np.mean(values)) * 100) if np.mean(values) > 0 else 0.0


def get_weekly_forecast_api(transactions_data: Union[Dict, List, pd.DataFrame]) -> Dict:

    try:
        
        forecaster = WeeklySpendingForecaster()

        

        if isinstance(transactions_data, dict):
            transactions_df = pd.DataFrame([transactions_data])
        elif isinstance(transactions_data, list):
            transactions_df = pd.DataFrame(transactions_data)
        else:
            transactions_df = transactions_data
            
        weekly_data = forecaster.create_weekly_data(transactions_df)
        
        if len(weekly_data) < forecaster.min_weeks:
            return {
                'success': False,
                'error': f'Недостаточно данных для прогнозирования (нужно минимум {forecaster.min_weeks} недели)',
                'available_weeks': len(weekly_data),
                'minimum_required': forecaster.min_weeks
            }
        
        forecast_result = forecaster.forecast(weekly_data, method="robust_ma")
        
        analysis = forecaster.analyze_patterns(weekly_data)
        
        recommendations = generate_recommendations(weekly_data, forecast_result, analysis)
        
        return {
            'success': True,
            'timestamp': datetime.now().isoformat(),
            'data_period': {
                'start_date': weekly_data['week_start'].min().strftime('%Y-%m-%d'),
                'end_date': weekly_data['week_start'].max().strftime('%Y-%m-%d'),
                'total_weeks': len(weekly_data)
            },
            'forecast': forecast_result,
            'analysis': analysis,
            'recommendations': recommendations,
            'next_steps': [
                f"Запланируйте {forecast_result['forecast']:.0f} руб. на следующую неделю",
                f"Подготовьте резерв до {forecast_result['confidence_interval'][1]:.0f} руб.",
                "Сравните фактические траты с прогнозом", 
                "Обновите прогноз через неделю с новыми данными"
            ],
            'metadata': {
                'forecaster_version': '1.1',
                'data_quality_check': 'passed',
                'outliers_processed': True
            }
        }
        
    except Exception as e:
        return {
            'success': False,
            'error': f"Ошибка прогнозирования: {str(e)}",
            'timestamp': datetime.now().isoformat(),
            'metadata': {
                'forecaster_version': '1.1',
                'data_quality_check': 'failed'
            }
        }

def generate_recommendations(weekly_data, forecast_result, analysis):
    values = weekly_data['expenses_total'].values
    forecast = forecast_result['forecast']
    mean_spending = np.mean(values)
    
    recommendations = {
        'budget_planning': {
            'recommended_budget': float(forecast),
            'safe_range': forecast_result['confidence_interval'],
            'reserve_amount': float(forecast_result['confidence_interval'][1])
        },
        'insights': [],
        'financial_tips': {
            'monthly_estimate': float(mean_spending * 4.33),
            'recommended_reserve': float(mean_spending * 2),
            'savings_goal': float(mean_spending * 0.1)
        }
    }
    

    reasonable_change_threshold = mean_spending * 0.3 
    
    change = forecast_result['change']
    if abs(change) > reasonable_change_threshold:
        if change < 0:
            recommendations['insights'].append({
                'type': 'significant_decrease',
                'message': f'Ожидается снижение трат на {abs(change):.0f} руб.',
                'advice': 'Можете рассмотреть дополнительные сбережения'
            })
        else:
            recommendations['insights'].append({
                'type': 'significant_increase', 
                'message': f'Ожидается рост трат на {change:.0f} руб.',
                'advice': 'Будьте готовы к повышенным расходам'
            })
    
    if analysis['volatility'] > 50:
        recommendations['insights'].append({
            'type': 'high_volatility',
            'message': f'Высокая волатильность трат ({analysis["volatility"]:.1f}%)',
            'advice': 'Создайте буферный фонд и планируйте бюджет с запасом'
        })
    

    if abs(analysis['last_week_deviation']) > 50:
        if analysis['last_week_deviation'] > 0:
            recommendations['insights'].append({
                'type': 'anomalous_week',
                'message': 'Последняя неделя была аномально высокой',
                'advice': 'Вероятно, были разовые траты. Следующая неделя ожидается ближе к среднему уровню'
            })
        else:
            recommendations['insights'].append({
                'type': 'anomalous_week_low',
                'message': 'Последняя неделя была аномально низкой', 
                'advice': 'Возможно, были пропущены некоторые траты. Будьте внимательны на следующей неделе'
            })
    
    return recommendations
