import logging
import pandas as pd
from weekly_spending_forecaster import get_weekly_forecast_api
from minio_storage import MinioStorage

logger = logging.getLogger(__name__)

class MLProcessor:
    def __init__(self):
        self.minio_storage = MinioStorage()
    
    def process_user_forecast(self, user_id, bank_client_id, db_manager, kafka_consumer=None):
        try:
            
            transactions_df = db_manager.get_user_transactions(user_id, bank_client_id)
            
            if transactions_df is None:
                return {
                    'success': False,
                    'error': 'Нет данных о транзакциях'
                }
            
            required_columns = ['date', 'amount', 'is_expense']
            missing_columns = [col for col in required_columns if col not in transactions_df.columns]
            if missing_columns:
                logger.error(f"Отсутствуют обязательные колонки: {missing_columns}")
                return {
                    'success': False,
                    'error': f'Отсутствуют данные: {missing_columns}'
                }
            
            expenses_count = transactions_df[transactions_df['is_expense'] == True].shape[0]
            income_count = transactions_df[transactions_df['is_expense'] == False].shape[0]
        
            if expenses_count < 4:
                return {
                    'success': False,
                    'error': f'Недостаточно данных о расходах для прогнозирования. Доступно транзакций расходов: {expenses_count}'
                }
            
            forecast_result = get_weekly_forecast_api(transactions_df)
            
            if forecast_result['success']:
                
                if 'category' in transactions_df.columns:
                    charts_data = self.generate_charts(user_id, transactions_df, forecast_result)
                    forecast_result['charts'] = charts_data
                else:
                    forecast_result['charts'] = {}
                
                success = db_manager.save_forecast(user_id, bank_client_id, forecast_result)
                    
            return forecast_result
            
        except Exception as e:
            logger.error(f"Критическая ошибка обработки user_id {user_id}, bank_client_id {bank_client_id}: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def generate_charts(self, user_id, transactions_df, forecast_result):
        charts_data = {}
        
        try:
            if 'category' in transactions_df.columns:
                pie_chart_url = self.generate_pie_chart(user_id, transactions_df)
                if pie_chart_url:
                    charts_data['pie_chart'] = pie_chart_url
            
        except Exception as e:
            logger.error(f"Ошибка генерации графиков: {e}")
        
        return charts_data
    
    def generate_pie_chart(self, user_id, transactions_df, days=14, threshold_percent=3):
        try:
            import plotly.express as px
            
            last_date = pd.to_datetime(transactions_df['date'].max())
            start_date = last_date - pd.Timedelta(days=days)
            
            period_expenses = transactions_df[
                (transactions_df['date'] >= start_date) & 
                (transactions_df['date'] <= last_date) &
                (transactions_df['is_expense'] == True)
            ]
            
            if period_expenses.empty:
                return None
            
            category_data = period_expenses.groupby('category')['absolute_amount'].sum().reset_index()
            category_data = category_data.sort_values('absolute_amount', ascending=False)
            total_amount = category_data['absolute_amount'].sum()
            
            category_data['percent'] = (category_data['absolute_amount'] / total_amount) * 100
            
            main_categories = category_data[category_data['percent'] >= threshold_percent]
            other_categories = category_data[category_data['percent'] < threshold_percent]
            
            if not other_categories.empty:
                other_sum = other_categories['absolute_amount'].sum()
                other_percent = (other_sum / total_amount) * 100
                
                final_data = pd.concat([
                    main_categories,
                    pd.DataFrame([{
                        'category': 'Другие',
                        'absolute_amount': other_sum,
                        'percent': other_percent
                    }])
                ], ignore_index=True)
            else:
                final_data = main_categories
            
            final_data = final_data.sort_values('absolute_amount', ascending=False)
            
            fig = px.pie(
                final_data,
                values='absolute_amount',
                names='category',
                title=f'Распределение трат за {days} дней',
                color_discrete_sequence=px.colors.qualitative.Set3
            )
            
            fig.update_traces(
                textposition='inside',
                textinfo='percent+label',
                hovertemplate='<b>%{label}</b><br>%{value:,.0f} руб.<br>(%{percent})',
                marker=dict(line=dict(color='white', width=2))
            )
            
            fig.update_layout(
                font_size=12,
                showlegend=True,
                height=500
            )
            
            chart_url = self.minio_storage.save_plotly_chart(user_id, 'pie_chart', fig)
            return chart_url
            
        except Exception as e:
            logger.error(f"Ошибка генерации pie chart: {e}")
            return None
