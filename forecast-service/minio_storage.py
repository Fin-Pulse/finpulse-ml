# minio_storage.py
import boto3
from io import BytesIO
import logging
from datetime import datetime
import json
from config import MINIO_CONFIG

logger = logging.getLogger(__name__)

class MinioStorage:
    def __init__(self):
        try:
            self.endpoint = MINIO_CONFIG['endpoint']
            self.access_key = MINIO_CONFIG['access_key']
            self.secret_key = MINIO_CONFIG['secret_key']
            self.bucket_name = MINIO_CONFIG['bucket_name']
            self.public_url = MINIO_CONFIG['public_url']
            
            self.s3_client = boto3.client(
                's3',
                endpoint_url=self.endpoint,
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                config=boto3.session.Config(signature_version='s3v4')
            )
            self._ensure_bucket_exists()
            self._set_bucket_public_policy()
        except Exception as e:
            logger.error(f"Ошибка инициализации MinioStorage: {e}")
            self.s3_client = None
    
    def _ensure_bucket_exists(self):
        if not self.s3_client:
            return
            
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
        except Exception as e:
            try:
                self.s3_client.create_bucket(Bucket=self.bucket_name)
            except Exception as create_error:
                logger.error(f"Ошибка создания bucket: {create_error}")
    
    def _set_bucket_public_policy(self):
        if not self.s3_client:
            return
            
        try:
            public_policy = {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"AWS": "*"},
                        "Action": [
                            "s3:GetObject",
                            "s3:GetObjectVersion"
                        ],
                        "Resource": f"arn:aws:s3:::{self.bucket_name}/*"
                    }
                ]
            }
            
            self.s3_client.put_bucket_policy(
                Bucket=self.bucket_name,
                Policy=json.dumps(public_policy)
            )
            
        except Exception as e:
            logger.error(f"Ошибка установки публичного доступа: {e}")
            try:
                self.s3_client.put_bucket_acl(
                    Bucket=self.bucket_name,
                    ACL='public-read'
                )
            except Exception as acl_error:
                logger.error(f"Ошибка установки ACL: {acl_error}")
    
    def save_plotly_chart(self, user_id: str, chart_type: str, fig) -> str:
        if not self.s3_client:
            logger.warning("Minio не доступен, пропускаем сохранение графика")
            return None
            
        try:
            try:
                img_bytes = fig.to_image(format="png", width=800, height=500)
            except Exception as kaleido_error:

                img_bytes = self._plotly_to_image_fallback(fig)
                if img_bytes is None:
                    logger.error(f"Не удалось создать изображение через fallback")
                    return None
            
            timestamp = int(datetime.now().timestamp())
            filename = f"{user_id}/{chart_type}_{timestamp}.png"
            
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=filename,
                Body=img_bytes,
                ContentType='image/png',
                ACL='public-read'
            )
            
            return filename
            
        except Exception as e:
            logger.error(f"Ошибка сохранения chart в Minio: {e}")
            return None
    
    def _plotly_to_image_fallback(self, fig):
        try:
            import matplotlib
            matplotlib.use('Agg')
            import matplotlib.pyplot as plt
            import numpy as np
            
            if not fig.data:
                return None
            
            data = fig.data[0]
            if data.type != 'pie':
                return None
            
            values = data.values
            labels = data.labels
            
            if hasattr(values, 'tolist'):
                values = values.tolist()
            elif not isinstance(values, list):
                values = list(values)
            
            if hasattr(labels, 'tolist'):
                labels = labels.tolist()
            elif not isinstance(labels, list):
                labels = list(labels)
            
            if not values or not labels or len(values) != len(labels):
                return None
            
            fig_mpl, ax = plt.subplots(figsize=(8, 5))
            colors = plt.cm.Set3(np.linspace(0, 1, len(values)))
            
            wedges, texts, autotexts = ax.pie(
                values, 
                labels=labels, 
                autopct='%1.1f%%',
                colors=colors,
                startangle=90
            )
            
            for autotext in autotexts:
                autotext.set_color('black')
                autotext.set_fontweight('bold')
            
            title_text = 'Chart'
            if fig.layout and fig.layout.title:
                if hasattr(fig.layout.title, 'text'):
                    title_text = fig.layout.title.text
                elif isinstance(fig.layout.title, str):
                    title_text = fig.layout.title
            
            ax.set_title(title_text, fontsize=14)
            
            img_buffer = BytesIO()
            fig_mpl.savefig(img_buffer, format='png', dpi=100, bbox_inches='tight')
            img_buffer.seek(0)
            img_bytes = img_buffer.getvalue()
            plt.close(fig_mpl)
            
            return img_bytes
            
        except Exception as e:
            logger.error(f"Ошибка в fallback методе: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return None
    
    def get_public_url(self, object_path: str) -> str:
        if not object_path:
            return None
        return f"{self.public_url}/{object_path}"