from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
import json
from kafka import KafkaProducer, KafkaConsumer
import boto3
from botocore.exceptions import ClientError


class QueueService(ABC):
    @abstractmethod
    def send(self, message: Any, **kwargs) -> Dict[str, Any]:
        pass
    
    @abstractmethod
    def receive(self, **kwargs) -> List[Dict[str, Any]]:
        pass
    
    @abstractmethod
    def delete(self, receipt_handle: str) -> bool:
        pass


class KafkaQueue(QueueService):
    def __init__(self, bootstrap_servers: str = 'localhost:9092', 
                 topic: str = 'default', **config):
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            **config.get('producer_config', {})
        )
        self.consumer = None
        self.consumer_config = {
            'bootstrap_servers': bootstrap_servers,
            'value_deserializer': lambda m: json.loads(m.decode('utf-8')),
            'auto_offset_reset': 'earliest',
            **config.get('consumer_config', {})
        }
    
    def send(self, message: Any, **kwargs) -> Dict[str, Any]:
        topic = kwargs.get('topic', self.topic)
        future = self.producer.send(topic, message)
        metadata = future.get(timeout=10)
        return {
            'topic': metadata.topic,
            'partition': metadata.partition,
            'offset': metadata.offset
        }
    
    def receive(self, **kwargs) -> List[Dict[str, Any]]:
        if not self.consumer:
            group_id = kwargs.get('group_id', 'default-group')
            topics = kwargs.get('topics', [self.topic])
            self.consumer = KafkaConsumer(
                *topics,
                group_id=group_id,
                **self.consumer_config
            )
        
        timeout_ms = kwargs.get('timeout_ms', 1000)
        max_records = kwargs.get('max_records', 10)
        
        records = self.consumer.poll(timeout_ms=timeout_ms, max_records=max_records)
        messages = []
        for topic_partition, records_list in records.items():
            for record in records_list:
                messages.append({
                    'body': record.value,
                    'receipt_handle': f"{record.topic}:{record.partition}:{record.offset}",
                    'topic': record.topic,
                    'partition': record.partition,
                    'offset': record.offset
                })
        return messages
    
    def delete(self, receipt_handle: str) -> bool:
        # Kafka commits handled via consumer.commit()
        if self.consumer:
            self.consumer.commit()
            return True
        return False
    
    def close(self):
        self.producer.close()
        if self.consumer:
            self.consumer.close()


class SQSQueue(QueueService):
    def __init__(self, queue_url: str, region_name: str = 'ap-south-2', **config):
        self.queue_url = queue_url
        self.client = boto3.client('sqs', region_name=region_name, **config)
    
    def send(self, message: Any, **kwargs) -> Dict[str, Any]:
        message_body = json.dumps(message) if not isinstance(message, str) else message
        try:
            response = self.client.send_message(
                QueueUrl=self.queue_url,
                MessageBody=message_body,
                **kwargs
            )
            return {
                'message_id': response['MessageId'],
                'md5': response['MD5OfMessageBody']
            }
        except ClientError as e:
            raise RuntimeError(f"SQS send failed: {e}")
    
    def receive(self, **kwargs) -> List[Dict[str, Any]]:
        max_messages = kwargs.get('max_messages', 10)
        wait_time = kwargs.get('wait_time_seconds', 0)
        
        try:
            response = self.client.receive_message(
                QueueUrl=self.queue_url,
                MaxNumberOfMessages=max_messages,
                WaitTimeSeconds=wait_time,
                **{k: v for k, v in kwargs.items() 
                   if k not in ['max_messages', 'wait_time_seconds']}
            )
            
            messages = []
            for msg in response.get('Messages', []):
                messages.append({
                    'body': json.loads(msg['Body']) if self._is_json(msg['Body']) else msg['Body'],
                    'receipt_handle': msg['ReceiptHandle'],
                    'message_id': msg['MessageId']
                })
            return messages
        except ClientError as e:
            raise RuntimeError(f"SQS receive failed: {e}")
    
    def delete(self, receipt_handle: str) -> bool:
        try:
            self.client.delete_message(
                QueueUrl=self.queue_url,
                ReceiptHandle=receipt_handle
            )
            return True
        except ClientError:
            return False
    
    @staticmethod
    def _is_json(s: str) -> bool:
        try:
            json.loads(s)
            return True
        except (ValueError, TypeError):
            return False


class QueueWrapper:
    def __init__(self, service_type: str, **config):
        if service_type.lower() == 'kafka':
            self.queue = KafkaQueue(**config)
        elif service_type.lower() == 'sqs':
            self.queue = SQSQueue(**config)
        else:
            raise ValueError(f"Unknown service type: {service_type}")
    
    def send(self, message: Any, **kwargs) -> Dict[str, Any]:
        return self.queue.send(message, **kwargs)
    
    def receive(self, **kwargs) -> List[Dict[str, Any]]:
        return self.queue.receive(**kwargs)
    
    def delete(self, receipt_handle: str) -> bool:
        return self.queue.delete(receipt_handle)
    
    def close(self):
        if hasattr(self.queue, 'close'):
            self.queue.close()


# Usage:
# kafka_queue = QueueWrapper('kafka', bootstrap_servers='localhost:9092', topic='my-topic')
# sqs_queue = QueueWrapper('sqs', queue_url='https://sqs.ap-south-2.amazonaws.com/123456789/my-queue')
# 
# kafka_queue.send({'data': 'test'})
# messages = kafka_queue.receive()
# kafka_queue.delete(messages[0]['receipt_handle'])