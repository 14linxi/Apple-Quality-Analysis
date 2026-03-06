#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Kafka Topic管理工具
用于创建和管理实时数据流的Kafka Topic
"""

import logging
from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import TopicAlreadyExistsError, KafkaError
import json

# 设置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KafkaTopicManager:
    """Kafka Topic管理器"""
    
    def __init__(self, kafka_servers=['192.168.28.180:9092']):
        """
        初始化Topic管理器
        
        Args:
            kafka_servers: Kafka服务器列表
        """
        self.kafka_servers = kafka_servers
        self.admin_client = None
        
    def init_admin_client(self):
        """初始化Kafka管理客户端"""
        try:
            self.admin_client = KafkaAdminClient(
                bootstrap_servers=self.kafka_servers,
                client_id='apple_quality_admin'
            )
            logger.info("Kafka管理客户端初始化成功")
            return True
        except Exception as e:
            logger.error(f"Kafka管理客户端初始化失败: {str(e)}")
            return False
    
    def create_topic(self, topic_name, num_partitions=3, replication_factor=1):
        """
        创建Kafka Topic
        
        Args:
            topic_name: Topic名称
            num_partitions: 分区数
            replication_factor: 副本因子
        """
        try:
            if not self.admin_client:
                if not self.init_admin_client():
                    return False
            
            # 创建Topic配置
            topic = NewTopic(
                name=topic_name,
                num_partitions=num_partitions,
                replication_factor=replication_factor,
                topic_configs={
                    'retention.ms': '86400000',  # 保留1天
                    'cleanup.policy': 'delete',
                    'compression.type': 'gzip'
                }
            )
            
            # 创建Topic
            result = self.admin_client.create_topics([topic])
            
            # 等待创建完成
            for topic_name, future in result.items():
                try:
                    future.result()  # 等待结果
                    logger.info(f"Topic '{topic_name}' 创建成功")
                    return True
                except TopicAlreadyExistsError:
                    logger.warning(f"Topic '{topic_name}' 已存在")
                    return True
                except Exception as e:
                    logger.error(f"创建Topic '{topic_name}' 失败: {str(e)}")
                    return False
                    
        except Exception as e:
            logger.error(f"创建Topic失败: {str(e)}")
            return False
    
    def list_topics(self):
        """列出所有Topic"""
        try:
            if not self.admin_client:
                if not self.init_admin_client():
                    return []
            
            metadata = self.admin_client.list_topics()
            topics = list(metadata)
            logger.info(f"找到 {len(topics)} 个Topic: {topics}")
            return topics
            
        except Exception as e:
            logger.error(f"列出Topic失败: {str(e)}")
            return []
    
    def delete_topic(self, topic_name):
        """删除Topic"""
        try:
            if not self.admin_client:
                if not self.init_admin_client():
                    return False
            
            result = self.admin_client.delete_topics([topic_name])
            
            for topic_name, future in result.items():
                try:
                    future.result()
                    logger.info(f"Topic '{topic_name}' 删除成功")
                    return True
                except Exception as e:
                    logger.error(f"删除Topic '{topic_name}' 失败: {str(e)}")
                    return False
                    
        except Exception as e:
            logger.error(f"删除Topic失败: {str(e)}")
            return False
    
    def test_topic_connection(self, topic_name):
        """测试Topic连接"""
        try:
            # 测试生产者
            producer = KafkaProducer(
                bootstrap_servers=self.kafka_servers,
                value_serializer=lambda x: json.dumps(x).encode('utf-8')
            )
            
            test_message = {
                'test': True,
                'message': 'Kafka连接测试',
                'timestamp': '2024-01-01T00:00:00'
            }
            
            future = producer.send(topic_name, test_message)
            future.get(timeout=10)
            producer.close()
            
            logger.info(f"Topic '{topic_name}' 生产者测试成功")
            
            # 测试消费者
            consumer = KafkaConsumer(
                topic_name,
                bootstrap_servers=self.kafka_servers,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                consumer_timeout_ms=5000,
                auto_offset_reset='latest'
            )
            
            # 尝试消费消息
            message_count = 0
            for message in consumer:
                message_count += 1
                if message_count >= 1:  # 只读取一条消息
                    break
            
            consumer.close()
            logger.info(f"Topic '{topic_name}' 消费者测试成功")
            
            return True
            
        except Exception as e:
            logger.error(f"Topic连接测试失败: {str(e)}")
            return False
    
    def setup_apple_quality_topics(self):
        """设置苹果品质相关的Topic"""
        topics_config = [
            {
                'name': 'apple_quality_changes',
                'description': '苹果品质数据变化流',
                'partitions': 3,
                'replication_factor': 1
            },
            {
                'name': 'apple_quality_processed',
                'description': '处理后的苹果品质数据',
                'partitions': 3,
                'replication_factor': 1
            },
            {
                'name': 'apple_quality_alerts',
                'description': '苹果品质异常告警',
                'partitions': 1,
                'replication_factor': 1
            }
        ]
        
        success_count = 0
        for topic_config in topics_config:
            logger.info(f"创建Topic: {topic_config['name']} - {topic_config['description']}")
            if self.create_topic(
                topic_config['name'],
                topic_config['partitions'],
                topic_config['replication_factor']
            ):
                success_count += 1
            else:
                logger.error(f"创建Topic {topic_config['name']} 失败")
        
        logger.info(f"成功创建 {success_count}/{len(topics_config)} 个Topic")
        return success_count == len(topics_config)
    
    def close(self):
        """关闭管理客户端"""
        if self.admin_client:
            self.admin_client.close()
            logger.info("Kafka管理客户端已关闭")

# 测试和初始化函数
def setup_kafka_environment():
    """设置Kafka环境"""
    manager = KafkaTopicManager()
    
    try:
        logger.info("开始设置Kafka环境...")
        
        # 初始化管理客户端
        if not manager.init_admin_client():
            logger.error("无法连接到Kafka集群")
            return False
        
        # 列出现有Topic
        existing_topics = manager.list_topics()
        logger.info(f"现有Topic: {existing_topics}")
        
        # 创建苹果品质相关Topic
        if manager.setup_apple_quality_topics():
            logger.info("Kafka环境设置成功")
            
            # 测试主要Topic连接
            if manager.test_topic_connection('apple_quality_changes'):
                logger.info("Topic连接测试成功")
                return True
            else:
                logger.error("Topic连接测试失败")
                return False
        else:
            logger.error("Topic创建失败")
            return False
            
    except Exception as e:
        logger.error(f"设置Kafka环境失败: {str(e)}")
        return False
    finally:
        manager.close()

if __name__ == "__main__":
    # 运行Kafka环境设置
    if setup_kafka_environment():
        print("✅ Kafka环境设置完成")
    else:
        print("❌ Kafka环境设置失败")
