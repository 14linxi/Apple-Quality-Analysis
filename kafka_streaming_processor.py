#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Kafka实时数据处理器
从Kafka读取实时数据变化，处理后更新数据库
"""

import json
import time
import logging
from datetime import datetime
from kafka import KafkaConsumer
from threading import Thread, Event
import pandas as pd
from data_change_manager import DataChangeManager

# 设置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KafkaStreamingProcessor:
    """Kafka实时数据处理器"""
    
    def __init__(self, kafka_config=None):
        """
        初始化Kafka实时数据处理器
        
        Args:
            kafka_config: Kafka配置
        """
        # Kafka配置
        self.kafka_config = kafka_config or {
            'bootstrap_servers': ['192.168.28.180:9092'],
            'value_deserializer': lambda x: json.loads(x.decode('utf-8')),
            'auto_offset_reset': 'latest',
            'enable_auto_commit': True,
            'group_id': 'apple_quality_processor',
            'security_protocol': 'PLAINTEXT',
            'request_timeout_ms': 30000,
            'session_timeout_ms': 30000
        }
        
        # Kafka主题
        self.input_topic = 'apple_quality_changes'
        self.output_topic = 'apple_quality_processed'
        
        # 数据管理器（用于数据库操作）
        self.data_manager = None
        
        # 控制变量
        self.running = False
        self.stop_event = Event()
        self.consumer = None
        
        # 处理统计
        self.process_stats = {
            'messages_processed': 0,
            'insert_processed': 0,
            'update_processed': 0,
            'delete_processed': 0,
            'errors': 0,
            'last_timestamp': None
        }
    
    def init_consumer(self):
        """初始化Kafka消费者"""
        try:
            logger.info(f"尝试创建Kafka消费者，配置: {self.kafka_config}")
            
            self.consumer = KafkaConsumer(
                self.input_topic,
                **self.kafka_config
            )
            
            logger.info(f"Kafka消费者初始化成功，订阅主题: {self.input_topic}")
            return True
        except Exception as e:
            logger.error(f"Kafka消费者初始化失败: {str(e)}")
            import traceback
            traceback.print_exc()
            return False
    
    def init_data_manager(self):
        """初始化数据管理器"""
        try:
            self.data_manager = DataChangeManager()
            logger.info("数据管理器初始化成功")
            return True
        except Exception as e:
            logger.error(f"数据管理器初始化失败: {str(e)}")
            return False
    
    def process_message(self, message):
        """
        处理单条Kafka消息
        
        Args:
            message: Kafka消息
        
        Returns:
            处理结果
        """
        try:
            if message is None or message.value is None:
                logger.warning("收到空消息，跳过处理")
                return None
            
            # 解析消息
            msg_data = message.value
            operation = msg_data.get('operation')
            timestamp = msg_data.get('timestamp')
            
            logger.info(f"处理 {operation} 操作，时间戳: {timestamp}")
            
            # 根据操作类型进行处理
            result = None
            if operation == 'insert':
                # 处理插入操作
                record_data = msg_data.get('data')
                if record_data and self.data_manager:
                    result = self.data_manager.insert_record(record_data, user_triggered=False)
                    if result.get('success'):
                        self.process_stats['insert_processed'] += 1
                        logger.info(f"插入记录成功: ID {result['data'].get('a_id')}")
                    else:
                        logger.error(f"插入记录失败: {result.get('error')}")
                
            elif operation == 'update':
                # 处理更新操作
                new_data = msg_data.get('new_data')
                if new_data and self.data_manager:
                    record_id = new_data.get('a_id')
                    if record_id:
                        update_data = {k: v for k, v in new_data.items() if k != 'a_id'}
                        result = self.data_manager.update_record(record_id, update_data, user_triggered=False)
                        if result.get('success'):
                            self.process_stats['update_processed'] += 1
                            logger.info(f"更新记录成功: ID {record_id}")
                        else:
                            logger.error(f"更新记录失败: {result.get('error')}")
                    else:
                        logger.error("更新操作缺少记录ID")
                
            elif operation == 'delete':
                # 处理删除操作
                record_data = msg_data.get('data')
                if record_data and self.data_manager:
                    record_id = record_data.get('a_id')
                    if record_id:
                        result = self.data_manager.delete_record(record_id, user_triggered=False)
                        if result.get('success'):
                            self.process_stats['delete_processed'] += 1
                            logger.info(f"删除记录成功: ID {record_id}")
                        else:
                            logger.error(f"删除记录失败: {result.get('error')}")
                    else:
                        logger.error("删除操作缺少记录ID")
            else:
                logger.warning(f"未知操作类型: {operation}")
            
            # 更新统计信息
            self.process_stats['messages_processed'] += 1
            self.process_stats['last_timestamp'] = datetime.now().isoformat()
            
            return result
            
        except Exception as e:
            logger.error(f"处理消息失败: {str(e)}")
            import traceback
            traceback.print_exc()
            self.process_stats['errors'] += 1
            return None
    
    def processing_loop(self):
        """处理循环"""
        try:
            logger.info("启动Kafka消息处理循环")
            
            while not self.stop_event.is_set():
                # 使用轮询方式检查新消息，设置超时时间
                message_batch = self.consumer.poll(timeout_ms=1000)
                
                if not message_batch:
                    continue
                
                # 处理获取的消息批次
                for topic_partition, messages in message_batch.items():
                    for message in messages:
                        self.process_message(message)
                
                # 提交偏移量
                self.consumer.commit()
                
            logger.info("处理循环已停止")
            
        except Exception as e:
            logger.error(f"处理循环异常: {str(e)}")
            import traceback
            traceback.print_exc()
    
    def start_processing(self):
        """启动实时数据处理"""
        try:
            logger.info("启动Kafka实时数据处理...")
            
            # 初始化消费者
            if not self.init_consumer():
                logger.error("Kafka消费者初始化失败，无法启动处理")
                return False
            
            # 初始化数据管理器
            if not self.init_data_manager():
                logger.error("数据管理器初始化失败，无法启动处理")
                return False
            
            # 启动处理线程
            self.running = True
            self.stop_event.clear()
            
            self.process_thread = Thread(target=self.processing_loop)
            self.process_thread.daemon = True
            self.process_thread.start()
            
            logger.info("Kafka实时数据处理已启动")
            return True
            
        except Exception as e:
            logger.error(f"启动数据处理失败: {str(e)}")
            return False
    
    def stop_processing(self):
        """停止实时数据处理"""
        try:
            if not self.running:
                logger.warning("处理器未运行，无需停止")
                return True
            
            logger.info("正在停止Kafka实时数据处理...")
            self.running = False
            self.stop_event.set()
            
            # 等待处理线程结束
            if hasattr(self, 'process_thread') and self.process_thread.is_alive():
                self.process_thread.join(timeout=5.0)
            
            # 关闭Kafka消费者
            if self.consumer:
                self.consumer.close()
                self.consumer = None
            
            # 关闭数据管理器
            if self.data_manager:
                self.data_manager.close()
            
            logger.info("Kafka实时数据处理已停止")
            return True
            
        except Exception as e:
            logger.error(f"停止数据处理失败: {str(e)}")
            return False
    
    def get_statistics(self):
        """获取处理统计信息"""
        return {
            'status': 'running' if self.running else 'stopped',
            'statistics': self.process_stats,
            'topic': self.input_topic,
            'timestamp': datetime.now().isoformat()
        }

# 测试函数
def test_kafka_processor():
    """测试Kafka处理器"""
    processor = KafkaStreamingProcessor()
    
    try:
        # 启动处理器
        logger.info("====== 测试Kafka实时处理器 ======")
        if processor.start_processing():
            logger.info("✓ 处理器启动成功")
            
            # 运行一段时间
            for i in range(5):  # 运行30秒
                time.sleep(6)
                stats = processor.get_statistics()
                logger.info(f"运行状态 ({i+1}/5): {stats}")
            
            # 停止处理器
            processor.stop_processing()
            logger.info("✓ 处理器已停止")
            
            # 显示最终统计
            final_stats = processor.get_statistics()
            logger.info("====== 最终统计 ======")
            logger.info(f"处理消息总数: {final_stats['statistics']['messages_processed']}")
            logger.info(f"插入操作: {final_stats['statistics']['insert_processed']}")
            logger.info(f"更新操作: {final_stats['statistics']['update_processed']}")
            logger.info(f"删除操作: {final_stats['statistics']['delete_processed']}")
            logger.info(f"错误数: {final_stats['statistics']['errors']}")
            
            return final_stats['statistics']['messages_processed'] > 0
        else:
            logger.error("✗ 处理器启动失败")
            return False
        
    except Exception as e:
        logger.error(f"测试过程中出错: {str(e)}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        # 确保处理器停止
        if processor.running:
            processor.stop_processing()

if __name__ == "__main__":
    # 测试运行
    if test_kafka_processor():
        print("\n✓ Kafka实时处理器测试通过")
    else:
        print("\n✗ Kafka实时处理器测试失败") 