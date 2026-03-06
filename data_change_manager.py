#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据变化管理器
提供数据的增删改查功能，使用简化的Spark操作
"""

import json
import logging
from datetime import datetime
import uuid
from pyhive import hive

# 设置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataChangeManager:
    """数据变化管理器"""

    def __init__(self):
        """初始化数据变化管理器"""
        self.hive_config = {
            'host': '192.168.28.180',
            'port': 10000,
            'username': 'root',
            'database': 'test'
        }
        self.change_log_table = "test.apple_quality_changes_log"
        self.change_log_available = True  # 启用变化记录表，用于实时更新检测

        # 初始化Spark处理器用于复杂操作
        try:
            from spark_data_processor import SparkDataProcessor
            self.spark_processor = SparkDataProcessor(mode="local")
            self.use_spark = True
            logger.info("Spark处理器初始化成功")
        except Exception as e:
            self.spark_processor = None
            self.use_spark = False
            logger.warning(f"Spark处理器初始化失败，将使用纯Hive操作: {str(e)}")

        # 初始化Kafka生产者
        self.kafka_producer = None
        self.init_kafka_producer()

        # 初始化连接
        self.init_connections()
        
    def init_connections(self):
        """初始化连接"""
        try:
            # 测试Hive连接
            self.test_hive_connection()
            logger.info("Hive连接初始化成功")

            # 初始化Spark（如果可用）
            if self.use_spark and self.spark_processor:
                if self.spark_processor.init_spark():
                    logger.info("Spark连接初始化成功")
                else:
                    logger.warning("Spark连接初始化失败，将使用纯Hive操作")
                    self.use_spark = False

            # 创建变化记录表，用于实时更新检测
            self.create_change_log_table()

        except Exception as e:
            logger.error(f"初始化连接失败: {str(e)}")

    def get_hive_connection(self):
        """获取Hive连接"""
        try:
            connection = hive.Connection(
                host=self.hive_config['host'],
                port=self.hive_config['port'],
                username=self.hive_config['username'],
                database=self.hive_config['database']
            )
            return connection
        except Exception as e:
            logger.error(f"获取Hive连接失败: {str(e)}")
            return None

    def test_hive_connection(self):
        """测试Hive连接"""
        connection = self.get_hive_connection()
        if connection:
            cursor = connection.cursor()
            cursor.execute("SHOW TABLES")
            cursor.close()
            connection.close()
            return True
        return False

    def init_kafka_producer(self):
        """初始化Kafka生产者"""
        try:
            from kafka import KafkaProducer
            import json

            self.kafka_producer = KafkaProducer(
                bootstrap_servers=['192.168.28.180:9092'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: str(k).encode('utf-8') if k else None
            )
            logger.info("Kafka生产者初始化成功")
            return True
        except Exception as e:
            logger.warning(f"Kafka生产者初始化失败: {str(e)}")
            self.kafka_producer = None
            return False

    def send_change_to_kafka(self, change_message):
        """发送变化消息到Kafka"""
        try:
            if self.kafka_producer:
                topic = 'apple_quality_changes'
                self.kafka_producer.send(topic, value=change_message)
                self.kafka_producer.flush()
                logger.info(f"变化消息已发送到Kafka: {change_message.get('operation', 'unknown')}")
            else:
                logger.warning("Kafka生产者未初始化，跳过消息发送")
        except Exception as e:
            logger.error(f"发送Kafka消息失败: {str(e)}")
    
    def create_change_log_table(self):
        """创建变化记录表"""
        try:
            # 优先使用Spark创建表
            if self.use_spark and self.spark_processor and self.spark_processor.spark:
                try:
                    create_table_sql = f"""
                    CREATE TABLE IF NOT EXISTS {self.change_log_table} (
                        change_id STRING,
                        record_id INT,
                        operation STRING,
                        change_time STRING,
                        old_data STRING,
                        new_data STRING,
                        changed_fields STRING,
                        user_triggered BOOLEAN
                    )
                    USING HIVE
                    """

                    self.spark_processor.spark.sql(create_table_sql)
                    logger.info(f"变化记录表 {self.change_log_table} 使用Spark创建成功")
                    return
                except Exception as spark_error:
                    logger.warning(f"Spark创建表失败，尝试Hive方式: {str(spark_error)}")

            # 备用方案：使用Hive创建表
            connection = self.get_hive_connection()
            if not connection:
                logger.error("无法获取Hive连接")
                return

            cursor = connection.cursor()

            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {self.change_log_table} (
                change_id STRING,
                record_id INT,
                operation STRING,
                change_time STRING,
                old_data STRING,
                new_data STRING,
                changed_fields STRING,
                user_triggered BOOLEAN
            )
            STORED AS TEXTFILE
            """

            cursor.execute(create_table_sql)
            cursor.close()
            connection.close()
            logger.info(f"变化记录表 {self.change_log_table} 使用Hive创建成功")

        except Exception as e:
            logger.error(f"创建变化记录表失败: {str(e)}")
            # 如果创建失败，设置一个标志，避免后续查询错误
            self.change_log_available = False
    
    def get_data_list(self, limit=100, offset=0, filters=None):
        """
        获取数据列表

        Args:
            limit: 限制返回数量
            offset: 偏移量
            filters: 过滤条件
        """
        try:
            # 优先使用Spark查询，避免Hive MapReduce问题
            if self.use_spark and self.spark_processor and self.spark_processor.spark:
                try:
                    # 构建查询SQL
                    base_sql = "SELECT * FROM test.apple_quality2"

                    # 添加过滤条件
                    where_conditions = []
                    if filters:
                        if filters.get('quality'):
                            where_conditions.append(f"quality = '{filters['quality']}'")
                        if filters.get('min_size'):
                            where_conditions.append(f"size >= {filters['min_size']}")
                        if filters.get('max_size'):
                            where_conditions.append(f"size <= {filters['max_size']}")

                    if where_conditions:
                        base_sql += " WHERE " + " AND ".join(where_conditions)

                    # 添加排序，但不在SQL中限制，在Python中处理分页
                    base_sql += " ORDER BY a_id"

                    logger.info(f"使用Spark执行查询: {base_sql}")
                    spark_df = self.spark_processor.spark.sql(base_sql)
                    pandas_df = self.spark_processor.to_pandas(spark_df)

                    if pandas_df is not None:
                        # 在Python中进行排序，避免Hive ORDER BY问题
                        if 'a_id' in pandas_df.columns:
                            # 确保a_id列为数字类型后再排序
                            pandas_df['a_id'] = pandas_df['a_id'].astype(int)
                            pandas_df = pandas_df.sort_values('a_id')

                        # 应用偏移量和限制
                        if offset > 0:
                            pandas_df = pandas_df.iloc[offset:]
                        if limit and len(pandas_df) > limit:
                            pandas_df = pandas_df.iloc[:limit]

                        data = pandas_df.to_dict('records')

                        # 获取总数
                        count_sql = "SELECT COUNT(*) as total FROM test.apple_quality2"
                        if where_conditions:
                            count_sql += " WHERE " + " AND ".join(where_conditions)

                        count_df = self.spark_processor.spark.sql(count_sql)
                        total_count = count_df.collect()[0]['total']

                        return {
                            'success': True,
                            'data': data,
                            'total': total_count,
                            'page_info': {
                                'limit': limit,
                                'offset': offset,
                                'has_more': offset + limit < total_count
                            }
                        }
                    else:
                        logger.warning("Spark查询返回空结果，尝试Hive方式")
                except Exception as spark_error:
                    logger.warning(f"Spark查询失败，尝试Hive方式: {str(spark_error)}")

            # 备用方案：使用Hive查询，但避免ORDER BY
            connection = self.get_hive_connection()
            if not connection:
                return {
                    'success': False,
                    'error': '无法连接到Hive数据库',
                    'data': [],
                    'total': 0
                }

            cursor = connection.cursor()

            # 构建查询SQL，不使用ORDER BY避免MapReduce问题
            base_sql = "SELECT * FROM test.apple_quality2"

            # 添加过滤条件
            where_conditions = []
            if filters:
                if filters.get('quality'):
                    where_conditions.append(f"quality = '{filters['quality']}'")
                if filters.get('min_size'):
                    where_conditions.append(f"size >= {filters['min_size']}")
                if filters.get('max_size'):
                    where_conditions.append(f"size <= {filters['max_size']}")

            if where_conditions:
                base_sql += " WHERE " + " AND ".join(where_conditions)

            # 只使用LIMIT，不使用ORDER BY
            base_sql += f" LIMIT {limit}"

            logger.info(f"使用Hive执行查询: {base_sql}")
            # 执行查询
            cursor.execute(base_sql)
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()

            # 转换为字典列表
            data = []
            for row in rows:
                record = {}
                for i, value in enumerate(row):
                    record[columns[i]] = value
                data.append(record)

            # 在Python中进行排序，确保按数字排序
            if data and 'a_id' in data[0]:
                data.sort(key=lambda x: int(x['a_id']) if x['a_id'] is not None else 0)

            # 应用偏移量和限制
            if offset > 0 and len(data) > offset:
                data = data[offset:]
            if limit and len(data) > limit:
                data = data[:limit]

            # 获取总数
            count_sql = "SELECT COUNT(*) as total FROM test.apple_quality2"
            if where_conditions:
                count_sql += " WHERE " + " AND ".join(where_conditions)

            cursor.execute(count_sql)
            total_count = cursor.fetchone()[0]

            cursor.close()
            connection.close()

            return {
                'success': True,
                'data': data,
                'total': total_count,
                'page_info': {
                    'limit': limit,
                    'offset': offset,
                    'has_more': offset + limit < total_count
                }
            }

        except Exception as e:
            logger.error(f"获取数据列表失败: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'data': [],
                'total': 0
            }
    
    def get_record_by_id(self, record_id):
        """根据ID获取单条记录"""
        try:
            # 优先使用Spark查询
            if self.use_spark and self.spark_processor and self.spark_processor.spark:
                try:
                    sql = f"SELECT * FROM test.apple_quality2 WHERE a_id = {record_id}"
                    spark_df = self.spark_processor.spark.sql(sql)
                    pandas_df = self.spark_processor.to_pandas(spark_df)

                    if pandas_df is not None and len(pandas_df) > 0:
                        record = pandas_df.iloc[0].to_dict()
                        return {
                            'success': True,
                            'data': record
                        }
                    else:
                        return {
                            'success': False,
                            'error': '记录不存在'
                        }
                except Exception as spark_error:
                    logger.warning(f"Spark查询记录失败，尝试Hive方式: {str(spark_error)}")

            # 备用方案：使用Hive查询
            connection = self.get_hive_connection()
            if not connection:
                return {
                    'success': False,
                    'error': '无法连接到数据库'
                }

            cursor = connection.cursor()
            sql = f"SELECT * FROM test.apple_quality2 WHERE a_id = {record_id}"
            cursor.execute(sql)

            columns = [desc[0] for desc in cursor.description]
            row = cursor.fetchone()

            cursor.close()
            connection.close()

            if row:
                record = {}
                for i, value in enumerate(row):
                    record[columns[i]] = value
                return {
                    'success': True,
                    'data': record
                }
            else:
                return {
                    'success': False,
                    'error': '记录不存在'
                }

        except Exception as e:
            logger.error(f"获取记录失败: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def insert_record(self, record_data, user_triggered=True):
        """
        插入新记录

        Args:
            record_data: 记录数据
            user_triggered: 是否用户触发
        """
        try:
            # 验证数据
            required_fields = ['size', 'weight', 'sweetness', 'crunchiness', 'juiciness', 'ripeness', 'acidity', 'quality']
            for field in required_fields:
                if field not in record_data:
                    return {
                        'success': False,
                        'error': f'缺少必需字段: {field}'
                    }

            # 生成新的ID
            if 'a_id' not in record_data:
                if self.use_spark and self.spark_processor and self.spark_processor.spark:
                    try:
                        # 使用Spark获取最大ID
                        max_id_df = self.spark_processor.spark.sql("SELECT COALESCE(MAX(a_id), 0) as max_id FROM test.apple_quality2")
                        max_id_result = max_id_df.collect()[0]['max_id']
                        record_data['a_id'] = int(max_id_result) + 1
                    except Exception as spark_id_error:
                        logger.warning(f"Spark获取最大ID失败: {str(spark_id_error)}")
                        # 使用时间戳作为备用ID
                        import time
                        record_data['a_id'] = int(time.time() * 1000) % 100000
                else:
                    return {
                        'success': False,
                        'error': 'Spark不可用，无法生成新ID'
                    }

            if self.use_spark and self.spark_processor:
                # 使用Spark进行插入操作
                try:
                    # 直接使用Spark SQL插入，避免pandas兼容性问题
                    values = (
                        record_data['a_id'],
                        record_data['size'],
                        record_data['weight'],
                        record_data['sweetness'],
                        record_data['crunchiness'],
                        record_data['juiciness'],
                        record_data['ripeness'],
                        record_data['acidity'],
                        f"'{record_data['quality']}'"
                    )

                    insert_sql = f"""
                    INSERT INTO test.apple_quality2 VALUES {values}
                    """

                    self.spark_processor.spark.sql(insert_sql)
                    logger.info(f"使用Spark SQL插入记录成功: {record_data['a_id']}")

                except Exception as spark_error:
                    logger.error(f"Spark SQL插入失败: {str(spark_error)}")
                    return {
                        'success': False,
                        'error': f'插入操作失败: {str(spark_error)}'
                    }
            else:
                return {
                    'success': False,
                    'error': 'Hive INSERT操作失败，需要Spark支持'
                }

            # 记录变化
            change_id = str(uuid.uuid4())
            self.log_change(
                change_id=change_id,
                record_id=record_data['a_id'],
                operation='insert',
                old_data=None,
                new_data=record_data,
                changed_fields=list(record_data.keys()),
                user_triggered=user_triggered
            )

            # 发送到Kafka
            self.send_change_to_kafka({
                'operation': 'insert',
                'timestamp': datetime.now().isoformat(),
                'change_id': change_id,
                'record_id': record_data['a_id'],
                'data': record_data,
                'user_triggered': user_triggered,
                'affected_rows': 1
            })

            logger.info(f"成功插入记录: {record_data['a_id']}")
            return {
                'success': True,
                'data': record_data,
                'change_id': change_id
            }

        except Exception as e:
            logger.error(f"插入记录失败: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def update_record(self, record_id, update_data, user_triggered=True):
        """
        更新记录

        Args:
            record_id: 记录ID
            update_data: 更新数据
            user_triggered: 是否用户触发
        """
        try:
            # 获取原始记录
            old_record_result = self.get_record_by_id(record_id)
            if not old_record_result['success']:
                return old_record_result

            old_data = old_record_result['data']

            # 合并更新数据
            new_data = old_data.copy()
            changed_fields = []

            for key, value in update_data.items():
                if key in new_data and new_data[key] != value:
                    new_data[key] = value
                    changed_fields.append(key)

            if not changed_fields:
                return {
                    'success': False,
                    'error': '没有字段发生变化'
                }

            if self.use_spark and self.spark_processor:
                # 使用Spark SQL进行更新操作，避免pandas兼容性问题
                try:
                    # 使用Spark SQL的MERGE或者重建表的方式
                    # 由于Hive不支持UPDATE，我们使用重建表的方式

                    # 构建CASE语句，确保数据类型正确
                    case_statements = ["a_id"]  # 先添加a_id字段

                    # 所有字段及其类型
                    field_types = {
                        'size': 'double', 'weight': 'double', 'sweetness': 'double',
                        'crunchiness': 'double', 'juiciness': 'double', 'ripeness': 'double',
                        'acidity': 'double', 'quality': 'string'
                    }

                    for field in field_types:
                        if field in changed_fields:
                            # 更新的字段
                            if field_types[field] == 'string':
                                case_statements.append(f"CASE WHEN a_id = {record_id} THEN '{new_data[field]}' ELSE {field} END as {field}")
                            else:
                                # 确保数值类型正确转换
                                case_statements.append(f"CASE WHEN a_id = {record_id} THEN CAST({new_data[field]} AS DOUBLE) ELSE {field} END as {field}")
                        else:
                            # 未更改的字段
                            case_statements.append(f"{field}")

                    update_sql = f"""
                    CREATE OR REPLACE TEMPORARY VIEW temp_updated AS
                    SELECT {', '.join(case_statements)}
                    FROM test.apple_quality2
                    """

                    self.spark_processor.spark.sql(update_sql)

                    # 重新写入原表
                    self.spark_processor.spark.sql("""
                        INSERT OVERWRITE TABLE test.apple_quality2
                        SELECT * FROM temp_updated
                    """)

                    logger.info(f"使用Spark SQL更新记录成功: {record_id}")

                except Exception as spark_sql_error:
                    logger.error(f"Spark SQL更新失败: {str(spark_sql_error)}")
                    return {
                        'success': False,
                        'error': f'更新操作失败: {str(spark_sql_error)}'
                    }
            else:
                return {
                    'success': False,
                    'error': 'Spark不可用，无法执行更新操作'
                }

            # 记录变化
            change_id = str(uuid.uuid4())
            self.log_change(
                change_id=change_id,
                record_id=record_id,
                operation='update',
                old_data=old_data,
                new_data=new_data,
                changed_fields=changed_fields,
                user_triggered=user_triggered
            )

            # 发送到Kafka
            self.send_change_to_kafka({
                'operation': 'update',
                'timestamp': datetime.now().isoformat(),
                'change_id': change_id,
                'record_id': record_id,
                'old_data': old_data,
                'new_data': new_data,
                'changed_fields': changed_fields,
                'user_triggered': user_triggered,
                'affected_rows': 1
            })

            logger.info(f"成功更新记录: {record_id}")
            return {
                'success': True,
                'data': new_data,
                'changed_fields': changed_fields,
                'change_id': change_id
            }

        except Exception as e:
            logger.error(f"更新记录失败: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def delete_record(self, record_id, user_triggered=True):
        """
        删除记录

        Args:
            record_id: 记录ID
            user_triggered: 是否用户触发
        """
        try:
            # 获取要删除的记录
            old_record_result = self.get_record_by_id(record_id)
            if not old_record_result['success']:
                return old_record_result

            old_data = old_record_result['data']

            if self.use_spark and self.spark_processor:
                # 使用Spark进行删除操作
                try:
                    # 使用临时视图避免表锁定问题
                    self.spark_processor.spark.sql(f"""
                        CREATE OR REPLACE TEMPORARY VIEW temp_filtered AS
                        SELECT * FROM test.apple_quality2 WHERE a_id != {record_id}
                    """)

                    # 重新写入表
                    self.spark_processor.spark.sql("""
                        INSERT OVERWRITE TABLE test.apple_quality2
                        SELECT * FROM temp_filtered
                    """)

                    logger.info(f"使用Spark SQL删除记录成功: {record_id}")

                except Exception as spark_error:
                    logger.error(f"Spark删除操作失败: {str(spark_error)}")
                    return {
                        'success': False,
                        'error': f'删除操作失败: {str(spark_error)}'
                    }
            else:
                return {
                    'success': False,
                    'error': 'Spark不可用，无法执行删除操作'
                }

            # 记录变化
            change_id = str(uuid.uuid4())
            self.log_change(
                change_id=change_id,
                record_id=record_id,
                operation='delete',
                old_data=old_data,
                new_data=None,
                changed_fields=[],
                user_triggered=user_triggered
            )

            # 发送到Kafka
            self.send_change_to_kafka({
                'operation': 'delete',
                'timestamp': datetime.now().isoformat(),
                'change_id': change_id,
                'record_id': record_id,
                'data': old_data,
                'user_triggered': user_triggered,
                'affected_rows': 1
            })

            logger.info(f"成功删除记录: {record_id}")
            return {
                'success': True,
                'data': old_data,
                'change_id': change_id
            }

        except Exception as e:
            logger.error(f"删除记录失败: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def log_change(self, change_id, record_id, operation, old_data, new_data, changed_fields, user_triggered):
        """记录变化到Hive表"""
        try:
            # 检查变化记录表是否可用
            if not hasattr(self, 'change_log_available') or not self.change_log_available:
                logger.info("变化记录表不可用，跳过变化记录")
                return

            # 准备数据
            change_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            old_data_str = json.dumps(old_data) if old_data else None
            new_data_str = json.dumps(new_data) if new_data else None
            changed_fields_str = json.dumps(changed_fields)

            # 优先使用Spark SQL插入
            if self.use_spark and self.spark_processor and self.spark_processor.spark:
                try:
                    # 使用Spark SQL直接插入，避免pandas兼容性问题
                    # 正确转义JSON字符串
                    old_data_escaped = old_data_str.replace("'", "\\'") if old_data_str else None
                    new_data_escaped = new_data_str.replace("'", "\\'") if new_data_str else None
                    changed_fields_escaped = changed_fields_str.replace("'", "\\'")

                    insert_sql = f"""
                    INSERT INTO {self.change_log_table} VALUES (
                        '{change_id}',
                        {record_id},
                        '{operation}',
                        '{change_time}',
                        {f"'{old_data_escaped}'" if old_data_escaped else 'NULL'},
                        {f"'{new_data_escaped}'" if new_data_escaped else 'NULL'},
                        '{changed_fields_escaped}',
                        {str(user_triggered).lower()}
                    )
                    """

                    self.spark_processor.spark.sql(insert_sql)
                    logger.info(f"变化记录已保存(Spark SQL): {change_id}")
                    return

                except Exception as spark_error:
                    logger.warning(f"Spark SQL记录变化失败，尝试Hive方式: {str(spark_error)}")

            # 备用方案：使用Hive插入
            connection = self.get_hive_connection()
            if not connection:
                logger.error("无法连接到Hive数据库，跳过变化记录")
                return

            cursor = connection.cursor()

            # 转义单引号以避免SQL注入
            old_data_str = old_data_str.replace("'", "''") if old_data_str else 'NULL'
            new_data_str = new_data_str.replace("'", "''") if new_data_str else 'NULL'
            changed_fields_str = changed_fields_str.replace("'", "''")
            user_triggered_str = 'true' if user_triggered else 'false'

            insert_sql = f"""
            INSERT INTO {self.change_log_table}
            (change_id, record_id, operation, change_time, old_data, new_data, changed_fields, user_triggered)
            VALUES
            ('{change_id}', {record_id}, '{operation}', '{change_time}',
             '{old_data_str}', '{new_data_str}', '{changed_fields_str}', {user_triggered_str})
            """

            try:
                cursor.execute(insert_sql)
                logger.info(f"变化记录已保存(Hive): {change_id}")
            except Exception as sql_error:
                logger.error(f"SQL执行失败: {str(sql_error)}")
                # 如果SQL执行失败，尝试简化的插入
                try:
                    simple_sql = f"""
                    INSERT INTO {self.change_log_table}
                    (change_id, record_id, operation, change_time, user_triggered)
                    VALUES
                    ('{change_id}', {record_id}, '{operation}', '{change_time}', {user_triggered_str})
                    """
                    cursor.execute(simple_sql)
                    logger.info(f"变化记录已保存(简化): {change_id}")
                except Exception as simple_error:
                    logger.error(f"简化插入也失败: {str(simple_error)}")
                    # 标记表不可用
                    self.change_log_available = False

            cursor.close()
            connection.close()

        except Exception as e:
            logger.error(f"记录变化失败: {str(e)}")
            # 如果是表不存在的错误，标记为不可用
            if "Table not found" in str(e) or "doesn't exist" in str(e):
                self.change_log_available = False
    
    def get_change_history(self, limit=50):
        """获取变化历史"""
        try:
            # 检查变化记录表是否可用
            if not hasattr(self, 'change_log_available') or not self.change_log_available:
                return {
                    'success': True,
                    'data': [],
                    'message': '变化记录表不可用'
                }

            # 优先使用Spark查询
            if self.use_spark and self.spark_processor and self.spark_processor.spark:
                try:
                    # 使用简化的查询，避免ORDER BY导致的MapReduce问题
                    sql = f"""
                    SELECT change_id, record_id, operation, change_time,
                           changed_fields, user_triggered
                    FROM {self.change_log_table}
                    LIMIT {limit}
                    """

                    spark_df = self.spark_processor.spark.sql(sql)

                    # 直接使用collect方法，避免pandas转换问题
                    rows = spark_df.collect()

                    data = []
                    for row in rows:
                        record = {
                            'change_id': row.change_id,
                            'record_id': row.record_id,
                            'operation': row.operation,
                            'change_time': row.change_time,
                            'user_triggered': row.user_triggered
                        }

                        # 解析changed_fields JSON
                        try:
                            if row.changed_fields:
                                record['changed_fields'] = json.loads(row.changed_fields)
                            else:
                                record['changed_fields'] = []
                        except (json.JSONDecodeError, AttributeError):
                            record['changed_fields'] = []

                        data.append(record)

                    # 按时间排序（在Python中进行）
                    data.sort(key=lambda x: x['change_time'], reverse=True)

                    return {
                        'success': True,
                        'data': data[:limit]
                    }
                except Exception as spark_error:
                    logger.warning(f"Spark查询变化历史失败: {str(spark_error)}")
                    # 不再尝试Hive方式，直接返回空结果

            # 如果Spark查询失败，返回空结果
            return {
                'success': True,
                'data': [],
                'message': '变化记录查询暂时不可用'
            }

        except Exception as e:
            logger.error(f"获取变化历史失败: {str(e)}")
            # 如果是表不存在的错误，标记为不可用
            if "Table not found" in str(e) or "doesn't exist" in str(e):
                self.change_log_available = False
            return {
                'success': True,  # 返回成功但数据为空，避免前端报错
                'data': [],
                'message': '暂无变化历史记录'
            }



    def get_changes_since(self, since_timestamp, limit=50):
        """获取指定时间戳之后的变化"""
        try:
            # 检查变化记录表是否可用
            if not hasattr(self, 'change_log_available') or not self.change_log_available:
                # 如果变化记录表不可用，尝试启用它
                self.change_log_available = True
                self.create_change_log_table()

            # 优先使用Spark查询
            if self.use_spark and self.spark_processor and self.spark_processor.spark:
                try:
                    # 首先检查表是否存在
                    check_table_sql = f"SHOW TABLES IN test LIKE '{self.change_log_table.split('.')[-1]}'"
                    table_check_df = self.spark_processor.spark.sql(check_table_sql)
                    table_exists = len(table_check_df.collect()) > 0

                    if not table_exists:
                        logger.info(f"变化记录表 {self.change_log_table} 不存在，尝试创建")
                        self.create_change_log_table()
                        # 重新检查
                        table_check_df = self.spark_processor.spark.sql(check_table_sql)
                        table_exists = len(table_check_df.collect()) > 0

                        if not table_exists:
                            logger.warning(f"变化记录表 {self.change_log_table} 创建失败")
                            return {
                                'success': True,
                                'data': [],
                                'message': '变化记录表不存在'
                            }

                    # 获取所有变化记录，在Python中进行时间过滤
                    sql = f"""
                    SELECT change_id, record_id, operation, change_time,
                           changed_fields, user_triggered
                    FROM {self.change_log_table}
                    ORDER BY change_time DESC
                    LIMIT {limit * 2}
                    """

                    spark_df = self.spark_processor.spark.sql(sql)
                    rows = spark_df.collect()

                    # 解析时间戳
                    from datetime import datetime
                    try:
                        # 处理ISO格式时间戳
                        if 'T' in since_timestamp:
                            since_dt = datetime.fromisoformat(since_timestamp.replace('Z', ''))
                        else:
                            since_dt = datetime.strptime(since_timestamp, '%Y-%m-%d %H:%M:%S')
                        logger.info(f"解析时间戳成功: {since_dt}")
                    except Exception as time_error:
                        logger.warning(f"时间戳解析失败: {str(time_error)}, 使用当前时间前1分钟")
                        since_dt = datetime.now().replace(second=0, microsecond=0)
                        since_dt = since_dt.replace(minute=since_dt.minute - 1)

                    filtered_data = []
                    for row in rows:
                        try:
                            # 解析记录时间 - 处理不同的时间格式
                            if isinstance(row.change_time, str):
                                record_time = datetime.strptime(row.change_time, '%Y-%m-%d %H:%M:%S')
                            else:
                                # 如果已经是datetime对象，直接使用
                                record_time = row.change_time

                            # 只包含指定时间之后的记录
                            if record_time > since_dt:
                                record = {
                                    'change_id': row.change_id,
                                    'record_id': row.record_id,
                                    'operation': row.operation,
                                    'change_time': row.change_time,
                                    'user_triggered': row.user_triggered
                                }

                                # 解析changed_fields JSON
                                try:
                                    if row.changed_fields:
                                        record['changed_fields'] = json.loads(row.changed_fields)
                                    else:
                                        record['changed_fields'] = []
                                except (json.JSONDecodeError, AttributeError):
                                    record['changed_fields'] = []

                                filtered_data.append(record)
                        except Exception as row_error:
                            logger.warning(f"处理变化记录失败: {str(row_error)}")
                            continue

                    # 按时间排序（最新的在前）
                    filtered_data.sort(key=lambda x: x['change_time'], reverse=True)

                    logger.info(f"找到 {len(filtered_data)} 个时间戳 {since_timestamp} 之后的变化")
                    return {
                        'success': True,
                        'data': filtered_data[:limit]  # 确保不超过限制
                    }

                except Exception as spark_error:
                    logger.error(f"Spark查询时间戳变化失败: {str(spark_error)}")

            # 如果Spark查询失败，返回空结果
            return {
                'success': True,
                'data': [],
                'message': '变化记录查询暂时不可用'
            }

        except Exception as e:
            logger.error(f"获取时间戳变化失败: {str(e)}")
            return {
                'success': True,  # 返回成功但数据为空，避免前端报错
                'data': [],
                'message': '暂无变化记录'
            }

    def close(self):
        """关闭连接"""
        logger.info("数据变化管理器已关闭")
