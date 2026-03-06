#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据管理API路由
提供数据的增删改查接口
"""

from flask import Blueprint, request, jsonify, render_template
from data_change_manager import DataChangeManager
from datetime import datetime
import logging

# 设置日志
logger = logging.getLogger(__name__)

# 创建蓝图
data_management_bp = Blueprint('data_management', __name__)

# 全局数据管理器实例
data_manager = None

# 全局Kafka处理器实例
kafka_processor = None

def get_data_manager():
    """获取数据管理器实例"""
    global data_manager
    if data_manager is None:
        data_manager = DataChangeManager()
    return data_manager

def get_kafka_processor():
    """获取Kafka处理器实例"""
    global kafka_processor
    if kafka_processor is None:
        try:
            from kafka_streaming_processor import KafkaStreamingProcessor
            kafka_processor = KafkaStreamingProcessor()
        except Exception as e:
            logger.error(f"创建Kafka处理器失败: {str(e)}")
    return kafka_processor

@data_management_bp.route('/data_management')
def data_management_page():
    """数据管理页面"""
    try:
        manager = get_data_manager()
        
        # 获取初始数据
        data_result = manager.get_data_list(limit=20)
        history_result = manager.get_change_history(limit=10)
        
        return render_template('data_management.html',
                             initial_data=data_result.get('data', []),
                             total_records=data_result.get('total', 0),
                             change_history=history_result.get('data', []))
    except Exception as e:
        logger.error(f"数据管理页面加载失败: {str(e)}")
        return render_template('data_management.html',
                             initial_data=[],
                             total_records=0,
                             change_history=[],
                             error=str(e))

@data_management_bp.route('/api/data/list', methods=['GET'])
def get_data_list():
    """获取数据列表API"""
    try:
        # 获取查询参数
        limit = int(request.args.get('limit', 20))
        offset = int(request.args.get('offset', 0))
        
        # 获取过滤条件
        filters = {}
        if request.args.get('quality'):
            filters['quality'] = request.args.get('quality')
        if request.args.get('min_size'):
            filters['min_size'] = float(request.args.get('min_size'))
        if request.args.get('max_size'):
            filters['max_size'] = float(request.args.get('max_size'))
        
        manager = get_data_manager()
        result = manager.get_data_list(limit=limit, offset=offset, filters=filters)
        
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"获取数据列表失败: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@data_management_bp.route('/api/data/<int:record_id>', methods=['GET'])
def get_record(record_id):
    """获取单条记录API"""
    try:
        manager = get_data_manager()
        result = manager.get_record_by_id(record_id)
        
        if result['success']:
            return jsonify(result)
        else:
            return jsonify(result), 404
            
    except Exception as e:
        logger.error(f"获取记录失败: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@data_management_bp.route('/api/data', methods=['POST'])
def create_record():
    """创建新记录API"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({
                'success': False,
                'error': '请提供记录数据'
            }), 400
        
        manager = get_data_manager()
        result = manager.insert_record(data, user_triggered=True)
        
        if result['success']:
            return jsonify(result), 201
        else:
            return jsonify(result), 400
            
    except Exception as e:
        logger.error(f"创建记录失败: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@data_management_bp.route('/api/data/<int:record_id>', methods=['PUT'])
def update_record(record_id):
    """更新记录API"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({
                'success': False,
                'error': '请提供更新数据'
            }), 400
        
        manager = get_data_manager()
        result = manager.update_record(record_id, data, user_triggered=True)
        
        if result['success']:
            return jsonify(result)
        else:
            return jsonify(result), 400
            
    except Exception as e:
        logger.error(f"更新记录失败: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@data_management_bp.route('/api/data/<int:record_id>', methods=['DELETE'])
def delete_record(record_id):
    """删除记录API"""
    try:
        manager = get_data_manager()
        result = manager.delete_record(record_id, user_triggered=True)
        
        if result['success']:
            return jsonify(result)
        else:
            return jsonify(result), 400
            
    except Exception as e:
        logger.error(f"删除记录失败: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@data_management_bp.route('/api/data/changes', methods=['GET'])
def get_change_history():
    """获取变化历史API"""
    try:
        limit = int(request.args.get('limit', 50))

        manager = get_data_manager()
        result = manager.get_change_history(limit=limit)

        return jsonify(result)

    except Exception as e:
        logger.error(f"获取变化历史失败: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@data_management_bp.route('/api/data/changes/recent', methods=['GET'])
def get_recent_changes():
    """获取最近的数据变化API - 用于实时更新"""
    try:
        # 获取时间戳参数，用于获取指定时间之后的变化
        since_timestamp = request.args.get('since')
        limit = int(request.args.get('limit', 10))

        manager = get_data_manager()

        # 如果提供了时间戳，获取该时间之后的变化
        if since_timestamp:
            result = manager.get_changes_since(since_timestamp, limit=limit)
        else:
            # 否则获取最近的变化
            result = manager.get_change_history(limit=limit)

        return jsonify(result)

    except Exception as e:
        logger.error(f"获取最近变化失败: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@data_management_bp.route('/api/data/realtime/status', methods=['GET'])
def get_realtime_status():
    """获取实时更新状态API"""
    try:
        manager = get_data_manager()
        processor = get_kafka_processor()

        # 获取最近的变化统计
        recent_changes = manager.get_change_history(limit=5)

        # 获取Kafka处理器状态
        kafka_status = processor.get_statistics() if processor else {
            'status': 'unavailable',
            'statistics': {'messages_processed': 0, 'errors': 0}
        }

        return jsonify({
            'success': True,
            'data': {
                'recent_changes': recent_changes.get('data', []),
                'kafka_status': kafka_status,
                'timestamp': datetime.now().isoformat()
            }
        })

    except Exception as e:
        logger.error(f"获取实时状态失败: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@data_management_bp.route('/api/data/batch', methods=['POST'])
def batch_operations():
    """批量操作API"""
    try:
        data = request.get_json()
        operations = data.get('operations', [])
        
        if not operations:
            return jsonify({
                'success': False,
                'error': '请提供操作列表'
            }), 400
        
        manager = get_data_manager()
        results = []
        
        for operation in operations:
            op_type = operation.get('type')
            op_data = operation.get('data')
            record_id = operation.get('record_id')
            
            try:
                if op_type == 'insert':
                    result = manager.insert_record(op_data, user_triggered=True)
                elif op_type == 'update':
                    result = manager.update_record(record_id, op_data, user_triggered=True)
                elif op_type == 'delete':
                    result = manager.delete_record(record_id, user_triggered=True)
                else:
                    result = {'success': False, 'error': f'未知操作类型: {op_type}'}
                
                results.append({
                    'operation': operation,
                    'result': result
                })
                
            except Exception as e:
                results.append({
                    'operation': operation,
                    'result': {'success': False, 'error': str(e)}
                })
        
        # 统计成功和失败的操作
        success_count = sum(1 for r in results if r['result']['success'])
        total_count = len(results)
        
        return jsonify({
            'success': True,
            'results': results,
            'summary': {
                'total': total_count,
                'success': success_count,
                'failed': total_count - success_count
            }
        })
        
    except Exception as e:
        logger.error(f"批量操作失败: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@data_management_bp.route('/api/data/simulate', methods=['POST'])
def trigger_simulation():
    """触发数据模拟API"""
    try:
        data = request.get_json()
        count = data.get('count', 1)  # 模拟次数
        operation_type = data.get('operation', 'random')  # 操作类型
        
        manager = get_data_manager()
        results = []
        
        for i in range(count):
            if operation_type == 'random':
                # 随机选择操作类型
                import random
                op_type = random.choice(['insert', 'update', 'delete'])
            else:
                op_type = operation_type
            
            try:
                if op_type == 'insert':
                    # 生成随机数据
                    new_record = manager.simulator.generate_new_record()
                    if new_record:
                        result = manager.insert_record(new_record, user_triggered=False)
                    else:
                        result = {'success': False, 'error': '生成新记录失败'}
                        
                elif op_type == 'update':
                    # 随机选择一条记录更新
                    data_list = manager.get_data_list(limit=10)
                    if data_list['success'] and data_list['data']:
                        import random
                        record = random.choice(data_list['data'])
                        update_data = {
                            'sweetness': round(record['sweetness'] + random.uniform(-1, 1), 2),
                            'size': round(record['size'] + random.uniform(-0.5, 0.5), 2)
                        }
                        result = manager.update_record(record['a_id'], update_data, user_triggered=False)
                    else:
                        result = {'success': False, 'error': '没有可更新的记录'}
                        
                elif op_type == 'delete':
                    # 随机选择一条记录删除
                    data_list = manager.get_data_list(limit=10)
                    if data_list['success'] and data_list['data'] and len(data_list['data']) > 5:
                        import random
                        record = random.choice(data_list['data'])
                        result = manager.delete_record(record['a_id'], user_triggered=False)
                    else:
                        result = {'success': False, 'error': '没有可删除的记录或记录数量过少'}
                
                results.append({
                    'operation': op_type,
                    'result': result
                })
                
            except Exception as e:
                results.append({
                    'operation': op_type,
                    'result': {'success': False, 'error': str(e)}
                })
        
        success_count = sum(1 for r in results if r['result']['success'])
        
        return jsonify({
            'success': True,
            'results': results,
            'summary': {
                'total': count,
                'success': success_count,
                'failed': count - success_count
            }
        })
        
    except Exception as e:
        logger.error(f"触发模拟失败: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

# 添加Kafka实时处理相关路由

@data_management_bp.route('/kafka_realtime_monitor')
def kafka_realtime_monitor_page():
    """Kafka实时监控页面"""
    try:
        processor = get_kafka_processor()
        stats = processor.get_statistics() if processor else {
            'status': 'unavailable',
            'statistics': {
                'messages_processed': 0,
                'insert_processed': 0,
                'update_processed': 0,
                'delete_processed': 0,
                'errors': 0
            },
            'topic': 'apple_quality_changes',
            'timestamp': None
        }
        
        # 获取最近的变化历史
        manager = get_data_manager()
        history_result = manager.get_change_history(limit=10)
        
        return render_template('kafka_monitor_simple.html',
                             processor_stats=stats,
                             change_history=history_result.get('data', []))
    except Exception as e:
        logger.error(f"Kafka监控页面加载失败: {str(e)}")
        return render_template('kafka_monitor_simple.html',
                             processor_stats={},
                             change_history=[],
                             error=str(e))

@data_management_bp.route('/api/kafka/status', methods=['GET'])
def get_kafka_status():
    """获取Kafka处理器状态"""
    try:
        processor = get_kafka_processor()
        if not processor:
            return jsonify({
                'success': False,
                'error': 'Kafka处理器未初始化'
            }), 404
        
        stats = processor.get_statistics()
        return jsonify({
            'success': True,
            'data': stats
        })
        
    except Exception as e:
        logger.error(f"获取Kafka状态失败: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@data_management_bp.route('/api/kafka/start', methods=['POST'])
def start_kafka_processor():
    """启动Kafka处理器"""
    try:
        processor = get_kafka_processor()
        if not processor:
            return jsonify({
                'success': False,
                'error': 'Kafka处理器未初始化'
            }), 404
        
        if processor.running:
            return jsonify({
                'success': True,
                'message': 'Kafka处理器已在运行',
                'data': processor.get_statistics()
            })
        
        success = processor.start_processing()
        if success:
            return jsonify({
                'success': True,
                'message': 'Kafka处理器启动成功',
                'data': processor.get_statistics()
            })
        else:
            return jsonify({
                'success': False,
                'error': 'Kafka处理器启动失败'
            }), 500
        
    except Exception as e:
        logger.error(f"启动Kafka处理器失败: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@data_management_bp.route('/api/kafka/stop', methods=['POST'])
def stop_kafka_processor():
    """停止Kafka处理器"""
    try:
        processor = get_kafka_processor()
        if not processor:
            return jsonify({
                'success': False,
                'error': 'Kafka处理器未初始化'
            }), 404
        
        if not processor.running:
            return jsonify({
                'success': True,
                'message': 'Kafka处理器已停止',
                'data': processor.get_statistics()
            })
        
        success = processor.stop_processing()
        if success:
            return jsonify({
                'success': True,
                'message': 'Kafka处理器停止成功',
                'data': processor.get_statistics()
            })
        else:
            return jsonify({
                'success': False,
                'error': 'Kafka处理器停止失败'
            }), 500
        
    except Exception as e:
        logger.error(f"停止Kafka处理器失败: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500
