from flask import Blueprint, request, jsonify, current_app
import os
import json
from datetime import datetime
import re

reports_bp = Blueprint('reports', __name__)

# 确保reports目录存在
def ensure_reports_dir():
    reports_dir = os.path.join(current_app.root_path, 'reports')
    if not os.path.exists(reports_dir):
        os.makedirs(reports_dir)
    return reports_dir

# 清理文件名中的特殊字符
def clean_filename(text):
    # 移除或替换文件名中不允许的字符
    text = re.sub(r'[<>:"/\\|?*]', '_', text)
    text = re.sub(r'\s+', '_', text)  # 将空格替换为下划线
    return text[:50]  # 限制长度

@reports_bp.route('/api/save_report', methods=['POST'])
def save_report():
    try:
        data = request.get_json()
        
        if not data or 'content' not in data:
            return jsonify({'success': False, 'message': '缺少报告内容'}), 400
        
        content = data['content']
        model_name = data.get('model_name', '未知模型')
        timestamp = datetime.now()
        
        # 生成文件名
        time_str = timestamp.strftime('%Y-%m-%d_%H-%M')
        clean_model_name = clean_filename(model_name)
        filename = f"{time_str}_{clean_model_name}.html"
        
        # 确保reports目录存在
        reports_dir = ensure_reports_dir()
        file_path = os.path.join(reports_dir, filename)
        
        # 保存报告内容到文件
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        
        # 返回报告信息
        report_info = {
            'id': filename.replace('.html', ''),
            'filename': filename,
            'model_name': model_name,
            'timestamp': timestamp.isoformat(),
            'file_path': file_path
        }
        
        return jsonify({
            'success': True, 
            'message': '报告保存成功',
            'report': report_info
        })
        
    except Exception as e:
        current_app.logger.error(f"保存报告失败: {str(e)}")
        return jsonify({'success': False, 'message': f'保存失败: {str(e)}'}), 500

@reports_bp.route('/api/list_reports', methods=['GET'])
def list_reports():
    try:
        reports_dir = ensure_reports_dir()
        reports = []
        
        # 扫描reports目录中的HTML文件
        for filename in os.listdir(reports_dir):
            if filename.endswith('.html'):
                file_path = os.path.join(reports_dir, filename)
                
                # 从文件名解析信息
                try:
                    # 文件名格式: 2025-01-05_11-22_DeepSeek-V3.html
                    name_without_ext = filename.replace('.html', '')
                    parts = name_without_ext.split('_')
                    
                    if len(parts) >= 3:
                        date_part = parts[0]  # 2025-01-05
                        time_part = parts[1]  # 11-22
                        model_part = '_'.join(parts[2:])  # DeepSeek-V3 (可能包含下划线)
                        
                        # 构造时间戳
                        datetime_str = f"{date_part} {time_part.replace('-', ':')}"
                        timestamp = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M')
                        
                        # 获取文件修改时间作为备选
                        file_mtime = datetime.fromtimestamp(os.path.getmtime(file_path))
                        
                        reports.append({
                            'id': name_without_ext,
                            'filename': filename,
                            'model_name': model_part.replace('_', ' '),
                            'timestamp': timestamp.isoformat(),
                            'file_mtime': file_mtime.isoformat()
                        })
                except Exception as parse_error:
                    # 如果解析失败，使用文件修改时间
                    file_mtime = datetime.fromtimestamp(os.path.getmtime(file_path))
                    reports.append({
                        'id': filename.replace('.html', ''),
                        'filename': filename,
                        'model_name': '未知模型',
                        'timestamp': file_mtime.isoformat(),
                        'file_mtime': file_mtime.isoformat()
                    })
        
        # 按时间戳降序排序（最新的在前）
        reports.sort(key=lambda x: x['timestamp'], reverse=True)
        
        return jsonify({
            'success': True,
            'reports': reports
        })
        
    except Exception as e:
        current_app.logger.error(f"获取报告列表失败: {str(e)}")
        return jsonify({'success': False, 'message': f'获取列表失败: {str(e)}'}), 500

@reports_bp.route('/api/get_report/<report_id>', methods=['GET'])
def get_report(report_id):
    try:
        reports_dir = ensure_reports_dir()
        filename = f"{report_id}.html"
        file_path = os.path.join(reports_dir, filename)
        
        if not os.path.exists(file_path):
            return jsonify({'success': False, 'message': '报告文件不存在'}), 404
        
        # 读取报告内容
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        return jsonify({
            'success': True,
            'content': content
        })
        
    except Exception as e:
        current_app.logger.error(f"获取报告内容失败: {str(e)}")
        return jsonify({'success': False, 'message': f'获取报告失败: {str(e)}'}), 500

@reports_bp.route('/api/delete_report/<report_id>', methods=['DELETE'])
def delete_report(report_id):
    try:
        reports_dir = ensure_reports_dir()
        filename = f"{report_id}.html"
        file_path = os.path.join(reports_dir, filename)
        
        if not os.path.exists(file_path):
            return jsonify({'success': False, 'message': '报告文件不存在'}), 404
        
        # 删除文件
        os.remove(file_path)
        
        return jsonify({
            'success': True,
            'message': '报告删除成功'
        })
        
    except Exception as e:
        current_app.logger.error(f"删除报告失败: {str(e)}")
        return jsonify({'success': False, 'message': f'删除报告失败: {str(e)}'}), 500
