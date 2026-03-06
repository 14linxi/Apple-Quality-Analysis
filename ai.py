import json
import os
import uuid
from datetime import datetime
import requests
import logging

# 设置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# AI配置文件路径
CONFIG_FILE = 'ai_config.json'

class AIHandler:
    """处理AI模型配置和API调用的类"""
    
    def __init__(self, config_file=CONFIG_FILE):
        """初始化处理器
        
        Args:
            config_file: 配置文件路径
        """
        self.config_file = config_file
        self.config = self._load_config()
    
    def _load_config(self):
        """从文件加载配置，如果不存在则创建默认配置"""
        if os.path.exists(self.config_file):
            try:
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"加载AI配置文件出错: {str(e)}, 将创建默认配置")
        
        # 默认配置 - 不含示例模型
        default_config = {
            "models": []
        }
        
        # 保存默认配置
        self._save_config(default_config)
        return default_config
    
    def _save_config(self, config=None):
        """保存配置到文件"""
        if config is None:
            config = self.config
        
        with open(self.config_file, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=4, ensure_ascii=False)
    
    def get_all_models(self):
        """获取所有模型配置"""
        return self.config.get("models", [])
    
    def get_enabled_models(self):
        """获取所有启用的模型配置"""
        return [model for model in self.get_all_models() if model.get("enabled", False)]
    
    def get_model_by_id(self, model_id):
        """通过ID获取模型配置"""
        try:
            if not model_id:
                logger.warning("尝试获取模型，但未提供有效的模型ID")
                return None
            
            # 获取模型配置
            for model in self.get_all_models():
                if model.get("id") == model_id:
                    logger.info(f"成功获取ID为 {model_id} 的模型")
                    return model
            
            # 如果没有找到模型
            logger.warning(f"找不到ID为 {model_id} 的模型")
            return None
        except Exception as e:
            logger.error(f"获取模型时出错 (ID: {model_id}): {str(e)}")
            # 重新抛出异常，让上层处理
            raise
    
    def add_model(self, model_data):
        """添加新模型配置"""
        if "id" not in model_data:
            model_data["id"] = str(uuid.uuid4())
        
        model_data["enabled"] = model_data.get("enabled", False)
        model_data["last_tested"] = None
        
        self.config["models"].append(model_data)
        self._save_config()
        return model_data
    
    def update_model(self, model_data):
        """更新模型配置"""
        try:
            model_id = model_data.get("id")
            
            # 验证必需字段
            required_fields = ["name", "provider", "model", "base_url"]
            for field in required_fields:
                if not model_data.get(field):
                    raise ValueError(f"缺少必需字段: {field}")
            
            # 新增模型
            if not model_id:
                # 生成UUID
                model_data["id"] = str(uuid.uuid4())
                model_data["enabled"] = model_data.get("enabled", False)
                model_data["last_tested"] = None
                
                # 确保API密钥存在
                if not model_data.get("api_key"):
                    raise ValueError("API密钥不能为空")
        
                # 添加到配置
                self.config["models"].append(model_data)
                self._save_config()
                logger.info(f"添加了新模型: {model_data['name']} ({model_data['provider']})")
                return True
        
            # 更新现有模型
            for i, model in enumerate(self.config["models"]):
                if model.get("id") == model_id:
                    # 保留原有API密钥（如果没有提供新的）
                    if "api_key" not in model_data or not model_data["api_key"]:
                        model_data["api_key"] = model.get("api_key", "")
                    
                    # 更新模型
                    self.config["models"][i] = model_data
                    self._save_config()
                    logger.info(f"更新了模型: {model_data['name']} ({model_data['provider']})")
                    return True
            
            # 如果没有找到要更新的模型
            raise ValueError(f"找不到ID为 {model_id} 的模型")
        except Exception as e:
            logger.error(f"更新模型时出错: {str(e)}")
            raise
    
    def delete_model(self, model_id):
        """删除模型配置"""
        for i, model in enumerate(self.config["models"]):
            if model.get("id") == model_id:
                del self.config["models"][i]
                self._save_config()
                return True
        
        return False
    
    def test_model_connection(self, model_id):
        """测试模型连接是否正常"""
        logger.info(f"开始测试模型连接，模型ID: {model_id}")

        model = self.get_model_by_id(model_id)
        if not model:
            logger.error(f"找不到模型ID: {model_id}")
            return {"success": False, "message": "找不到指定的模型"}

        provider = model.get("provider", "").lower()
        api_key = model.get("api_key", "")
        base_url = model.get("base_url", "")
        model_name = model.get("model", "")

        logger.info(f"模型配置 - 提供商: {provider}, 基础URL: {base_url}, 模型名: {model_name}")

        if not api_key:
            logger.error("API密钥为空")
            return {"success": False, "message": "API密钥不能为空"}

        if not base_url:
            logger.error("基础URL为空")
            return {"success": False, "message": "API基础URL不能为空"}

        if not model_name:
            logger.error("模型名称为空")
            return {"success": False, "message": "模型名称不能为空"}
        
        try:
            if provider == "openai":
                # 确保URL格式正确（去除尾部斜杠）
                clean_base_url = base_url
                if clean_base_url.endswith('/'):
                    clean_base_url = clean_base_url[:-1]
                
                # 直接使用API请求而不是LangChain
                headers = {
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json"
                }
                
                try:
                    # 首先测试获取模型列表
                    models_url = f"{clean_base_url}/v1/models"
                    logger.info(f"测试连接: 请求模型列表 {models_url}")

                    models_response = requests.get(models_url, headers=headers, timeout=10)
                    logger.info(f"测试连接: 模型列表响应状态码 {models_response.status_code}")

                    # 检查是否成功获取到JSON响应
                    content_type = models_response.headers.get('content-type', '').lower()
                    if content_type.startswith('application/json'):
                        models_data = models_response.json()  # 尝试解析JSON，如果失败会抛出异常
                        logger.info(f"测试连接: 成功解析模型列表JSON，包含 {len(models_data.get('data', []))} 个模型")
                    else:
                        response_preview = models_response.text[:200] + ('...' if len(models_response.text) > 200 else '')
                        logger.error(f"测试连接: API返回非JSON格式 {content_type}, 内容预览: {response_preview}")
                        raise ValueError(f"API返回了非JSON格式的响应: {content_type}")

                    models_response.raise_for_status()  # 如果请求失败会抛出异常
                    
                    # 再测试简单的聊天请求
                    chat_url = f"{clean_base_url}/v1/chat/completions"

                    # 准备测试消息 - 根据API类型调整
                    test_message = "Hello! Please respond with a simple test message."
                    if "claude" in model_name.lower() or "anthropic" in clean_base_url.lower():
                        test_message = "请回复一个简单的测试消息。"
                    elif "gpt" in model_name.lower() or "openai" in clean_base_url.lower():
                        test_message = "Please respond with a simple test message."

                    chat_data = {
                        "model": model_name,
                        "messages": [
                            {"role": "user", "content": test_message}
                        ],
                        "temperature": 0,
                        "max_tokens": 50,  # 增加token数量以获得更完整的响应
                        "stream": False  # 明确禁用流式输出
                    }

                    logger.info(f"测试连接: 发送聊天请求到 {chat_url}, 使用模型 {model_name}")
                    chat_response = requests.post(chat_url, json=chat_data, headers=headers, timeout=15)
                    logger.info(f"测试连接: 聊天响应状态码 {chat_response.status_code}")

                    chat_response.raise_for_status()  # 先检查HTTP状态码

                    # 检查响应类型并相应处理
                    chat_content_type = chat_response.headers.get('content-type', '').lower()
                    response_data = None
                    actual_response_content = ""

                    if chat_content_type.startswith('application/json'):
                        # 标准JSON响应
                        response_data = chat_response.json()
                        logger.info(f"测试连接: 成功解析标准JSON响应")

                        # 提取响应内容
                        if response_data and "choices" in response_data and len(response_data["choices"]) > 0:
                            message = response_data["choices"][0].get("message", {})
                            if message and "content" in message:
                                actual_response_content = message["content"].strip()

                    elif chat_content_type.startswith('text/event-stream') or 'stream' in chat_content_type:
                        # 流式响应 - 解析SSE格式
                        logger.info(f"测试连接: 检测到流式响应，开始解析SSE数据")
                        response_text = chat_response.text

                        # 解析SSE格式的流式数据
                        lines = response_text.split('\n')
                        collected_content = []

                        for line in lines:
                            line = line.strip()  # 去除行首尾空白字符
                            if line.startswith('data: '):
                                data_str = line[6:]  # 移除 'data: ' 前缀
                                if data_str.strip() == '[DONE]':
                                    break
                                try:
                                    data = json.loads(data_str)
                                    if 'choices' in data and len(data['choices']) > 0:
                                        delta = data['choices'][0].get('delta', {})
                                        if 'content' in delta and delta['content']:
                                            content_piece = delta['content']
                                            # 确保内容是有效的UTF-8字符串
                                            if isinstance(content_piece, str):
                                                collected_content.append(content_piece)
                                            logger.debug(f"提取到内容片段: {repr(content_piece)}")
                                except json.JSONDecodeError as e:
                                    logger.debug(f"JSON解析失败: {data_str[:50]}... 错误: {e}")
                                    continue

                        actual_response_content = ''.join(collected_content)
                        logger.info(f"测试连接: 从流式响应中提取到内容长度: {len(actual_response_content)}, 内容预览: {repr(actual_response_content[:50])}")

                        # 构造标准格式的response_data用于后续处理
                        response_data = {
                            "choices": [{"message": {"content": actual_response_content}}],
                            "usage": {"total_tokens": len(actual_response_content.split())},
                            "model": model_name
                        }

                    else:
                        # 未知响应格式
                        response_preview = chat_response.text[:200] + ('...' if len(chat_response.text) > 200 else '')
                        logger.error(f"测试连接: 未知响应格式 {chat_content_type}, 内容预览: {response_preview}")
                        raise ValueError(f"未知的响应格式: {chat_content_type}, 内容预览: {response_preview}")
                    
                    # 使用实际提取的响应内容并进行清理
                    content = actual_response_content if actual_response_content else "无响应内容"

                    # 清理和验证内容
                    if content and content != "无响应内容":
                        # 移除可能的控制字符，但保留换行符和制表符
                        import re
                        content = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', content)
                        # 确保内容是有效的UTF-8
                        try:
                            content = content.encode('utf-8').decode('utf-8')
                        except UnicodeError:
                            logger.warning("内容包含无效的UTF-8字符，尝试修复")
                            content = content.encode('utf-8', errors='replace').decode('utf-8')

                        logger.info(f"清理后的内容预览: {repr(content[:100])}")

                    # 获取响应头信息
                    headers_info = dict(chat_response.headers)
                    usage_info = response_data.get("usage", {}) if response_data else {}

                    # 确定响应类型
                    response_type = "流式响应" if chat_content_type.startswith('text/event-stream') else "标准JSON响应"

                    # 更新最后测试时间
                    for i, m in enumerate(self.config["models"]):
                        if m.get("id") == model_id:
                            self.config["models"][i]["last_tested"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            self.config["models"][i]["enabled"] = True
                            self._save_config()
                            break

                    logger.info(f"测试连接成功: 模型 {model_name}, 响应类型: {response_type}, 内容长度: {len(content)}")

                    return {
                        "success": True,
                        "message": "连接测试成功",
                        "response": f"连接正常，模型可用 ({response_type})",
                        "details": {
                            "model_response": content,
                            "response_type": response_type,
                            "content_type": chat_content_type,
                            "tokens_used": usage_info.get("total_tokens", 0),
                            "response_time": f"{chat_response.elapsed.total_seconds():.2f}秒",
                            "model_used": model_name,
                            "api_version": headers_info.get("x-ratelimit-limit-requests", "未知"),
                            "response_length": len(content),
                            "encoding": "UTF-8",
                            "content_preview": repr(content[:50]) if content else "无内容"
                        }
                    }
                except requests.exceptions.JSONDecodeError as e:
                    # 处理JSON解析错误
                    # 获取响应内容的前200个字符作为错误信息
                    response_text = "无法获取响应内容"
                    if 'models_response' in locals():
                        response_text = models_response.text[:200] + ('...' if len(models_response.text) > 200 else '')
                    elif 'chat_response' in locals():
                        response_text = chat_response.text[:200] + ('...' if len(chat_response.text) > 200 else '')
                    
                    logger.error(f"API响应JSON解析错误: {str(e)}, 响应内容: {response_text}")
                    
                    return {
                        "success": False,
                        "message": f"API响应格式错误，无法解析JSON: {str(e)}",
                        "error_details": {
                            "type": "JSON解析错误",
                            "raw_response": response_text,
                            "error": str(e)
                        }
                    }
                except ValueError as e:
                    # 处理非JSON响应错误
                    logger.error(f"API响应格式错误: {str(e)}")
                    
                    return {
                        "success": False,
                        "message": str(e),
                        "error_details": {
                            "type": "响应格式错误",
                            "error": str(e)
                        }
                    }
            else:
                return {
                    "success": False, 
                    "message": f"不支持的提供商: {provider}",
                    "error_details": {
                        "type": "不支持的提供商",
                        "provider": provider
                    }
                }
                
        except requests.exceptions.RequestException as e:
            # 处理请求异常
            for i, m in enumerate(self.config["models"]):
                if m.get("id") == model_id:
                    self.config["models"][i]["last_tested"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    self.config["models"][i]["enabled"] = False
                    self._save_config()
                    break
            
            # 获取更友好的错误消息
            error_msg = str(e)
            error_type = type(e).__name__
            status_code = getattr(e.response, 'status_code', None) if hasattr(e, 'response') else None
            
            error_details = {
                "type": error_type,
                "status_code": status_code,
                "raw_error": error_msg
            }
            
            # 尝试获取响应内容
            response_text = None
            if hasattr(e, 'response') and hasattr(e.response, 'text'):
                response_text = e.response.text[:200] + ('...' if len(e.response.text) > 200 else '')
                error_details["response_text"] = response_text
            
            if status_code == 401:
                error_msg = "认证失败，请检查API密钥"
                error_details["type"] = "认证错误"
            elif status_code == 404:
                error_msg = "API路径错误或资源不存在"
                error_details["type"] = "资源不存在"
            elif status_code == 429:
                error_msg = "API请求频率超限"
                error_details["type"] = "速率限制"
            elif hasattr(e, 'response') and hasattr(e.response, 'text'):
                try:
                    response_json = e.response.json()
                    if 'error' in response_json:
                        error_details["api_error"] = response_json['error'].get('message', '')
                        error_details["api_type"] = response_json['error'].get('type', '')
                        error_details["api_code"] = response_json['error'].get('code', '')
                        
                        # 提供更具体的错误消息
                        if response_json['error'].get('message'):
                            error_msg = f"API错误: {response_json['error'].get('message')}"
                except:
                    # 如果响应无法解析为JSON，则直接显示响应内容的一部分
                    if response_text:
                        error_msg = f"API错误: {response_text}"
            elif "ConnectionError" in error_msg or "timeout" in error_msg.lower():
                error_msg = "连接超时，请检查网络或API基础URL"
                error_details["type"] = "连接超时"
            
            return {
                "success": False,
                "message": f"连接测试失败: {error_msg}",
                "error_details": error_details
            }
        except Exception as e:
            # 更新最后测试时间并禁用模型
            for i, m in enumerate(self.config["models"]):
                if m.get("id") == model_id:
                    self.config["models"][i]["last_tested"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    self.config["models"][i]["enabled"] = False
                    self._save_config()
                    break
            
            # 获取更友好的错误消息
            error_msg = str(e)
            error_type = type(e).__name__
            
            error_details = {
                "type": error_type,
                "raw_error": error_msg
            }
            
            if "authentication" in error_msg.lower():
                error_msg = "认证失败，请检查API密钥"
                error_details["type"] = "认证错误"
            elif "connection" in error_msg.lower() or "timeout" in error_msg.lower():
                error_msg = "连接超时，请检查网络或API基础URL"
                error_details["type"] = "连接超时"
            elif "not found" in error_msg.lower() or "404" in error_msg:
                error_msg = "API路径错误或资源不存在"
                error_details["type"] = "资源不存在"
            elif "rate limit" in error_msg.lower() or "429" in error_msg:
                error_msg = "API请求频率超限"
                error_details["type"] = "速率限制"
            elif "model_dump" in error_msg.lower():
                error_msg = "接口响应格式错误，可能与LangChain不兼容"
                error_details["type"] = "格式不兼容"
            
            return {
                "success": False,
                "message": f"连接测试失败: {error_msg}",
                "error_details": error_details
            }
    
    def generate_analysis_report_stream(self, prompt, model_id=None):
        """生成AI分析报告 - 流式版本

        Args:
            prompt: 提示信息
            model_id: 指定的模型ID，若为None则使用第一个可用模型

        Yields:
            生成的报告文本片段
        """
        try:
            # 获取可用的模型
            models = self.get_enabled_models()
            if not models:
                logger.warning("没有可用的AI模型")
                yield from self._get_mock_response_stream()
                return

            # 选择模型
            model = None
            if model_id:
                model = self.get_model_by_id(model_id)
                if not model or not model.get("enabled", False):
                    logger.warning(f"指定的模型ID {model_id} 不可用，使用第一个可用模型")
                    model = models[0] if models else None
            else:
                model = models[0] if models else None

            if not model:
                logger.warning("没有可用的AI模型")
                yield from self._get_mock_response_stream()
                return

            # 获取模型配置
            provider = model.get("provider", "").lower()
            api_key = model.get("api_key", "")
            base_url = model.get("base_url", "")
            model_name = model.get("model", "")

            if not api_key:
                logger.warning("AI模型API密钥为空")
                yield from self._get_mock_response_stream()
                return

            # 调用API
            if provider == "openai":
                # 确保URL格式正确（去除尾部斜杠）
                clean_base_url = base_url
                if clean_base_url.endswith('/'):
                    clean_base_url = clean_base_url[:-1]

                # 直接使用API请求而不是LangChain
                headers = {
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json"
                }

                # 准备聊天请求 - 启用流式输出
                chat_url = f"{clean_base_url}/v1/chat/completions"
                chat_data = {
                    "model": model_name,
                    "messages": [
                        {"role": "user", "content": prompt}
                    ],
                    "temperature": 0.7,
                    "stream": True  # 启用流式输出
                }

                # 发送流式请求
                response = requests.post(chat_url, json=chat_data, headers=headers, timeout=60, stream=True)
                response.raise_for_status()

                # 处理流式响应
                for line in response.iter_lines():
                    if line:
                        line = line.decode('utf-8')
                        if line.startswith('data: '):
                            data_str = line[6:]  # 移除 'data: ' 前缀
                            if data_str.strip() == '[DONE]':
                                break
                            try:
                                data = json.loads(data_str)
                                if 'choices' in data and len(data['choices']) > 0:
                                    delta = data['choices'][0].get('delta', {})
                                    if 'content' in delta:
                                        content = delta['content']
                                        if content:
                                            yield content
                            except json.JSONDecodeError:
                                continue

            else:
                logger.warning(f"不支持的提供商: {provider}")
                yield from self._get_mock_response_stream()

        except Exception as e:
            logger.error(f"生成AI报告时出错: {str(e)}")
            yield from self._get_mock_response_stream()


    
    def _get_mock_response_stream(self):
        """当AI API调用失败时返回的流式消息"""
        import time

        mock_chunks = [
            '<h2>⚠️ AI调用失败</h2>',
            '<p>无法连接到AI服务，请检查网络连接或AI模型配置。</p>',
            '<h3>可能的原因：</h3>',
            '<ul>',
            '<li>API密钥无效或已过期</li>',
            '<li>网络连接问题</li>',
            '<li>AI服务暂时不可用</li>',
            '<li>模型配置参数错误</li>',
            '</ul>',
            '<p>请尝试重新配置AI模型或稍后再试。</p>'
        ]

        for chunk in mock_chunks:
            yield chunk
            time.sleep(0.1)  # 模拟流式输出的延迟

    def _get_mock_response(self):
        """当AI API调用失败时返回的消息"""
        return """
        <div class="ai-report">
            <div class="alert alert-warning">
                <i class="fas fa-exclamation-triangle me-2"></i>
                <strong>AI调用失败</strong>: 无法连接到AI服务，请检查网络连接或AI模型配置。
            </div>
            <p>可能的原因：</p>
            <ul>
                <li>API密钥无效或已过期</li>
                <li>网络连接问题</li>
                <li>AI服务暂时不可用</li>
                <li>模型配置参数错误</li>
        </ul>
            <p>请尝试重新配置AI模型或稍后再试。</p>
        </div>
        """

    def format_response_as_html(self, response_text):
        """将响应格式化为HTML"""
        # 检查响应是否已经是HTML格式
        if response_text.strip().startswith("<") and response_text.strip().endswith(">"):
            # 已经是HTML格式，但需要确保包含在ai-report div中
            if '<div class="ai-report">' not in response_text:
                return f'<div class="ai-report">{response_text}</div>'
            return response_text
        
        # 提取思考部分（如果有）
        thinking_content = ""
        main_content = response_text
        
        if "<think>" in response_text and "</think>" in response_text:
            think_start = response_text.find("<think>") + len("<think>")
            think_end = response_text.find("</think>")
            if think_start < think_end:
                thinking_content = response_text[think_start:think_end].strip()
                # 移除思考部分，只保留主要内容
                main_content = response_text[:think_start-len("<think>")] + response_text[think_end+len("</think>"):].strip()
        
        # 处理标题（假设#开头的行是标题）
        lines = main_content.split('\n')
        formatted_lines = []
        
        in_ul = False
        in_ol = False
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
            
            # 处理标题
            if line.startswith('# '):
                # 关闭任何打开的列表
                if in_ul:
                    formatted_lines.append('</ul>')
                    in_ul = False
                if in_ol:
                    formatted_lines.append('</ol>')
                    in_ol = False
                    
                formatted_lines.append(f'<h1>{line[2:]}</h1>')
            elif line.startswith('## '):
                # 关闭任何打开的列表
                if in_ul:
                    formatted_lines.append('</ul>')
                    in_ul = False
                if in_ol:
                    formatted_lines.append('</ol>')
                    in_ol = False
                    
                formatted_lines.append(f'<h2>{line[3:]}</h2>')
            elif line.startswith('### '):
                # 关闭任何打开的列表
                if in_ul:
                    formatted_lines.append('</ul>')
                    in_ul = False
                if in_ol:
                    formatted_lines.append('</ol>')
                    in_ol = False
                    
                formatted_lines.append(f'<h3>{line[4:]}</h3>')
            # 处理无序列表
            elif line.startswith('- ') or line.startswith('* '):
                # 如果在有序列表中，关闭它
                if in_ol:
                    formatted_lines.append('</ol>')
                    in_ol = False
                    
                # 如果不在无序列表中，开始一个新的
                if not in_ul:
                    formatted_lines.append('<ul>')
                    in_ul = True
                    
                formatted_lines.append(f'<li>{line[2:]}</li>')
            # 处理有序列表
            elif line.startswith('1. ') or (line[0].isdigit() and line[1:].startswith('. ')):
                # 提取序号后的内容
                content = line[line.find('.')+1:].strip()
                
                # 如果在无序列表中，关闭它
                if in_ul:
                    formatted_lines.append('</ul>')
                    in_ul = False
                    
                # 如果不在有序列表中，开始一个新的
                if not in_ol:
                    formatted_lines.append('<ol>')
                    in_ol = True
                    
                formatted_lines.append(f'<li>{content}</li>')
            else:
                # 关闭任何打开的列表
                if in_ul:
                    formatted_lines.append('</ul>')
                    in_ul = False
                if in_ol:
                    formatted_lines.append('</ol>')
                    in_ol = False
                
                # 处理常规段落
                formatted_lines.append(f'<p>{line}</p>')
        
        # 确保所有列表都被关闭
        if in_ul:
            formatted_lines.append('</ul>')
        if in_ol:
            formatted_lines.append('</ol>')
        
        # 将所有处理后的行连接成HTML
        formatted_text = '\n'.join(formatted_lines)
        
        # 将连续的短横线转换为分割线
        formatted_text = formatted_text.replace("<p>---</p>", "<hr>")
        
        # 添加思考区域（如果有）
        thinking_html = ""
        if thinking_content:
            thinking_html = f"""
            <div class="ai-thinking-section">
                <div class="ai-thinking-header" onclick="toggleThinking(this)">
                    <span>AI思考过程 (点击展开)</span>
                    <i class="fas fa-chevron-down"></i>
                </div>
                <div class="ai-thinking-content">
                    {thinking_content}
                </div>
            </div>
            """
        
        # 包装在div中，但不添加额外的标题
        formatted_html = f"""
        <div class="ai-report">
            {thinking_html}
            <div class="streaming-text">{formatted_text}</div>
        </div>
        """
        
        return formatted_html

    def get_available_models(self):
        """获取所有可用的AI模型信息，用于前端选择
        
        Returns:
            字典: 包含所有模型信息的字典，包括ID、名称、提供商和模型名称
        """
        try:
            models = self.get_all_models()
            
            # 对每个模型移除敏感信息
            for model in models:
                if 'api_key' in model:
                    # 隐藏完整API密钥，只保留最后几个字符用于标识
                    if model['api_key']:
                        model['api_key_preview'] = '*' * 10 + model['api_key'][-5:] if len(model['api_key']) > 5 else '*****'
                    else:
                        model['api_key_preview'] = ''
                    
                    # 从返回结果中删除完整API密钥
                    del model['api_key']
            
            logger.info(f"成功获取了 {len(models)} 个AI模型信息")
            return {
                "success": True,
                "models": models
            }
        except Exception as e:
            logger.error(f"获取可用模型列表时出错: {str(e)}")
            # 返回错误信息而不是抛出异常，确保API正常响应
            return {
                "success": False,
                "error": str(e),
                "message": "获取模型列表时出错"
            }

# 创建全局实例
ai_handler = AIHandler()

# 提供简单的访问函数
def get_all_models():
    """获取所有模型配置"""
    return ai_handler.get_all_models()

def get_enabled_models():
    """获取所有启用的模型配置"""
    return ai_handler.get_enabled_models()

def get_model_by_id(model_id):
    """通过ID获取模型配置"""
    try:
        if not model_id:
            logger.warning("尝试获取模型，但未提供有效的模型ID")
            return None
        
        # 获取模型配置
        model = ai_handler.get_model_by_id(model_id)
        
        # 记录获取结果
        if model:
            logger.info(f"成功获取ID为 {model_id} 的模型")
        else:
            logger.warning(f"找不到ID为 {model_id} 的模型")
        
        return model
    except Exception as e:
        logger.error(f"获取模型时出错 (ID: {model_id}): {str(e)}")
        # 重新抛出异常，让上层处理
        raise

def add_model(model_data):
    """添加新模型配置"""
    return ai_handler.add_model(model_data)

def update_model(model_data):
    """更新模型配置"""
    return ai_handler.update_model(model_data)

def delete_model(model_id):
    """删除模型配置"""
    return ai_handler.delete_model(model_id)

def test_model_connection(model_id):
    """测试模型连接是否正常"""
    return ai_handler.test_model_connection(model_id)



def generate_analysis_report_stream(prompt, model_id=None):
    """生成AI分析报告 - 流式版本"""
    return ai_handler.generate_analysis_report_stream(prompt, model_id)

def format_response_as_html(response_text):
    """将响应格式化为HTML"""
    return ai_handler.format_response_as_html(response_text)

def generate_apple_analysis_prompt(data):
    """根据苹果数据生成分析提示
    
    Args:
        data: 包含品质分布、特征数据等的字典
    
    Returns:
        生成的提示字符串
    """
    prompt = """
    <think>
    作为专业的苹果品质数据分析师，我需要基于真实数据进行科学分析：

    1. StandardScaler标准化数据解读原则：
       - 数据特征：均值=0，标准差=1，包含负值是正常现象
       - 相关性系数解读（针对标准化数据）：|r| ≥ 0.3 较强相关，0.15 ≤ |r| < 0.3 中等相关，|r| < 0.15 弱相关
       - 正相关(r > 0)：标准化特征值越高，品质越好
       - 负相关(r < 0)：标准化特征值越低，品质越好
       - 雷达图差异：基于3σ映射((-3,+3)→(0,10))的特征对比

    2. 分析框架：
       - 数据驱动：完全基于提供的真实数据，避免主观假设
       - 统计严谨：使用科学的统计方法解读数据
       - 实用导向：提供可操作的改进建议
       - 结构清晰：按逻辑层次组织分析内容

    3. 报告要求：
       - 使用专业术语但保持易懂
       - 提供具体数值支撑结论
       - 突出关键发现和洞察
       - 给出实际应用建议
    </think>

    # 苹果品质科学分析报告

    您好！我是专业的农产品品质数据分析师。我将基于您提供的苹果品质数据集的**真实计算结果**为您生成科学、准确的分析报告。

    ## 📊 真实数据概览
    {data_summary}

    ## ⚠️ StandardScaler数据处理重要说明
    **请特别注意**：
    1. **StandardScaler标准化**：使用sklearn.StandardScaler进行数据预处理
       - 标准化公式：(x - μ) / σ，其中μ是均值，σ是标准差
       - 结果特征：均值=0，标准差=1，数值范围通常在-3到+3之间
       - **负数是正常现象**：StandardScaler会产生负值，这不代表数据错误
       - 标准化消除了不同特征间的量纲差异，使比较更科学

    2. **相关性分析**：基于StandardScaler标准化数据的皮尔逊相关系数
       - StandardScaler使数据均值=0，标准差=1，会产生负值
       - 标准化数据的相关性阈值：|r| ≥ 0.3 为较强相关，0.15 ≤ |r| < 0.3 为中等相关
       - 0.05 ≤ |r| < 0.15 为弱相关，|r| < 0.05 为极弱相关

    3. **雷达图数据**：StandardScaler标准化值映射到0-10范围显示
       - 使用3σ原则映射：假设标准化数据在-3到+3范围，映射到0-10
       - 映射公式：mapped_value = ((standardized_value + 3) / 6) * 10
       - 保持了标准化数据的相对关系和差异特征

    ## 🎯 分析任务
    **请严格基于上述提供的真实数据进行分析，不要使用假设或示例数据。**

    请按以下结构生成基于真实数据的专业分析报告：

    ### 1. 📈 相关性强度分析（基于StandardScaler标准化数据）
    **请使用上述提供的具体相关性系数进行分析，不要编造数据**
    - **较强相关特征** (|r| ≥ 0.3)：引用具体的相关性系数值
    - **中等相关特征** (0.15 ≤ |r| < 0.3)：引用具体的相关性系数值
    - **弱相关特征** (0.05 ≤ |r| < 0.15)：引用具体的相关性系数值
    - **极弱相关特征** (|r| < 0.05)：引用具体的相关性系数值
    - **相关性方向解读**：基于实际正负值分析影响方向
    - **标准化影响说明**：解释StandardScaler对相关性的影响

    ### 2. 🎯 品质特征对比（基于真实雷达图数据）
    **请使用上述提供的具体雷达图数值进行分析**
    - **优质苹果特征画像**：引用具体的映射值（0-10范围）
    - **劣质苹果问题诊断**：引用具体的映射值和差异数据
    - **特征差异量化**：使用提供的具体差异值进行分析
    - **显著差异识别**：重点分析差异值大于1.0的特征

    ### 3. 📋 特征重要性排序（基于标准化数据）
    **基于真实的相关性系数和雷达图差异进行排序**
    - **综合评分方法**：相关性强度 × 特征差异程度
    - **关键控制点**：相关性≥0.3且差异≥1.0的特征（适应标准化数据）
    - **次要监控点**：中等相关性(0.15-0.3)或中等差异的特征
    - **具体排序结果**：列出前5个最重要的特征及其评分

    ### 4. 💡 科学结论与洞察（基于实际数据）
    **严格基于提供的数据得出结论，避免主观推测**
    - **核心发现**：从实际相关性和差异数据中发现的规律
    - **统计显著性**：基于相关性系数的强度评估
    - **数据一致性**：相关性分析与雷达图分析的一致性验证
    - **实际意义**：将统计结果转化为生产指导意义

    ### 5. 🚀 品质改进建议（数据支撑）
    **基于具体的相关性和差异数据提出建议**
    - **优先改进方向**：针对高相关性特征的具体建议
    - **量化改进目标**：基于优劣质差异提出具体改进幅度
    - **实施优先级**：根据相关性强度和改进难度排序
    - **预期效果评估**：基于相关性系数估算改进效果

    ## 📝 格式要求
    请使用规范的HTML格式，包含：
    - 清晰的标题层次：`<h1>`, `<h2>`, `<h3>`
    - 结构化段落：`<p>`
    - 有序/无序列表：`<ol><li>`, `<ul><li>`
    - 重点强调：`<strong>`, `<em>`
    - 数据表格：`<table><tr><th><td>`（用于展示具体数值）
    - 适当的分隔和空白，确保可读性

    ## ⚠️ 重要提醒
    1. **必须使用真实数据**：所有分析必须基于上述提供的具体数值
    2. **引用具体数值**：在分析中明确引用相关性系数、雷达图数值等
    3. **避免编造数据**：不要使用示例数据或假设数值
    4. **数据一致性检查**：确保相关性分析与雷达图分析结论一致
    5. **标准化数据理解**：正确理解标准化数据的含义和限制

    请确保分析客观、准确，严格基于提供的真实数据，避免任何主观臆断或数据编造。
    """
    
    # 准备数据摘要
    data_summary = "样本数据包含苹果的多种特征测量值，包括甜度、脆度、酸度等。"
    
    if "quality_distribution" in data and data["quality_distribution"]:
        try:
            # 检查是否有labels和values
            if "labels" in data["quality_distribution"] and "values" in data["quality_distribution"]:
                labels = data["quality_distribution"]["labels"]
                values = data["quality_distribution"]["values"]
                
                # 确保labels和values有相同的长度
                if len(labels) == len(values) and len(labels) >= 2:
                    # 假设第一个是优质，第二个是劣质
                    quality_info = f"优质苹果占比约为{values[0]:.1f}%，"
                    quality_info += f"劣质苹果占比约为{values[1]:.1f}%。"
                    data_summary += " " + quality_info
            # 兼容旧格式
            elif "good" in data["quality_distribution"] and "bad" in data["quality_distribution"]:
                quality_info = f"优质苹果占比约为{data['quality_distribution']['good']:.1f}%，"
                quality_info += f"劣质苹果占比约为{data['quality_distribution']['bad']:.1f}%。"
            data_summary += " " + quality_info
            
            logger.info("成功提取品质分布数据")
        except Exception as e:
            logger.error(f"提取品质分布数据出错: {str(e)}")
            pass
    
    if "correlations" in data and data["correlations"]:
        try:
            corr_info = "主要特征与品质的相关性："
            strong_corr = []
            medium_corr = []
            weak_corr = []

            for feature, value in data["correlations"].items():
                abs_value = abs(value)
                if abs_value >= 0.7:
                    strong_corr.append(f"{feature}({value:.3f})")
                elif abs_value >= 0.4:
                    medium_corr.append(f"{feature}({value:.3f})")
                else:
                    weak_corr.append(f"{feature}({value:.3f})")

            if strong_corr:
                corr_info += f" 强相关特征：{', '.join(strong_corr)}；"
            if medium_corr:
                corr_info += f" 中等相关特征：{', '.join(medium_corr)}；"
            if weak_corr:
                corr_info += f" 弱相关特征：{', '.join(weak_corr)}；"

            data_summary += " " + corr_info
            logger.info("成功提取相关性数据")
        except Exception as e:
            logger.error(f"提取相关性数据出错: {str(e)}")
            pass

    # 添加雷达图数据分析
    if "radar_analysis" in data and data["radar_analysis"]:
        try:
            radar_info = " 特征对比分析："
            if "features" in data["radar_analysis"] and "high_quality_avg" in data["radar_analysis"] and "low_quality_avg" in data["radar_analysis"]:
                features = data["radar_analysis"]["features"]
                high_values = data["radar_analysis"]["high_quality_avg"]
                low_values = data["radar_analysis"]["low_quality_avg"]

                for i, feature in enumerate(features):
                    if i < len(high_values) and i < len(low_values):
                        high_val = high_values[i]
                        low_val = low_values[i]
                        diff = high_val - low_val
                        if abs(diff) > 1:  # 显著差异
                            advantage = "优质更高" if diff > 0 else "劣质更高"
                            radar_info += f" {feature}({advantage}，差异{abs(diff):.1f})；"

                data_summary += radar_info
            logger.info("成功提取雷达图数据")
        except Exception as e:
            logger.error(f"提取雷达图数据出错: {str(e)}")
            pass

    # 添加特征差异数据
    if "feature_differences" in data and data["feature_differences"]:
        try:
            diff_info = " 关键特征差异："
            significant_diffs = []
            for feature, diff_data in data["feature_differences"].items():
                if abs(diff_data.get("difference", 0)) > 1:
                    significant_diffs.append(f"{feature}(差异{abs(diff_data['difference']):.1f})")

            if significant_diffs:
                diff_info += f" {', '.join(significant_diffs)}。"
                data_summary += diff_info
            logger.info("成功提取特征差异数据")
        except Exception as e:
            logger.error(f"提取特征差异数据出错: {str(e)}")
            pass

    # 添加详细的绘图数据信息
    if "correlation_analysis" in data and data["correlation_analysis"]:
        try:
            chart_info = "\n\n## 📊 详细绘图数据分析\n"
            chart_info += "以下是所有图表的详细数据，请基于这些真实数据进行分析：\n\n"

            # 相关性详细分析
            corr_analysis = data["correlation_analysis"]
            chart_info += "### 📈 相关性强度详细数据\n"
            chart_info += "**按相关性强度排序的完整数据：**\n"
            for item in corr_analysis["sorted_by_strength"]:
                chart_info += f"- **{item['feature']}**：{item['correlation']:.3f} ({item['strength']}{item['direction']})\n"

            if corr_analysis.get("strongest_positive"):
                sp = corr_analysis["strongest_positive"]
                chart_info += f"\n**最强正相关**：{sp['feature']} (r={sp['correlation']:.3f})\n"

            if corr_analysis.get("strongest_negative"):
                sn = corr_analysis["strongest_negative"]
                chart_info += f"**最强负相关**：{sn['feature']} (r={sn['correlation']:.3f})\n"

            if corr_analysis.get("weakest"):
                w = corr_analysis["weakest"]
                chart_info += f"**最弱相关**：{w['feature']} (r={w['correlation']:.3f})\n\n"

            data_summary += chart_info
            logger.info("成功添加相关性详细分析数据")
        except Exception as e:
            logger.error(f"添加相关性详细分析数据出错: {str(e)}")
            pass

    # 添加雷达图详细数据
    if "radar_detailed_analysis" in data and data["radar_detailed_analysis"]:
        try:
            radar_info = "### 🎯 雷达图特征对比详细数据\n"
            radar_analysis = data["radar_detailed_analysis"]

            radar_info += "**所有特征对比数据（按差异大小排序）：**\n"
            for comp in radar_analysis["feature_comparisons"]:
                radar_info += f"- **{comp['feature']}**：优质{comp['high_quality']:.1f} vs 劣质{comp['low_quality']:.1f} "
                radar_info += f"(差异{comp['difference']:+.1f}, {comp['significance']})\n"

            if radar_analysis["significant_differences"]:
                radar_info += f"\n**显著差异特征 (差异≥1.0)：**\n"
                for diff in radar_analysis["significant_differences"]:
                    radar_info += f"- {diff['feature']}：差异{diff['difference']:+.1f} ({diff['advantage']})\n"

            advantages = radar_analysis["advantages_summary"]
            if advantages["high_quality_advantages"]:
                radar_info += f"\n**优质苹果优势特征：** {', '.join(advantages['high_quality_advantages'])}\n"
            if advantages["low_quality_advantages"]:
                radar_info += f"**劣质苹果优势特征：** {', '.join(advantages['low_quality_advantages'])}\n"

            radar_info += "\n"
            data_summary += radar_info
            logger.info("成功添加雷达图详细分析数据")
        except Exception as e:
            logger.error(f"添加雷达图详细分析数据出错: {str(e)}")
            pass

    # 添加特征分布详细数据
    if "feature_distributions" in data and data["feature_distributions"]:
        try:
            feature_info = "### � 特征分布直方图详细数据\n"
            feature_info += "**每个特征的分布统计：**\n"

            for feature, dist_data in data["feature_distributions"].items():
                if isinstance(dist_data, dict) and "bins" in dist_data and "counts" in dist_data:
                    feature_info += f"- **{feature}**：\n"
                    feature_info += f"  - 总样本数：{dist_data.get('total_count', 0)}\n"
                    feature_info += f"  - 分布区间数：{len(dist_data['bins']) if dist_data['bins'] else 0}\n"

                    # 找出最高频率的区间
                    if dist_data['counts'] and len(dist_data['counts']) > 0:
                        max_count_idx = dist_data['counts'].index(max(dist_data['counts']))
                        if max_count_idx < len(dist_data['bins']):
                            feature_info += f"  - 最高频率区间：{dist_data['bins'][max_count_idx]} (样本数：{dist_data['counts'][max_count_idx]})\n"

                    # 显示前3个最高频率的区间
                    if dist_data['counts'] and len(dist_data['counts']) >= 3:
                        sorted_indices = sorted(range(len(dist_data['counts'])),
                                              key=lambda i: dist_data['counts'][i], reverse=True)[:3]
                        feature_info += f"  - 前3高频区间：\n"
                        for i, idx in enumerate(sorted_indices):
                            if idx < len(dist_data['bins']):
                                feature_info += f"    {i+1}. {dist_data['bins'][idx]}: {dist_data['counts'][idx]}样本\n"

            feature_info += "\n"
            data_summary += feature_info
            logger.info("成功添加特征分布详细数据")
        except Exception as e:
            logger.error(f"添加特征分布详细数据出错: {str(e)}")
            pass

    # 添加图表描述信息（用于不支持图片识别的模型）
    if "chart_descriptions" in data and data["chart_descriptions"]:
        try:
            chart_info = "\n\n## 📊 图表可视化描述\n"
            chart_info += "以下是图表的文字描述：\n\n"

            descriptions = data["chart_descriptions"]

            # 数据统计信息
            if "data_statistics" in descriptions:
                stats = descriptions["data_statistics"]
                chart_info += f"**📈 数据集统计**：\n"
                chart_info += f"- 总样本数：{stats.get('total_records', 0)}条\n"
                chart_info += f"- 优质苹果：{stats.get('good_quality_count', 0)}条\n"
                chart_info += f"- 劣质苹果：{stats.get('bad_quality_count', 0)}条\n"
                chart_info += f"- 分析特征数：{stats.get('features_count', 0)}个\n"
                chart_info += f"- 数据预处理：{stats.get('data_preprocessing', '未知')}\n\n"

            # 品质分布图
            if "quality_distribution" in descriptions:
                chart_info += f"**🥧 品质分布饼图**：{descriptions['quality_distribution']}\n"
                if "quality_distribution_data" in descriptions:
                    qd_data = descriptions["quality_distribution_data"]
                    chart_info += f"  - 图表类型：{qd_data.get('chart_type', '未知')}\n"
                    chart_info += f"  - 数据来源：{qd_data.get('data_source', '未知')}\n"
                    if qd_data.get('labels') and qd_data.get('values'):
                        for i, (label, value) in enumerate(zip(qd_data['labels'], qd_data['values'])):
                            chart_info += f"  - {label}：{value:.1f}%\n"
                chart_info += "\n"

            # 相关性强度图
            if "correlation_strength" in descriptions:
                chart_info += f"**📊 相关性强度条形图**：{descriptions['correlation_strength']}\n"
                if "correlation_data" in descriptions:
                    corr_data = descriptions["correlation_data"]
                    chart_info += f"  - 图表类型：{corr_data.get('chart_type', '未知')}\n"
                    chart_info += f"  - 评估标准：{corr_data.get('note', '未知')}\n"
                    if corr_data.get('correlations'):
                        chart_info += "  - 具体相关性系数：\n"
                        sorted_corr = sorted(corr_data['correlations'].items(),
                                           key=lambda x: abs(x[1]), reverse=True)
                        for feature, value in sorted_corr:
                            chart_info += f"    * {feature}: {value:.3f}\n"
                chart_info += "\n"

            # 雷达对比图
            if "radar_comparison" in descriptions:
                chart_info += f"**🎯 雷达对比图**：{descriptions['radar_comparison']}\n"
                if "radar_data" in descriptions:
                    radar_data = descriptions["radar_data"]
                    chart_info += f"  - 图表类型：{radar_data.get('chart_type', '未知')}\n"
                    chart_info += f"  - 数据说明：{radar_data.get('note', '未知')}\n"
                    if (radar_data.get('features') and
                        radar_data.get('high_quality_values') and
                        radar_data.get('low_quality_values')):
                        chart_info += "  - 特征对比数据：\n"
                        for i, feature in enumerate(radar_data['features']):
                            high_val = radar_data['high_quality_values'][i]
                            low_val = radar_data['low_quality_values'][i]
                            diff = high_val - low_val
                            chart_info += f"    * {feature}: 优质{high_val:.1f} vs 劣质{low_val:.1f} (差异{diff:+.1f})\n"
                chart_info += "\n"

            # 特征分布图
            if "feature_distributions" in descriptions:
                chart_info += f"**📈 特征分布直方图**：{descriptions['feature_distributions']}\n"
                if "feature_distributions_data" in descriptions:
                    fd_data = descriptions["feature_distributions_data"]
                    chart_info += f"  - 图表类型：{fd_data.get('chart_type', '未知')}\n"
                    chart_info += f"  - 数据来源：{fd_data.get('data_source', '未知')}\n"
                    chart_info += f"  - 分析特征：{', '.join(fd_data.get('features_analyzed', []))}\n"
                    chart_info += f"  - 说明：{fd_data.get('note', '未知')}\n"
                chart_info += "\n"

            # 相关性热图
            if "correlation_heatmap" in descriptions:
                chart_info += f"**🔥 相关性热图**：{descriptions['correlation_heatmap']}\n"
                if "heatmap_data" in descriptions:
                    hm_data = descriptions["heatmap_data"]
                    chart_info += f"  - 图表类型：{hm_data.get('chart_type', '未知')}\n"
                    chart_info += f"  - 颜色方案：{hm_data.get('color_scheme', '未知')}\n"
                    chart_info += f"  - 计算方法：{hm_data.get('note', '未知')}\n"
                chart_info += "\n"

            data_summary += chart_info
            logger.info("成功添加详细图表描述信息")
        except Exception as e:
            logger.error(f"添加图表描述信息出错: {str(e)}")
            pass

    # logger.info(f"生成的提示摘要: {data_summary}")
    return prompt.format(data_summary=data_summary)

def get_available_models():
    """获取所有可用的AI模型信息，用于前端选择
    
    Returns:
        字典: 包含所有模型信息的字典，包括ID、名称、提供商和模型名称
    """
    try:
        models = get_all_models()
        
        # 对每个模型移除敏感信息
        for model in models:
            if 'api_key' in model:
                # 隐藏完整API密钥，只保留最后几个字符用于标识
                if model['api_key']:
                    model['api_key_preview'] = '*' * 10 + model['api_key'][-5:] if len(model['api_key']) > 5 else '*****'
                else:
                    model['api_key_preview'] = ''
                
                # 从返回结果中删除完整API密钥
                del model['api_key']
        
        logger.info(f"成功获取了 {len(models)} 个AI模型信息")
        return {
            "success": True,
            "models": models
        }
    except Exception as e:
        logger.error(f"获取可用模型列表时出错: {str(e)}")
        # 返回错误信息而不是抛出异常，确保API正常响应
        return {
            "success": False,
            "error": str(e),
            "message": "获取模型列表时出错"
        } 