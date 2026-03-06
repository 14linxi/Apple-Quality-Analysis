import os
import io
import base64
import json
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')  # 设置非交互式后端
import matplotlib.pyplot as plt
import seaborn as sns
import plotly
import plotly.graph_objs as go
import plotly.utils
from flask import Flask, render_template, request, redirect, url_for, flash, session, jsonify, Response
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestClassifier, BaggingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.neighbors import KNeighborsClassifier
from sklearn.naive_bayes import GaussianNB
from sklearn.neural_network import MLPClassifier
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix
import joblib
import warnings
# PyHive已被Spark替代
# from pyhive import hive
import traceback
# 删除对data_utils的导入
# from data_utils import balance_classes, add_synthetic_samples
# 导入logger模块
# from logger import logger

# 导入AI模块
import ai

# 导入报告管理蓝图
from routes.reports import reports_bp

# 导入数据管理蓝图
from routes.data_management import data_management_bp

# 设置日志
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 修复pandas兼容性问题
def fix_pandas_compatibility():
    """修复pandas版本兼容性问题"""
    try:
        import pandas as pd
        pandas_version = pd.__version__
        logger.info(f"检测到Pandas版本: {pandas_version}")

        # 检查是否存在iteritems方法
        if not hasattr(pd.DataFrame, 'iteritems'):
            # 在新版本pandas中，iteritems已被弃用，使用items代替
            pd.DataFrame.iteritems = pd.DataFrame.items
            pd.Series.iteritems = pd.Series.items
            logger.info("已修复pandas iteritems兼容性问题")

        return True
    except Exception as e:
        logger.error(f"修复pandas兼容性失败: {str(e)}")
        return False

# 应用兼容性修复
fix_pandas_compatibility()

# 导入Spark数据处理器
try:
    from spark_data_processor import SparkDataProcessor
    SPARK_AVAILABLE = True
    logger.info("Spark数据处理器导入成功")
    # 创建全局Spark处理器实例（本地模式）
    spark_processor = SparkDataProcessor(mode="local")
    # 初始化Spark会话
    if spark_processor.init_spark():
        logger.info("Spark会话初始化成功")
    else:
        logger.error("Spark会话初始化失败")
        SPARK_AVAILABLE = False
        spark_processor = None
except ImportError as e:
    SPARK_AVAILABLE = False
    spark_processor = None
    logger.error(f"Spark数据处理器不可用: {str(e)}")
    logger.error("请确保PySpark已安装且Spark集群正在运行")

# 设置日志
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 忽略警告信息
warnings.filterwarnings('ignore')

# 设置模型保存目录
MODEL_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'models')
os.makedirs(MODEL_DIR, exist_ok=True)

app = Flask(__name__)
app.secret_key = 'apple_quality_analysis_secret_key'  # 设置秘钥用于会话管理
app.config['ENV'] = os.environ.get('FLASK_ENV', 'development')  # 默认为开发环境

# 注册蓝图
app.register_blueprint(reports_bp)
app.register_blueprint(data_management_bp)

# 配置信息
HIVE_CONFIG = {
    'host': '192.168.28.180',
    'port': 10000,
    'username': 'root',
    'database': 'test'
}

# 获取数据函数 - 使用Spark替代PyHive
def get_hive_data(query=None, limit=None):
    """使用Spark从Hive获取数据，返回Pandas DataFrame"""
    if not SPARK_AVAILABLE or spark_processor is None:
        logger.error("Spark不可用，无法获取数据")
        raise Exception("Spark服务不可用，请检查Spark集群是否正在运行")

    try:
        # 使用Spark数据处理器获取数据
        spark_df = spark_processor.get_hive_data(query, limit)
        if spark_df is None:
            logger.error("Spark获取Hive数据失败")
            raise Exception("无法从Hive获取数据，请检查Spark和Hive服务状态")

        # 转换为Pandas DataFrame用于前端显示
        pandas_df = spark_processor.to_pandas(spark_df)
        if pandas_df is None:
            logger.error("Spark DataFrame转换为Pandas失败")
            raise Exception("数据转换失败")

        logger.info(f"成功获取数据: {pandas_df.shape}")
        return pandas_df

    except Exception as e:
        logger.error(f"获取Hive数据时出错: {str(e)}")
        raise e
def get_hive_data1(query=None, limit=None):
    """从Hive获取数据并直接转换为Pandas DataFrame，用于模型训练"""
    if not SPARK_AVAILABLE or spark_processor is None:
        logger.error("Spark不可用，无法获取数据")
        raise Exception("Spark服务不可用，请检查Spark集群是否正在运行")

    try:
        # 初始化Spark会话
        if not spark_processor.init_spark():
            logger.error("Spark初始化失败")
            raise Exception("无法初始化Spark会话")

        # 构建查询语句
        if query is None:
            # 默认查询模型训练需要的数据表
            query = "SELECT * FROM test.apple_quality"

        # 添加LIMIT子句
        if limit:
            if "LIMIT" not in query.upper():
                query = f"{query} LIMIT {limit}"

        logger.info(f"执行数据查询: {query}")

        # 直接执行SQL查询
        spark_df = spark_processor.spark.sql(query)
        if spark_df is None:
            logger.error("Spark SQL查询失败")
            raise Exception("无法执行SQL查询")

        # 转换为Pandas DataFrame
        pandas_df = spark_processor.to_pandas(spark_df)
        if pandas_df is None:
            logger.error("Spark DataFrame转换为Pandas失败")
            raise Exception("数据转换失败")

        logger.info(f"成功获取数据: {pandas_df.shape}")
        return pandas_df

    except Exception as e:
        logger.error(f"获取Hive数据时出错: {str(e)}")
        raise e



# 获取原始数据函数 - 专门用于数据页面显示原始数据
def get_raw_hive_data(query=None, limit=None):
    """获取原始的Hive数据，不进行任何预处理，用于数据页面显示"""
    if not SPARK_AVAILABLE or spark_processor is None:
        logger.error("Spark不可用，无法获取数据")
        raise Exception("Spark服务不可用，请检查Spark集群是否正在运行")

    try:
        # 直接使用SQL查询获取原始数据，确保不经过任何预处理
        if query is None:
            # 构建原始查询，获取所有字段和所有记录
            query = "SELECT * FROM test.apple_quality2"

        # 添加LIMIT子句
        if limit:
            if "LIMIT" not in query.upper():
                query = f"{query} LIMIT {limit}"

        logger.info(f"执行原始数据查询: {query}")

        # 直接执行SQL查询
        spark_df = spark_processor.spark.sql(query)
        if spark_df is None:
            logger.error("Spark SQL查询失败")
            raise Exception("无法执行SQL查询")

        # 获取记录数
        record_count = spark_df.count()
        logger.info(f"查询到 {record_count} 条原始记录")

        # 转换为Pandas DataFrame，保持原始数据不变
        pandas_df = spark_processor.to_pandas(spark_df)
        if pandas_df is None:
            logger.error("Spark DataFrame转换为Pandas失败")
            raise Exception("数据转换失败")

        logger.info(f"成功获取原始数据: {pandas_df.shape} (完全未经处理)")

        # 显示数据样本用于验证
        if len(pandas_df) > 0:
            logger.info(f"原始数据样本 (前3条):")
            for i in range(min(3, len(pandas_df))):
                logger.info(f"  记录 {i+1}: {pandas_df.iloc[i].to_dict()}")

        return pandas_df

    except Exception as e:
        logger.error(f"获取原始Hive数据时出错: {str(e)}")
        raise e



# 数据预处理函数 - 直接使用pandas处理
def preprocess_data(df):
    """使用pandas直接预处理DataFrame，用于模型训练"""
    if df is None or df.empty:
        logger.warning("预处理阶段: 数据为空")
        return None

    try:
        logger.info(f"开始数据预处理，原始数据大小: {df.shape}")

        # 创建数据副本，避免修改原始数据
        processed_df = df.copy()

        # 1. 删除空值
        initial_count = len(processed_df)
        processed_df = processed_df.dropna()
        logger.info(f"删除空值后数据大小: {processed_df.shape} (删除了 {initial_count - len(processed_df)} 条记录)")

        # 2. 删除重复行
        initial_count = len(processed_df)
        processed_df = processed_df.drop_duplicates()
        logger.info(f"删除重复行后数据大小: {processed_df.shape} (删除了 {initial_count - len(processed_df)} 条记录)")

        # 3. 删除不需要的列（如果存在）
        if 'a_id' in processed_df.columns:
            processed_df = processed_df.drop('a_id', axis=1)
            logger.info("删除a_id列")

        # 4. 确保数值列的数据类型正确
        numeric_columns = ['size', 'weight', 'sweetness', 'crunchiness', 'juiciness', 'ripeness', 'acidity']
        for col in numeric_columns:
            if col in processed_df.columns:
                processed_df[col] = pd.to_numeric(processed_df[col], errors='coerce')

        # 5. 处理目标变量quality
        if 'quality' in processed_df.columns:
            # 将quality转换为数值类型，并映射为0和1
            processed_df['quality'] = processed_df['quality'].astype(str).str.lower()
            quality_mapping = {'bad': 0, 'good': 1, '0': 0, '1': 1, 'false': 0, 'true': 1}
            processed_df['quality'] = processed_df['quality'].map(quality_mapping)

            # 删除无法映射的quality值
            before_quality_filter = len(processed_df)
            processed_df = processed_df.dropna(subset=['quality'])
            processed_df['quality'] = processed_df['quality'].astype(int)
            logger.info(f"处理quality列后数据大小: {processed_df.shape} (删除了 {before_quality_filter - len(processed_df)} 条无效记录)")

            # 显示quality分布
            quality_counts = processed_df['quality'].value_counts().to_dict()
            logger.info(f"Quality分布: {quality_counts}")

        # 6. 再次删除任何产生的空值
        processed_df = processed_df.dropna()

        logger.info(f"数据预处理完成，最终数据大小: {processed_df.shape}")
        return processed_df

    except Exception as e:
        logger.error(f"数据预处理失败: {str(e)}")
        import traceback
        traceback.print_exc()
        return df  # 返回原始数据



# 异常值处理函数
def remove_outliers(df, column):
    try:
        df[column] = pd.to_numeric(df[column])
    except ValueError:
        return df
    
    Q1 = df[column].quantile(0.25)
    Q3 = df[column].quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    
    return df[(df[column] >= lower_bound) & (df[column] <= upper_bound)]

# 训练并保存模型
def train_and_save_models(X_train, X_test, y_train, y_test, random_state=42):
    models = {
        'naive_bayes': GaussianNB(),
        'neural_network': MLPClassifier(hidden_layer_sizes=(1000, 10), activation='relu', solver='adam', random_state=random_state),
        'logistic_regression': LogisticRegression(random_state=random_state),
        'random_forest': RandomForestClassifier(random_state=random_state),
        'knn': KNeighborsClassifier(n_neighbors=5),
        'bagging': BaggingClassifier(n_estimators=10, random_state=random_state)
    }
    
    # 打印测试集中的标签分布
    y_test_counts = pd.Series(y_test).value_counts().to_dict()
    print(f"测试集中的标签分布: {y_test_counts}")
    
    # 检查测试集是否包含所有类别
    if 0 not in y_test_counts or 1 not in y_test_counts:
        print("警告：测试集中缺少一个或多个类别")
    
    results = {}
    
    for name, model in models.items():
        model.fit(X_train, y_train)
        y_pred = model.predict(X_test)
        accuracy = accuracy_score(y_test, y_pred)
        
        # # 打印预测标签分布
        # y_pred_counts = pd.Series(y_pred).value_counts().to_dict()
        # print(f"{name}模型预测的标签分布: {y_pred_counts}")
        
        # 获取报告
        report = classification_report(y_test, y_pred, output_dict=True, target_names=['劣质', '优质'])
        
        # 打印报告的键
        # print(f"{name}模型的报告键: {list(report.keys())}")
        
        # 标准化报告键，处理各种可能的键名格式
        standardized_report = {}
        
        # 首先复制所有非类别的键（如'accuracy', 'macro avg', 'weighted avg'）
        for key in report:
            if key not in ['劣质', '优质', '0', '1', '0.0', '1.0', 0, 1, 0.0, 1.0]:
                standardized_report[key] = report[key]
        
        # 处理类别映射
        # 劣质 = 0, 优质 = 1 (根据数据预处理阶段的映射)
        
        # 处理劣质(0)的数据
        if '劣质' in report:
            standardized_report['0'] = report['劣质']
        elif '0' in report:
            standardized_report['0'] = report['0']
        elif 0 in report:
            standardized_report['0'] = report[0]
        elif '0.0' in report:
            standardized_report['0'] = report['0.0']
        elif 0.0 in report:
            standardized_report['0'] = report[0.0]
        else:
            print(f"报告中缺少劣质(0)类别，添加默认值")
            weighted_avg = report.get('weighted avg', {})
            standardized_report['0'] = {
                'precision': weighted_avg.get('precision', 0.0),
                'recall': weighted_avg.get('recall', 0.0),
                'f1-score': weighted_avg.get('f1-score', 0.0),
                'support': int(weighted_avg.get('support', 0) * 0.4) if weighted_avg.get('support', 0) > 0 else 0
            }
        
        # 处理优质(1)的数据
        if '优质' in report:
            standardized_report['1'] = report['优质']
        elif '1' in report:
            standardized_report['1'] = report['1']
        elif 1 in report:
            standardized_report['1'] = report[1]
        elif '1.0' in report:
            standardized_report['1'] = report['1.0']
        elif 1.0 in report:
            standardized_report['1'] = report[1.0]
        else:
            print(f"报告中缺少优质(1)类别，添加默认值")
            weighted_avg = report.get('weighted avg', {})
            standardized_report['1'] = {
                'precision': weighted_avg.get('precision', 0.0),
                'recall': weighted_avg.get('recall', 0.0),
                'f1-score': weighted_avg.get('f1-score', 0.0),
                'support': int(weighted_avg.get('support', 0) * 0.6) if weighted_avg.get('support', 0) > 0 else 0
            }
        
        # 确保报告中类别标签使用中文（为前端展示）
        standardized_report['class_names'] = {
            '0': '劣质',
            '1': '优质'
        }
        
        # 保存模型
        joblib.dump(model, os.path.join(MODEL_DIR, f"{name}_model.pkl"))
        
        # # 打印最终的报告内容
        # print(f"{name}模型最终报告中的劣质(0)类别指标: {standardized_report.get('0', {})}")
        # print(f"{name}模型最终报告中的优质(1)类别指标: {standardized_report.get('1', {})}")
        
        results[name] = {
            'accuracy': accuracy,
            'report': standardized_report
        }
    
    return results

# 生成模型评估图表
def generate_model_comparison_chart(results):
    # 按准确度从高到低排序模型
    models = list(results.keys())
    models.sort(key=lambda x: results[x]['accuracy'], reverse=True)
    
    accuracies = [results[model]['accuracy'] for model in models]
    
    model_names_chinese = {
        'naive_bayes': '高斯朴素贝叶斯',
        'neural_network': 'BP神经网络',
        'logistic_regression': '逻辑回归模型',
        'random_forest': '随机森林',
        'knn': 'KNN',
        'bagging': '装袋模型'
    }
    
    chinese_names = [model_names_chinese.get(model, model) for model in models]
    
    fig = go.Figure(data=[
        go.Bar(
            x=chinese_names,
            y=[acc * 100 for acc in accuracies],  # 将精确度转换为百分比
            text=[f'{acc * 100:.2f}%' for acc in accuracies],  # 显示为百分比形式，保留两位小数
            textposition='auto',
            marker_color=['#63b2ee', '#76da91', '#f8cb7f', '#f89588', '#7cd6cf', '#9192ab']
        )
    ])
    
    fig.update_layout(
        title='模型准确性比较',
        xaxis_title='模型',
        yaxis_title='准确度(%)',  # 更新Y轴标题，表明是百分比
        yaxis=dict(range=[0, 100]),  # 更新Y轴范围为0-100
        plot_bgcolor='white'
    )
    
    return json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)

# 计算相关性数据用于前端图表 - 使用Spark处理器
def calculate_correlation_data(df):
    """计算相关性数据，内部使用Spark处理器"""
    if not SPARK_AVAILABLE or spark_processor is None:
        logger.error("Spark不可用，无法计算相关性数据")
        return {'labels': [], 'values': [], 'colors': []}

    try:
        # 将Pandas DataFrame转换为Spark DataFrame
        spark_session = spark_processor.spark
        if spark_session is None:
            spark_processor.init_spark()
            spark_session = spark_processor.spark

        if spark_session is None:
            logger.error("无法初始化Spark会话")
            return {'labels': [], 'values': [], 'colors': []}

        # 转换为Spark DataFrame并计算相关性
        spark_df = spark_session.createDataFrame(df)
        correlation_data = spark_processor.calculate_correlation_data(spark_df)

        logger.info(f"相关性计算完成: {len(correlation_data.get('labels', []))} 个特征")
        return correlation_data

    except Exception as e:
        logger.error(f"计算相关性数据时出错: {str(e)}")
        return {'labels': [], 'values': [], 'colors': []}

# 计算雷达图数据 - 使用Spark处理器
def calculate_radar_data(df):
    """计算雷达图数据，内部使用Spark处理器"""
    if not SPARK_AVAILABLE or spark_processor is None:
        logger.error("Spark不可用，无法计算雷达图数据")
        return {'features': [], 'high_quality': [], 'low_quality': []}

    try:
        # 将Pandas DataFrame转换为Spark DataFrame
        spark_session = spark_processor.spark
        if spark_session is None:
            spark_processor.init_spark()
            spark_session = spark_processor.spark

        if spark_session is None:
            logger.error("无法初始化Spark会话")
            return {'features': [], 'high_quality': [], 'low_quality': []}

        # 转换为Spark DataFrame并计算雷达图数据
        spark_df = spark_session.createDataFrame(df)
        radar_data = spark_processor.calculate_radar_data(spark_df)

        logger.info(f"雷达图数据计算完成: {len(radar_data.get('features', []))} 个特征")
        return radar_data

    except Exception as e:
        logger.error(f"计算雷达图数据时出错: {str(e)}")
        return {'features': [], 'high_quality': [], 'low_quality': []}

# 生成相关性热图
def generate_correlation_heatmap(df):
    try:
        # 禁用matplotlib的所有交互式后端
        import matplotlib
        matplotlib.use('Agg')  # 非交互式后端
        # 确保Tkinter不会被使用
        import os
        os.environ['MPLBACKEND'] = 'Agg'
        
        import matplotlib.pyplot as plt
        import seaborn as sns
        
        # 设置中文显示
        plt.rcParams['font.sans-serif'] = ['SimHei', 'Arial Unicode MS', 'Microsoft YaHei', 'Arial']  # 尝试多种中文字体
        plt.rcParams['axes.unicode_minus'] = False  # 正确显示负号
        
        # 计算相关系数矩阵
        correlation_matrix = df.corr(numeric_only=True)  # 确保只对数值列进行计算
        
        # 创建新图形
        plt.figure(figsize=(12, 10))  # 增大图形尺寸
        plt.clf()
        
        # 绘制热图
        sns.heatmap(
            correlation_matrix, 
            annot=True, 
            cmap='coolwarm', 
            linewidths=0.5,
            vmin=-1, 
            vmax=1, 
            center=0,
            square=True,
            fmt='.2f'  # 保留两位小数
        )
        
        plt.title('特征相关性热图', fontsize=16)
        plt.tight_layout()  # 调整布局，避免标签被截断
        
        # 将图保存为base64字符串
        buffer = io.BytesIO()
        plt.savefig(buffer, format='png', dpi=120)  # 增加分辨率
        buffer.seek(0)
        plot_data = base64.b64encode(buffer.getvalue()).decode('utf-8')
        plt.close('all')  # 确保关闭所有图形，释放资源
        
        print("相关性热图生成成功")
        return plot_data
    
    except Exception as e:
        print(f"生成相关性热图时出错: {str(e)}")
        # 创建错误信息图
        plt.figure(figsize=(8, 6))
        plt.text(0.5, 0.5, f"热图生成失败: {str(e)}", 
                 horizontalalignment='center', fontsize=12, color='red')
        plt.axis('off')
        
        buffer = io.BytesIO()
        plt.savefig(buffer, format='png')
        buffer.seek(0)
        plot_data = base64.b64encode(buffer.getvalue()).decode('utf-8')
        plt.close('all')
        
        return plot_data

# 使用保存的模型进行预测
def predict_with_model(model_name, features):
    try:
        # 加载模型
        model_path = os.path.join(MODEL_DIR, f"{model_name}_model.pkl")
        if os.path.exists(model_path):
            model = joblib.load(model_path)
            
            # 进行预测
            prediction = model.predict([features])[0]
            
            return int(prediction)
        else:
            return None
    except Exception as e:
        print(f"使用模型预测时出错: {str(e)}")
        return None

# 添加一个内部的balance_classes函数，替代data_utils中的函数
def balance_classes(df, target_column='quality', min_ratio=0.4):
    """
    平衡数据集中的类别分布
    
    参数:
    df: 包含目标变量的DataFrame
    target_column: 目标变量的列名
    min_ratio: 最小类别应占总样本的最小比例
    
    返回:
    平衡后的DataFrame
    """
    from sklearn.utils import resample
    
    if target_column not in df.columns:
        print(f"目标列 {target_column} 不在DataFrame中")
        return df
    
    # 获取类别计数
    class_counts = df[target_column].value_counts()
    
    # 如果只有一个类别或类别已经平衡，则返回原始DataFrame
    if len(class_counts) <= 1:
        print(f"只有一个类别，无法平衡")
        return df
    
    # 获取多数类和少数类
    majority_class = class_counts.idxmax()
    minority_class = class_counts.idxmin()
    
    # 计算当前的类别比例
    minority_ratio = class_counts.min() / class_counts.sum()
    
    # 如果少数类比例已经满足要求，则返回原始DataFrame
    if minority_ratio >= min_ratio:
        print("类别分布已经满足要求，无需平衡")
        return df
    
    # 分离多数类和少数类样本
    majority_samples = df[df[target_column] == majority_class]
    minority_samples = df[df[target_column] == minority_class]
    
    # 计算多数类需要的样本数量，以满足最小比例要求
    majority_samples_needed = int(len(minority_samples) * (1 - min_ratio) / min_ratio)
    
    # 如果计算出的样本数量大于现有多数类样本，则进行上采样
    if majority_samples_needed > len(majority_samples):
        # 对少数类上采样
        print(f"对少数类进行上采样")
        minority_upsampled = resample(
            minority_samples,
            replace=True,
            n_samples=int(majority_samples.shape[0] * min_ratio / (1 - min_ratio)),
            random_state=42
        )
        # 合并数据集
        balanced_df = pd.concat([majority_samples, minority_upsampled])
    else:
        # 对多数类下采样
        print(f"对多数类进行下采样，从{len(majority_samples)}降至{majority_samples_needed}")
        majority_downsampled = resample(
            majority_samples,
            replace=False,
            n_samples=majority_samples_needed,
            random_state=42
        )
        # 合并数据集
        balanced_df = pd.concat([majority_downsampled, minority_samples])
    
    # 打乱数据
    balanced_df = balanced_df.sample(frac=1, random_state=42).reset_index(drop=True)
    
    return balanced_df

@app.route('/')
def index():
    return render_template('index.html')



@app.route('/api/test/spark')
def test_spark_connection():
    """测试Spark连接"""
    try:
        if not SPARK_AVAILABLE or spark_processor is None:
            return jsonify({'success': False, 'error': 'Spark不可用'})

        # 测试简单查询
        test_df = spark_processor.spark.sql("SELECT COUNT(*) as count FROM test.apple_quality2")
        count = test_df.collect()[0]['count']

        return jsonify({
            'success': True,
            'message': f'Spark连接正常，表中有{count}条记录',
            'count': count
        })

    except Exception as e:
        logger.error(f"Spark连接测试失败: {str(e)}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/data/save', methods=['POST'])
def api_save_record():
    """保存记录API"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({'success': False, 'error': '无效的数据'}), 400

        record_id = data.get('record_id')
        record_data = {
            'size': float(data.get('size', 0)),
            'weight': float(data.get('weight', 0)),
            'sweetness': float(data.get('sweetness', 0)),
            'crunchiness': float(data.get('crunchiness', 0)),
            'juiciness': float(data.get('juiciness', 0)),
            'ripeness': float(data.get('ripeness', 0)),
            'acidity': float(data.get('acidity', 0)),
            'quality': data.get('quality', 'good')
        }

        if not SPARK_AVAILABLE or spark_processor is None:
            logger.error("Spark服务不可用")
            return jsonify({'success': False, 'error': 'Spark服务不可用'}), 500

        # 使用data_change_manager进行数据操作，确保Kafka消息发送
        from data_change_manager import DataChangeManager
        manager = DataChangeManager()

        if record_id:
            # 更新记录
            result = manager.update_record(record_id, record_data, user_triggered=True)
            if result['success']:
                # 返回更新后的完整记录数据
                updated_data = record_data.copy()
                updated_data['a_id'] = record_id
                return jsonify({
                    'success': True,
                    'message': '记录更新成功',
                    'data': updated_data
                })
            else:
                return jsonify({'success': False, 'error': result['error']}), 500
        else:
            # 新增记录
            logger.info(f"开始创建新记录: {record_data}")
            result = manager.insert_record(record_data, user_triggered=True)
            if result['success']:
                new_id = result['data']['a_id']
                logger.info(f"记录创建成功，新ID: {new_id}")
                # 返回完整的新记录数据
                return jsonify({
                    'success': True,
                    'message': '记录创建成功',
                    'data': result['data']
                })
            else:
                logger.error(f"创建记录失败: {result['error']}")
                return jsonify({'success': False, 'error': result['error']}), 500

    except Exception as e:
        logger.error(f"保存记录失败: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/data/delete/<int:record_id>', methods=['DELETE'])
def api_delete_record(record_id):
    """删除记录API"""
    try:
        if not SPARK_AVAILABLE or spark_processor is None:
            return jsonify({'success': False, 'error': 'Spark服务不可用'}), 500

        # 使用data_change_manager进行删除操作，确保Kafka消息发送
        from data_change_manager import DataChangeManager
        manager = DataChangeManager()

        result = manager.delete_record(record_id, user_triggered=True)
        if result['success']:
            return jsonify({'success': True, 'message': '记录删除成功'})
        else:
            return jsonify({'success': False, 'error': result['error']}), 500

    except Exception as e:
        logger.error(f"删除记录失败: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/data/check-updates', methods=['POST'])
def api_check_updates():
    """检查数据更新API"""
    try:
        data = request.get_json()
        current_count = data.get('current_count', 0)
        last_record_id = data.get('last_record_id', 0)

        # 使用简单的COUNT查询，避免复杂操作
        try:
            if SPARK_AVAILABLE and spark_processor is not None:
                # 使用Spark进行简单查询
                count_df = spark_processor.spark.sql("SELECT COUNT(*) as total FROM test.apple_quality2")
                total_count = count_df.collect()[0]['total']

                # 使用简单的MAX查询
                max_id_df = spark_processor.spark.sql("SELECT COALESCE(MAX(a_id), 0) as max_id FROM test.apple_quality2")
                max_id = max_id_df.collect()[0]['max_id']
            else:
                # 备用方案：使用data_change_manager
                from data_change_manager import DataChangeManager
                manager = DataChangeManager()

                # 获取少量数据来估算总数
                result = manager.get_data_list(limit=1)
                if result['success']:
                    total_count = result['total']
                    # 获取最大ID（从数据中估算）
                    if result['data']:
                        max_id = max([record.get('a_id', 0) for record in result['data']])
                    else:
                        max_id = 0
                else:
                    total_count = current_count
                    max_id = last_record_id or 0

        except Exception as query_error:
            logger.warning(f"查询统计信息失败: {str(query_error)}")
            # 如果查询失败，返回保守的结果
            total_count = current_count
            max_id = last_record_id or 0

        # 添加调试日志
        logger.info(f"检查更新: current_count={current_count}, last_record_id={last_record_id}, max_id={max_id}, total_count={total_count}")

        # 检查是否有更新 - 使用更合理的逻辑
        has_updates = False
        new_records = 0

        # 现在last_record_id是前端页面显示的最大ID
        # 只有当数据库最大ID大于页面最大ID时，才认为有新记录
        if last_record_id is not None and max_id > last_record_id:
            has_updates = True
            new_records = max_id - last_record_id
        elif last_record_id is None and max_id > 0:
            # 如果页面没有记录但数据库有记录，说明有新数据
            has_updates = True
            new_records = 1

        logger.info(f"更新检查结果: has_updates={has_updates}, new_records={new_records}")

        return jsonify({
            'success': True,
            'has_updates': has_updates,
            'new_records': new_records,
            'total_count': total_count,
            'max_id': max_id
        })

    except Exception as e:
        logger.error(f"检查数据更新失败: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/data/list')
def api_data_list():
    """获取数据列表API"""
    try:
        limit = request.args.get('limit', 50, type=int)
        format_type = request.args.get('format', 'html')

        # 使用data_change_manager获取数据，避免MapReduce问题
        from data_change_manager import DataChangeManager
        manager = DataChangeManager()

        result = manager.get_data_list(limit=limit)

        if result['success']:
            if format_type == 'json':
                # 返回JSON格式
                return jsonify({
                    'success': True,
                    'data': result['data'],
                    'total': result['total']
                })
            else:
                # 返回HTML格式（原有逻辑）
                return jsonify({'success': False, 'error': '不支持的格式'}), 400
        else:
            return jsonify({'success': False, 'error': result['error']}), 500

    except Exception as e:
        logger.error(f"获取数据列表失败: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

def update_record_in_hive(record_id, record_data):
    """在Hive中更新记录 - 使用Spark SQL方式"""
    try:
        # 使用与data_change_manager相同的Spark SQL方式
        # 构建CASE语句，确保数据类型正确
        field_types = {
            'size': 'double', 'weight': 'double', 'sweetness': 'double',
            'crunchiness': 'double', 'juiciness': 'double', 'ripeness': 'double',
            'acidity': 'double', 'quality': 'string'
        }

        case_statements = ["a_id"]  # 先添加a_id字段

        for field in field_types:
            if field in record_data:
                # 更新的字段
                if field_types[field] == 'string':
                    case_statements.append(f"CASE WHEN a_id = {record_id} THEN '{record_data[field]}' ELSE {field} END as {field}")
                else:
                    # 确保数值类型正确转换
                    case_statements.append(f"CASE WHEN a_id = {record_id} THEN CAST({record_data[field]} AS DOUBLE) ELSE {field} END as {field}")
            else:
                # 未更改的字段
                case_statements.append(f"{field}")

        # 创建临时视图
        update_sql = f"""
        CREATE OR REPLACE TEMPORARY VIEW temp_updated AS
        SELECT {', '.join(case_statements)}
        FROM test.apple_quality2
        """

        spark_processor.spark.sql(update_sql)

        # 重新写入原表
        spark_processor.spark.sql("""
            INSERT OVERWRITE TABLE test.apple_quality2
            SELECT * FROM temp_updated
        """)

        logger.info(f"记录 {record_id} 使用Spark SQL更新成功")
        return True

    except Exception as e:
        logger.error(f"更新记录失败: {str(e)}")
        return False

def insert_record_to_hive(record_data):
    """向Hive中插入新记录 - 使用Spark SQL方式"""
    try:
        logger.info("开始插入新记录到Hive")

        # 1. 获取下一个ID
        try:
            max_id_df = spark_processor.spark.sql("SELECT COALESCE(MAX(a_id), 0) as max_id FROM test.apple_quality2")
            max_id_result = max_id_df.collect()[0]['max_id']
            new_id = int(max_id_result) + 1
            logger.info(f"获取到最大ID: {max_id_result}, 新ID: {new_id}")
        except Exception as id_error:
            logger.error(f"获取最大ID失败: {str(id_error)}")
            new_id = 1
            logger.info(f"使用默认ID: {new_id}")

        # 2. 使用Spark SQL插入
        insert_sql = f"""
        INSERT INTO test.apple_quality2
        VALUES (
            {new_id},
            {record_data['size']},
            {record_data['weight']},
            {record_data['sweetness']},
            {record_data['crunchiness']},
            {record_data['juiciness']},
            {record_data['ripeness']},
            {record_data['acidity']},
            '{record_data['quality']}'
        )
        """

        spark_processor.spark.sql(insert_sql)
        logger.info(f"新记录 {new_id} 插入成功")
        return new_id

    except Exception as e:
        logger.error(f"插入记录失败: {str(e)}")
        return None

def delete_record_from_hive(record_id):
    """从Hive中删除记录 - 使用Spark SQL方式"""
    try:
        # 使用临时视图避免表锁定问题
        spark_processor.spark.sql(f"""
            CREATE OR REPLACE TEMPORARY VIEW temp_filtered AS
            SELECT * FROM test.apple_quality2 WHERE a_id != {record_id}
        """)

        # 重新写入表
        spark_processor.spark.sql("""
            INSERT OVERWRITE TABLE test.apple_quality2
            SELECT * FROM temp_filtered
        """)

        logger.info(f"记录 {record_id} 删除成功")
        return True

    except Exception as e:
        logger.error(f"删除记录失败: {str(e)}")
        return False

@app.route('/data')
def data_page():
    # 使用Spark从Hive获取数据
    # 获取数据条数参数
    limit_param = request.args.get('limit', '50')
    custom_limit = request.args.get('custom_limit', '')

    # 获取页码参数，默认为第一页
    page = request.args.get('page', '1')
    try:
        page = int(page)
        if page < 1:
            page = 1
    except ValueError:
        page = 1

    # 每页显示的记录数
    per_page = 10

    # 处理"不限制"选项
    if limit_param == 'all':
        limit = None  # 不限制总条数
    elif limit_param == 'custom' and custom_limit:
        # 处理自定义数据条数
        try:
            limit = int(custom_limit)
            if limit < 1:  # 确保自定义条数至少为1
                limit = 1
        except ValueError:
            limit = 50  # 如果自定义值无效，使用默认值
    else:
        try:
            # 尝试将参数转换为整数
            limit = int(limit_param)
        except ValueError:
            # 如果转换失败，使用默认值50
            limit = 50

    # 获取原始数据（不经过预处理）
    try:
        df = get_raw_hive_data(limit=limit)
    except Exception as e:
        logger.error(f"获取原始Hive数据失败: {str(e)}")
        flash(f'获取原始数据失败: {str(e)}', 'danger')
        return render_template('data.html', realtime_mode=False)

    if df is not None:
        try:
            # 数据统计信息
            stats = df.describe().to_html(
                classes='table table-striped table-hover',
                justify='center'  # 设置居中对齐
            )
        except Exception as e:
            logger.error(f"生成数据统计信息失败: {str(e)}")
            stats = f'<div class="alert alert-warning">统计信息生成失败: {str(e)}</div>'

        # 给统计信息表格添加表头样式
        stats = stats.replace('<thead>', '<thead class="table-primary">')

        # 添加样式使表头水平居中对齐
        stats = stats.replace('<th>', '<th style="text-align: center;">')

        # 添加样式使数据单元格水平居中对齐
        stats = stats.replace('<td>', '<td style="text-align: center;">')

        try:
            # 缺失值信息
            missing_values = df.isnull().sum().to_frame().reset_index()
            missing_values.columns = ['特征', '缺失值数量']
            missing_values = missing_values.to_html(
                classes='table table-striped table-hover',
                index=False,
                table_id='missing-values-table',
                justify='center'  # 设置居中对齐
            )
        except Exception as e:
            logger.error(f"生成缺失值信息失败: {str(e)}")
            missing_values = f'<div class="alert alert-warning">缺失值信息生成失败: {str(e)}</div>'

        # 给缺失值表格添加表头样式
        missing_values = missing_values.replace('<thead>', '<thead class="table-primary">')
        missing_values = missing_values.replace('<th>', '<th style="text-align: center;">')

        # 添加样式使数据单元格水平居中对齐
        missing_values = missing_values.replace('<td>', '<td style="text-align: center;">')

        try:
            # 计算总记录数和总页数
            total_records = len(df)
            total_pages = (total_records + per_page - 1) // per_page  # 向上取整

            # 确保页码在有效范围内
            if page > total_pages and total_pages > 0:
                page = total_pages

            # 计算当前页的数据起止索引
            start_idx = (page - 1) * per_page
            end_idx = min(start_idx + per_page, total_records)

            # 获取当前页的数据
            current_page_df = df.iloc[start_idx:end_idx]

            # 准备模板变量
            columns = df.columns.tolist()
            data = current_page_df.values.tolist()

            logger.info(f"数据页面显示原始数据: 总共{total_records}条记录，当前第{page}页，显示{len(data)}条")
        except Exception as e:
            logger.error(f"数据分页处理失败: {str(e)}")
            # 使用默认值
            total_records = 0
            total_pages = 1
            columns = []
            data = []

        return render_template(
            'data.html',
            stats=stats,
            sample=True,  # 用于检查是否有数据
            columns=columns,
            data=data,
            missing_values=missing_values,
            current_page=page,
            total_pages=total_pages,
            total_records=total_records,
            realtime_mode=False
        )
    else:
        flash('无法获取数据，请检查Spark和Hive连接', 'danger')
        return render_template('data.html', realtime_mode=False)

@app.route('/visualization')
def visualization_page():
    """可视化页面 - 使用Spark进行数据处理"""
    if not SPARK_AVAILABLE or spark_processor is None:
        flash('Spark服务不可用，请检查Spark配置', 'danger')

        # 获取AI模型列表
        try:
            ai_models = ai.get_enabled_models()
        except:
            ai_models = []

        return render_template('visualization.html',
                             correlation_data={'labels': [], 'values': [], 'colors': []},
                             radar_data={'features': [], 'high_quality': [], 'low_quality': []},
                             correlation_heatmap=None,
                             data_stats={'total_records': 0, 'good_quality': 0, 'bad_quality': 0, 'features_count': 0},
                             quality_chart=None,
                             feature_figs={},
                             ai_models=ai_models)

    try:
        logger.info("使用Spark进行数据可视化处理")

        # 使用Spark获取数据
        spark_df = spark_processor.get_hive_data1()
        if spark_df is None:
            raise Exception("无法从Hive获取数据")

        # 使用Spark预处理数据
        processed_spark_df = spark_processor.preprocess_data(spark_df)
        if processed_spark_df is None:
            raise Exception("数据预处理失败")

        # 使用Spark计算相关性数据
        correlation_data = spark_processor.calculate_correlation_data(processed_spark_df)
        # 确保相关性数据可序列化
        if correlation_data:
            correlation_data = {
                'labels': [str(label) for label in correlation_data.get('labels', [])],
                'values': [float(val) if val is not None else 0.0 for val in correlation_data.get('values', [])],
                'colors': [str(color) for color in correlation_data.get('colors', [])]
            }
        else:
            correlation_data = {'labels': [], 'values': [], 'colors': []}
        logger.info(f"相关性数据计算完成: {len(correlation_data['labels'])} 个特征")
        logger.info(f"相关性数据内容: {correlation_data}")

        # 使用Spark计算雷达图数据
        radar_data = spark_processor.calculate_radar_data(processed_spark_df)
        logger.info(f"原始雷达图数据: {radar_data}")

        # 确保雷达图数据可序列化
        if radar_data and radar_data.get('features'):
            try:
                radar_data = {
                    'features': [str(feature) for feature in radar_data.get('features', [])],
                    'high_quality': [float(val) if val is not None else 0.0 for val in radar_data.get('high_quality', [])],
                    'low_quality': [float(val) if val is not None else 0.0 for val in radar_data.get('low_quality', [])]
                }
                logger.info(f"雷达图数据序列化成功: {radar_data}")
            except Exception as e:
                logger.error(f"雷达图数据序列化失败: {str(e)}")
                radar_data = {'features': [], 'high_quality': [], 'low_quality': []}
        else:
            logger.warning("雷达图数据为空或无效")
            radar_data = {'features': [], 'high_quality': [], 'low_quality': []}

        logger.info(f"雷达图数据计算完成: {len(radar_data['features'])} 个特征")
        logger.info(f"最终雷达图数据内容: {radar_data}")

        # 验证数据完整性
        if len(radar_data['features']) > 0:
            if len(radar_data['features']) != len(radar_data['high_quality']) or len(radar_data['features']) != len(radar_data['low_quality']):
                logger.error(f"雷达图数据长度不一致: features={len(radar_data['features'])}, high={len(radar_data['high_quality'])}, low={len(radar_data['low_quality'])}")
            else:
                logger.info("雷达图数据完整性验证通过")

        # 转换为Pandas以生成热图（保持兼容性）
        processed_df = spark_processor.to_pandas(processed_spark_df)
        if processed_df is not None:
            # 生成相关性热图
            correlation_heatmap = generate_correlation_heatmap(processed_df)
            logger.info("相关性热图生成完成")
        else:
            correlation_heatmap = None
            logger.warning("Spark DataFrame转换为Pandas失败，跳过热图生成")

        # 添加数据统计信息（确保所有值都是可序列化的）
        try:
            total_records = processed_spark_df.count()
            good_quality = processed_spark_df.filter(processed_spark_df.quality == 1).count()
            bad_quality = processed_spark_df.filter(processed_spark_df.quality == 0).count()
            features_count = len(correlation_data.get('labels', []))

            data_stats = {
                'total_records': int(total_records) if total_records is not None else 0,
                'good_quality': int(good_quality) if good_quality is not None else 0,
                'bad_quality': int(bad_quality) if bad_quality is not None else 0,
                'features_count': int(features_count) if features_count is not None else 0
            }
        except Exception as e:
            logger.warning(f"计算数据统计信息失败: {str(e)}")
            data_stats = {
                'total_records': 0,
                'good_quality': 0,
                'bad_quality': 0,
                'features_count': 0
            }

        # 生成其他图表
        quality_chart = None
        feature_figs = {}

        if processed_df is not None:
            try:
                # 生成品质分布图 - ECharts格式
                quality_counts = processed_df['quality'].value_counts()
                good_count = quality_counts.get(1, 0)
                bad_count = quality_counts.get(0, 0)

                # ECharts饼图配置 - 优化大小和布局
                quality_chart = {
                    'title': {
                        'text': '苹果品质分布',
                        'left': 'center',
                        'top': '3%',
                        'textStyle': {'fontSize': 20, 'fontWeight': 'bold'}
                    },
                    'tooltip': {
                        'trigger': 'item',
                        'formatter': '{b}: {c} 个 ({d}%)',
                        'textStyle': {'fontSize': 14}
                    },
                    'legend': {
                        'orient': 'horizontal',
                        'bottom': '5%',
                        'left': 'center',
                        'itemGap': 30,
                        'textStyle': {'fontSize': 14}
                    },
                    'series': [{
                        'name': '品质分布',
                        'type': 'pie',
                        'radius': ['15%', '55%'],  # 缩小饼图避免遮挡
                        'center': ['50%', '50%'],  # 居中显示
                        'avoidLabelOverlap': False,
                        'label': {
                            'show': True,
                            'position': 'inside',  # 显示在饼图内部
                            'formatter': '{d}%',   # 只显示百分比
                            'fontSize': 16,
                            'fontWeight': 'bold',
                            'color': '#fff'        # 白色文字，确保可见性
                        },
                        'labelLine': {
                            'show': False          # 不显示引导线
                        },
                        'data': [
                            {'value': int(good_count), 'name': '优质', 'itemStyle': {'color': '#63b2ee'}},
                            {'value': int(bad_count), 'name': '劣质', 'itemStyle': {'color': '#f89588'}}
                        ],
                        'emphasis': {
                            'itemStyle': {
                                'shadowBlur': 15,
                                'shadowOffsetX': 0,
                                'shadowColor': 'rgba(0, 0, 0, 0.3)'
                            },
                            'scale': True,
                            'scaleSize': 5
                        },
                        'animationType': 'scale',
                        'animationEasing': 'elasticOut',
                        'animationDelay': 200
                    }]
                }
                logger.info(f"品质分布图生成成功: 优质{good_count}, 劣质{bad_count}")

                # 生成特征分布图 - Plotly格式
                import plotly.graph_objects as go
                import plotly.utils

                feature_names_chinese = {
                    'sweetness': '甜度',
                    'crunchiness': '脆度',
                    'juiciness': '多汁程度',
                    'ripeness': '成熟度',
                    'acidity': '酸度',
                    'size': '大小',
                    'weight': '质量'
                }

                for feature in processed_df.columns:
                    if feature != 'quality' and pd.api.types.is_numeric_dtype(processed_df[feature]):
                        try:
                            fig = go.Figure()
                            colors = {'优质': '#63b2ee', '劣质': '#f89588'}

                            for quality_val, quality_name in [(1, '优质'), (0, '劣质')]:
                                feature_data = processed_df[processed_df['quality'] == quality_val][feature].dropna()
                                if not feature_data.empty:
                                    fig.add_trace(go.Histogram(
                                        x=feature_data.tolist(),
                                        name=quality_name,
                                        marker_color=colors[quality_name],
                                        opacity=0.7,
                                        histnorm='probability density'
                                    ))

                            feature_chinese = feature_names_chinese.get(feature, feature)
                            fig.update_layout(
                                title=f'{feature_chinese} 分布',
                                xaxis_title=feature_chinese,
                                yaxis_title='概率密度',
                                barmode='overlay',
                                height=350,
                                margin=dict(l=50, r=80, t=50, b=50),  # 增加右边距给图例留空间
                                legend=dict(
                                    x=1.02,  # 图例放在图表右侧外部
                                    y=1,
                                    xanchor='left',
                                    yanchor='top',
                                    bgcolor='rgba(255,255,255,0.8)',
                                    bordercolor='rgba(0,0,0,0.2)',
                                    borderwidth=1
                                )
                            )

                            feature_figs[feature] = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
                            logger.info(f"{feature}分布图生成成功")
                        except Exception as e:
                            logger.error(f"生成{feature}分布图失败: {str(e)}")

            except Exception as e:
                logger.error(f"生成图表失败: {str(e)}")

        # 获取AI模型列表
        try:
            ai_models = ai.get_enabled_models()
        except:
            ai_models = []

        return render_template('visualization.html',
                             correlation_data=correlation_data,
                             radar_data=radar_data,
                             correlation_heatmap=correlation_heatmap,
                             data_stats=data_stats,
                             quality_chart=quality_chart,
                             feature_figs=feature_figs,
                             ai_models=ai_models,
                             use_spark=True)

    except Exception as e:
        logger.error(f"Spark数据可视化处理失败: {str(e)}")
        flash(f'数据可视化处理失败: {str(e)}', 'danger')

        # 获取AI模型列表
        try:
            ai_models = ai.get_enabled_models()
        except:
            ai_models = []

        return render_template('visualization.html',
                             correlation_data={'labels': [], 'values': [], 'colors': []},
                             radar_data={'features': [], 'high_quality': [], 'low_quality': []},
                             correlation_heatmap=None,
                             data_stats={'total_records': 0, 'good_quality': 0, 'bad_quality': 0, 'features_count': 0},
                             quality_chart=None,
                             feature_figs={},
                             ai_models=ai_models,
                             use_spark=True)

@app.route('/modeling', methods=['GET', 'POST'])
def modeling_page():
    if request.method == 'POST':
        try:
            # 获取用户设置的训练参数
            test_size = float(request.form.get('test_size', 0.3))
            random_state = int(request.form.get('random_state', 42))
            
            # 验证参数在有效范围内
            if test_size < 0.1 or test_size > 0.5:
                flash('测试集比例必须在0.1到0.5之间', 'warning')
                test_size = 0.3  # 使用默认值
                
            if random_state < 0 or random_state > 100:
                flash('随机种子必须在0到100之间', 'warning')
                random_state = 42  # 使用默认值
            
            # 获取数据
            df = get_hive_data1()

            if df is not None:
                # 数据预处理
                processed_df = preprocess_data(df)
                
                if processed_df is not None:
                    # 对每个特征处理异常值
                    for column in processed_df.columns:
                        processed_df = remove_outliers(processed_df, column)
                    
                    # 分割特征和目标变量
                    X = processed_df.drop('quality', axis=1)
                    y = processed_df['quality']
                    
                    # 打印类别分布
                    class_counts = pd.Series(y).value_counts().to_dict()
                    print(f"处理后的数据集类别分布: {class_counts}")
                    
                    # 确保数据集中有两个类别的样本
                    if 0 not in class_counts or 1 not in class_counts:
                        flash('数据集中缺少一个或多个类别的样本，无法训练模型。', 'danger')
                        return redirect(url_for('modeling_page'))
                    
                    # 检查类别是否严重不平衡
                    minority_class = min(class_counts.get(0, 0), class_counts.get(1, 0))
                    if minority_class < 10:  # 少数类别少于10个样本
                        flash(f'数据集严重不平衡，少数类别只有{minority_class}个样本，可能影响模型性能。', 'warning')
                    
                    # 使用balance_classes函数平衡数据集
                    balanced_df = balance_classes(processed_df, target_column='quality', min_ratio=0.4)
                    
                    # 重新分割特征和目标变量
                    X = balanced_df.drop('quality', axis=1)
                    y = balanced_df['quality']
                    
                    # 使用用户指定的参数划分训练集和测试集
                    X_train, X_test, y_train, y_test = train_test_split(
                        X, y, test_size=test_size, random_state=random_state, stratify=y
                    )
                    
                    # 标准化特征
                    scaler = StandardScaler()
                    X_train = scaler.fit_transform(X_train)
                    X_test = scaler.transform(X_test)
                    
                    # 保存标准化模型
                    joblib.dump(scaler, os.path.join(MODEL_DIR, 'scaler.pkl'))
                    
                    # 训练并保存模型
                    results = train_and_save_models(X_train, X_test, y_train, y_test, random_state=random_state)
                    
                    # 模型比较图表
                    model_comparison_chart = generate_model_comparison_chart(results)
                    
                    # 保存结果到session
                    session['modeling_results'] = results
                    session['model_comparison_chart'] = model_comparison_chart
                    # 保存用户使用的参数到session，以便在页面重载时显示
                    session['test_size'] = test_size
                    session['random_state'] = random_state
                    
                    flash(f'模型训练成功! 测试集比例: {test_size}, 随机种子: {random_state}', 'success')
                    return redirect(url_for('modeling_page'))
                else:
                    flash('数据预处理失败', 'danger')
            else:
                flash('获取数据失败，请检查Hive连接', 'danger')
                
        except Exception as e:
            flash(f'模型训练过程中出错: {str(e)}', 'danger')
    
    # 从session中获取结果
    results = session.get('modeling_results', {})
    model_comparison_chart = session.get('model_comparison_chart', None)
    
    # 如果有结果，按照准确率从高到低排序
    if results:
        # 创建有序字典，按准确率从高到低排序模型
        from collections import OrderedDict
        sorted_models = sorted(results.keys(), key=lambda x: results[x]['accuracy'], reverse=True)
        sorted_results = OrderedDict()
        for model in sorted_models:
            sorted_results[model] = results[model]
        results = sorted_results
    
    # 获取上次使用的参数，如果没有则使用默认值
    last_test_size = session.get('test_size', 0.3)
    last_random_state = session.get('random_state', 42)
    
    return render_template(
        'modeling.html',
        results=results,
        model_comparison_chart=model_comparison_chart,
        test_size=last_test_size,
        random_state=last_random_state
    )

@app.route('/prediction', methods=['GET', 'POST'])
def prediction_page():
    if request.method == 'POST':
        try:
            # 获取输入特征
            features = [
                float(request.form.get('size', 0)),
                float(request.form.get('weight', 0)),
                float(request.form.get('sweetness', 0)),
                float(request.form.get('crunchiness', 0)),
                float(request.form.get('juiciness', 0)),
                float(request.form.get('ripeness', 0)),
                float(request.form.get('acidity', 0))
            ]
            
            # 获取选择的模型
            model_name = request.form.get('model', 'random_forest')
            
            # 保存选择的模型到session
            session['selected_model'] = model_name
            
            # 加载标准化模型
            scaler_path = os.path.join(MODEL_DIR, 'scaler.pkl')
            if os.path.exists(scaler_path):
                scaler = joblib.load(scaler_path)
                
                # 标准化特征
                features_scaled = scaler.transform([features])[0]
                
                # 使用选择的模型进行预测
                prediction = predict_with_model(model_name, features_scaled)
                
                if prediction is not None:
                    # 获取预测结果
                    result = '优质' if prediction == 1 else '劣质'
                    
                    # 保存预测结果到session
                    session['prediction_result'] = result
                    
                    flash(f'预测结果: 此苹果的品质为 {result}', 'success')
                else:
                    flash('模型预测失败', 'danger')
            else:
                flash('标准化模型不存在，请先训练模型', 'warning')
                
        except Exception as e:
            flash(f'预测过程中出错: {str(e)}', 'danger')
    
    # 获取已有模型列表
    available_models = []
    for filename in os.listdir(MODEL_DIR):
        if filename.endswith('_model.pkl'):
            model_name = filename.replace('_model.pkl', '')
            available_models.append(model_name)
    
    model_names_chinese = {
        'naive_bayes': '高斯朴素贝叶斯',
        'neural_network': 'BP神经网络',
        'logistic_regression': '逻辑回归模型',
        'random_forest': '随机森林',
        'knn': 'KNN',
        'bagging': '装袋模型'
    }
    
    model_choices = [(name, model_names_chinese.get(name, name)) for name in available_models]
    
    # 获取当前选择的模型（如果存在）
    selected_model = session.get('selected_model', None)
    
    # 如果没有选择过模型且有可用模型，默认选择第一个
    if selected_model is None and available_models:
        selected_model = available_models[0]
        session['selected_model'] = selected_model
    
    return render_template(
        'prediction.html',
        model_choices=model_choices,
        prediction_result=session.get('prediction_result', None),
        selected_model=selected_model
    )

# 添加雷达图数据测试路由
@app.route('/test_radar')
def test_radar_data():
    try:
        # 获取数据
        df = get_hive_data()
        if df is None:
            return jsonify({'error': '无法获取Hive数据'})

        # 预处理数据
        processed_df = preprocess_data(df)
        if processed_df is None:
            return jsonify({'error': '数据预处理失败'})

        # 初始化Spark会话
        if not spark_processor.init_spark():
            return jsonify({'error': '无法初始化Spark会话'})

        # 转换为Spark DataFrame
        spark_df = spark_processor.spark.createDataFrame(processed_df)

        # 计算雷达图数据
        radar_data = spark_processor.calculate_radar_data(spark_df)

        return jsonify({
            'success': True,
            'radar_data': radar_data,
            'data_info': {
                'total_records': len(processed_df),
                'features_count': len(radar_data.get('features', [])),
                'has_high_quality': len(radar_data.get('high_quality', [])) > 0,
                'has_low_quality': len(radar_data.get('low_quality', [])) > 0
            }
        })
    except Exception as e:
        import traceback
        return jsonify({
            'error': str(e),
            'traceback': traceback.format_exc()
        })

# 添加调试路由，不在导航中显示
@app.route('/debug')
def debug_page():
    try:
        # 获取数据
        df = get_hive_data()
        
        debug_info = {
            'get_hive_data': {
                'success': df is not None,
                'data_shape': str(df.shape) if df is not None else 'None',
                'columns': list(df.columns) if df is not None else []
            }
        }
        
        if df is not None:
            # 数据预处理
            processed_df = preprocess_data(df)
            
            debug_info['preprocess_data'] = {
                'success': processed_df is not None,
                'data_shape': str(processed_df.shape) if processed_df is not None else 'None',
                'columns': list(processed_df.columns) if processed_df is not None else [],
                'quality_values': str(processed_df['quality'].value_counts().to_dict()) if processed_df is not None and 'quality' in processed_df.columns else 'None',
                'data_sample': processed_df.head(5).to_dict() if processed_df is not None else {}
            }
            
            if processed_df is not None:
                # 检查数值列
                numeric_info = {}
                for col in processed_df.columns:
                    numeric_info[col] = {
                        'dtype': str(processed_df[col].dtype),
                        'is_numeric': pd.api.types.is_numeric_dtype(processed_df[col]),
                        'has_null': bool(processed_df[col].isnull().any()),
                        'unique_count': len(processed_df[col].unique()),
                        'min': float(processed_df[col].min()) if pd.api.types.is_numeric_dtype(processed_df[col]) else 'N/A',
                        'max': float(processed_df[col].max()) if pd.api.types.is_numeric_dtype(processed_df[col]) else 'N/A'
                    }
                
                debug_info['column_stats'] = numeric_info
                
                # 判断是否可以生成图表
                debug_info['can_generate_charts'] = {
                    'has_quality_column': 'quality' in processed_df.columns,
                    'has_numeric_features': any([pd.api.types.is_numeric_dtype(processed_df[col]) for col in processed_df.columns if col != 'quality']),
                    'correlation_possible': len(processed_df.select_dtypes(include=['number']).columns) >= 2
                }
        
        return jsonify(debug_info)
        
    except Exception as e:
        return jsonify({
            'error': str(e),
            'traceback': traceback.format_exc()
        })

# 添加AI设置页面路由
@app.route('/ai_settings')
def ai_settings_page():
    # 获取所有AI模型配置
    models = ai.get_all_models()

    # 添加调试信息
    logger.info(f"AI设置页面: 获取到 {len(models)} 个模型")
    for i, model in enumerate(models):
        logger.info(f"模型 {i+1}: ID={model.get('id', 'N/A')}, 名称={model.get('name', 'N/A')}, 模型={model.get('model', 'N/A')}")

    return render_template('ai_settings.html', models=models)

# AI模型API - 获取单个模型
@app.route('/api/ai_models/<model_id>', methods=['GET'])
def get_ai_model(model_id):
    try:
        # 获取模型详情
        model = ai.get_model_by_id(model_id)
        if model:
            return jsonify({"success": True, "model": model})
        else:
            return jsonify({"success": False, "message": "找不到指定的模型"})
    except Exception as e:
        # 记录错误
        logger.error(f"获取模型详情时出错: {str(e)}")
        # 返回错误响应
        return jsonify({"success": False, "message": f"获取模型详情时出错: {str(e)}"})

# AI模型API - 添加模型
@app.route('/api/ai_models/add', methods=['POST'])
def add_ai_model():
    try:
        model_data = request.json
        if not model_data:
            return jsonify({
                'success': False,
                'message': '未提供模型数据'
            })

        # 添加模型
        new_model = ai.add_model(model_data)

        return jsonify({
            'success': True,
            'message': '模型添加成功',
            'model': new_model
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'添加模型时出错: {str(e)}'
        })

# AI模型API - 更新模型
@app.route('/api/ai_models/update', methods=['POST'])
def update_ai_model():
    try:
        model_data = request.json
        if not model_data:
            return jsonify({
                'success': False,
                'message': '未提供模型数据'
            })

        # 更新模型
        ai.update_model(model_data)

        return jsonify({
            'success': True,
            'message': '模型更新成功'
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'更新模型时出错: {str(e)}'
        })

# AI模型API - 删除模型
@app.route('/api/ai_models/delete/<model_id>', methods=['POST'])
def delete_ai_model(model_id):
    try:
        # 删除模型
        result = ai.delete_model(model_id)
        
        if result:
            return jsonify({
                'success': True,
                'message': '模型删除成功'
            })
        else:
            return jsonify({
                'success': False,
                'message': '删除模型失败'
            })
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'删除模型时出错: {str(e)}'
        })

# AI模型API - 测试模型连接
@app.route('/api/ai_models/test/<model_id>', methods=['GET'])
def test_ai_model(model_id):
    try:
        # 设置线程超时时间（25秒）
        import threading
        import time
        
        # 存储结果的容器
        result_container = {
            "success": False,
            "message": "测试连接超时，请检查API地址和网络连接",
            "error_details": {
                "type": "超时错误",
                "error": "请求在25秒内未完成"
            }
        }
        
        # 定义测试函数
        def run_test():
            try:
                test_result = ai.test_model_connection(model_id)
                # 将结果保存到容器中
                for key, value in test_result.items():
                    result_container[key] = value
            except Exception as e:
                logger.error(f"测试模型连接时出错: {str(e)}")
                result_container["message"] = f"测试模型连接时出现系统错误: {str(e)}"
                result_container["error_details"] = {
                    "type": "系统错误",
                    "error": str(e)
                }
        
        # 创建并启动线程
        test_thread = threading.Thread(target=run_test)
        test_thread.daemon = True  # 设置为守护线程，不阻止程序退出
        test_thread.start()
        
        # 等待线程完成，最多等待25秒
        test_thread.join(25)
        
        # 如果线程仍在运行，则返回超时
        if test_thread.is_alive():
            logger.warning(f"测试模型连接超时，模型ID: {model_id}")
        
        return jsonify(result_container)
    except Exception as e:
        logger.error(f"测试模型连接API错误: {str(e)}")
        return jsonify({
            "success": False,
            "message": f"测试模型连接时出现系统错误: {str(e)}",
            "error_details": {
                "type": "系统错误",
                "error": str(e)
            }
        })

# AI分析报告生成路由 - 流式输出版本
@app.route('/generate_ai_report_stream', methods=['POST'])
def generate_ai_report_stream():
    try:
        # 获取前端发送的可视化数据
        data = request.json
        visualization_data = data.get('visualization_data', {})

        if not visualization_data:
            return jsonify({
                'success': False,
                'error': '无可视化数据提供'
            })

        # 获取用户指定的AI模型ID（如果有）
        model_id = data.get('model_id', None)

        # 生成报告的提示信息
        prompt = ai.generate_apple_analysis_prompt(visualization_data)

        def generate():
            try:
                # 发送开始信号
                yield f"data: {json.dumps({'type': 'start', 'message': '开始生成AI分析报告...'})}\n\n"

                # 调用AI模型API生成流式报告
                for chunk in ai.generate_analysis_report_stream(prompt, model_id):
                    if chunk:
                        yield f"data: {json.dumps({'type': 'content', 'content': chunk})}\n\n"

                # 发送完成信号
                yield f"data: {json.dumps({'type': 'end', 'message': '报告生成完成'})}\n\n"

            except Exception as e:
                # 发送错误信号
                yield f"data: {json.dumps({'type': 'error', 'error': str(e)})}\n\n"

        return Response(generate(), mimetype='text/event-stream')

    except Exception as e:
        print(f"生成AI报告时出错: {str(e)}")
        return jsonify({
            'success': False,
            'error': f'处理请求时出错: {str(e)}'
        })



@app.route('/api/environment')
def get_environment():
    """返回当前环境信息给前端"""
    return jsonify({
        'environment': app.config['ENV']
    })

@app.route('/api/ai_models/list')
def get_ai_models_list():
    try:
        # 获取所有配置的模型列表
        models = ai.get_all_models()
        return jsonify({
            "success": True,
            "models": models
        })
    except Exception as e:
        logger.error(f"获取AI模型列表API错误: {str(e)}")
        return jsonify({
            "success": False,
            "message": f"获取模型列表失败: {str(e)}"
        })

@app.route('/api/ai_models/available')
def get_available_ai_models():
    try:
        # 获取可用模型列表
        result = ai.get_available_models()
        return jsonify(result)
    except Exception as e:
        logger.error(f"获取可用AI模型API错误: {str(e)}")
        return jsonify({
            "success": False,
            "message": f"获取模型列表失败: {str(e)}"
        })

# Spark测试API
@app.route('/api/spark/test', methods=['GET'])
def test_spark_api():
    """测试Spark连接和功能"""
    if not SPARK_AVAILABLE or spark_processor is None:
        return jsonify({
            'success': False,
            'message': 'Spark服务不可用',
            'error': 'Spark数据处理器未初始化'
        })

    try:
        # 测试获取少量数据
        spark_df = spark_processor.get_hive_data(limit=5)
        if spark_df is None:
            return jsonify({
                'success': False,
                'message': 'Spark获取Hive数据失败'
            })

        # 转换为Pandas
        pandas_df = spark_processor.to_pandas(spark_df)
        if pandas_df is None:
            return jsonify({
                'success': False,
                'message': 'Spark DataFrame转换失败'
            })

        # 获取数据摘要
        summary = spark_processor.get_data_summary(spark_df)

        return jsonify({
            'success': True,
            'message': 'Spark连接测试成功',
            'data': {
                'spark_version': spark_processor.spark.version if spark_processor.spark else 'Unknown',
                'mode': spark_processor.mode,
                'database': spark_processor.database,
                'table': spark_processor.table,
                'sample_data_shape': list(pandas_df.shape),
                'columns': list(pandas_df.columns),
                'sample_records': pandas_df.head(3).to_dict('records'),
                'summary': summary
            }
        })

    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Spark测试失败: {str(e)}'
        })

@app.route('/api/spark/status', methods=['GET'])
def get_spark_status():
    """获取Spark服务状态"""
    return jsonify({
        'spark_available': SPARK_AVAILABLE,
        'processor_initialized': spark_processor is not None,
        'mode': spark_processor.mode if spark_processor else None,
        'database': spark_processor.database if spark_processor else None,
        'table': spark_processor.table if spark_processor else None
    })



# 重复的路由已删除

if __name__ == '__main__':
    # logger.info("苹果品质分析系统启动中...")

    # 检查Spark可用性
    # if SPARK_AVAILABLE and spark_processor:
    #     logger.info("✓ Spark数据处理器已就绪")
    #     logger.info("系统将使用Spark从Hive获取数据")
    # else:
    #     logger.error("✗ Spark数据处理器不可用")
    #     logger.error("请确保Spark集群正在运行并且PySpark已安装")

    # logger.info("苹果品质分析系统启动完成")
    # logger.info("访问 http://localhost:5000/data 浏览数据")
    # logger.info("访问 http://localhost:5000/visualization 查看可视化")
    # logger.info("访问 http://localhost:5000/api/spark/test 测试Spark连接")

    app.run(debug=True)