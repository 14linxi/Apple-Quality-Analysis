
# 苹果品质分析系统

## 项目概述
苹果品质分析系统是一个基于机器学习的应用程序，旨在分析和预测苹果的品质。系统通过分析苹果的多种特征（如大小、重量、甜度、脆度、多汁程度、成熟度和酸度），训练模型来预测苹果是"优质"还是"劣质"。该系统集成了数据采集、数据分析、可视化、模型训练和预测功能，为苹果品质评估提供了全面的解决方案。

## 系统架构

整个系统采用前后端不分离的架构，使用Flask作为Web框架，提供以下核心功能模块：

```
苹果品质分析系统
│
├── 数据获取模块 - 从Hive数据库或本地文件获取原始数据
│
├── 数据处理模块 - 数据清洗、预处理和特征工程
│
├── 数据可视化模块 - 特征分布和相关性分析的可视化展示
│
├── 模型训练模块 - 训练多种机器学习算法并评估性能
│
└── 预测模块 - 使用训练好的模型进行苹果品质预测
```

## 目录结构

```
项目根目录
│
├── app.py - 主应用程序，包含所有后端逻辑和路由
│
├── templates/ - HTML模板文件
│   ├── base.html - 基础模板
│   ├── index.html - 首页
│   ├── data.html - 数据展示页面
│   ├── visualization.html - 数据可视化页面
│   ├── modeling.html - 模型训练页面
│   └── prediction.html - 品质预测页面
│
├── static/ - 静态资源文件(CSS、JavaScript、图片等)
│
├── models/ - 保存训练好的模型文件
│
└── requirements.txt - 项目依赖列表
```

## 功能模块

### 1. 首页
- 系统概览和功能导航
- 展示系统的主要功能和使用流程

### 2. 数据展示
- 从Hive数据库获取原始数据
- 提供数据预览和基本统计信息
- 支持分页查看和限制数据条数

### 3. 数据可视化
- 特征分布可视化：展示不同特征的分布情况
- 品质分布可视化：展示优质和劣质苹果的比例
- 特征相关性热图：分析各特征间的相关关系

### 4. 模型训练
- 支持训练多种机器学习模型:
  - 高斯朴素贝叶斯
  - BP神经网络
  - 逻辑回归
  - 随机森林
  - K最近邻(KNN)
  - 装袋模型(Bagging)
- 提供模型性能评估和比较
- 可配置训练参数（测试集比例、随机种子）

### 5. 品质预测
- 使用训练好的模型进行苹果品质预测
- 支持输入苹果的各项特征参数
- 提供预测结果展示

## 框架流程（系统流程）

系统的整体工作流程如下：

1. **数据获取阶段**
   - 从Hive数据库获取苹果品质数据
   - 如果连接失败，提供错误提示

2. **数据展示阶段**
   - 展示原始数据和基本统计信息
   - 提供数据缺失值分析
   - 支持分页浏览数据

3. **数据可视化阶段**
   - 对数据进行预处理
   - 生成特征分布图
   - 生成特征相关性热图
   - 生成品质分布饼图

4. **模型训练阶段**
   - 用户设置训练参数（测试集比例、随机种子）
   - 对数据进行预处理和异常值处理
   - 检查和处理类别不平衡问题
   - 划分训练集和测试集
   - 训练多种机器学习模型
   - 评估模型性能并保存模型
   - 生成模型比较图表

5. **预测阶段**
   - 用户输入苹果特征参数
   - 选择使用的模型
   - 加载模型进行预测
   - 展示预测结果

## 数据处理流程

数据处理流程是系统的核心部分，包含以下关键步骤和对应的函数：

### 1. 数据获取
- **函数**：`get_hive_data(query, limit)`
- **操作**：
  - 使用PyHive连接Hive数据库
  - 执行SQL查询获取数据
  - 将结果转换为Pandas DataFrame

### 2. 数据预处理
- **函数**：`preprocess_data(df)`
- **操作**：
  - 删除空值：`df.dropna(inplace=True)`
  - 删除重复行：`df.drop_duplicates()`
  - 标准化品质字段：将'good'映射为1，'bad'映射为0
  - 将所有特征列转换为浮点型：`column.astype(float)`
  - 删除与分析无关的列（如'a_id'）

### 3. 异常值处理
- **函数**：`remove_outliers(df, column)`
- **操作**：
  - 使用四分位数(IQR)方法检测异常值
  - 计算Q1(25%)、Q3(75%)和IQR = Q3-Q1
  - 定义下边界：Q1 - 1.5 * IQR
  - 定义上边界：Q3 + 1.5 * IQR
  - 过滤掉超出范围的值

### 4. 类别平衡处理
- **函数**：`balance_classes(df, target_column, min_ratio)`
- **操作**：
  - 检查类别分布：`df[target_column].value_counts()`
  - 如果少数类比例低于min_ratio，进行平衡处理
  - 方法1：如需要样本较多，进行少数类上采样：`sklearn.utils.resample(minority_samples, replace=True)`
  - 方法2：如需要样本较少，进行多数类下采样：`sklearn.utils.resample(majority_samples, replace=False)`
  - 合并处理后的样本：`pd.concat([majority_samples, minority_samples])`
  - 随机打乱数据：`balanced_df.sample(frac=1).reset_index(drop=True)`

### 5. 数据集划分
- **操作**：
  - 分离特征和目标变量：`X = df.drop('quality', axis=1), y = df['quality']`
  - 使用stratify参数确保训练集和测试集的类别分布一致
  - 使用`train_test_split(X, y, test_size, random_state, stratify=y)`划分数据集

### 6. 特征标准化
- **操作**：
  - 创建StandardScaler对象
  - 对训练集拟合并转换：`scaler.fit_transform(X_train)`
  - 对测试集只进行转换：`scaler.transform(X_test)`
  - 保存标准化器模型供预测使用：`joblib.dump(scaler, path)`

### 7. 模型训练和评估
- **函数**：`train_and_save_models(X_train, X_test, y_train, y_test, random_state)`
- **操作**：
  - 定义多种模型并设置参数
  - 使用训练数据拟合模型：`model.fit(X_train, y_train)`
  - 使用测试数据进行预测：`model.predict(X_test)`
  - 计算准确率：`accuracy_score(y_test, y_pred)`
  - 生成分类报告：`classification_report(y_test, y_pred, output_dict=True)`
  - 将模型保存到模型目录：`joblib.dump(model, model_path)`

### 8. 模型性能可视化
- **函数**：`generate_model_comparison_chart(results)`
- **操作**：
  - 按照准确率从高到低排序模型：`models.sort(key=lambda x: results[x]['accuracy'], reverse=True)`
  - 使用Plotly创建条形图
  - 将精确度转换为百分比并保留两位小数
  - 返回JSON格式的图表数据

### 9. 预测
- **函数**：`predict_with_model(model_name, features)`
- **操作**：
  - 加载保存的模型：`joblib.load(model_path)`
  - 使用模型进行预测：`model.predict([features])[0]`
  - 返回预测结果

## 技术栈

- **后端框架**：Flask
- **数据处理**：Pandas, NumPy
- **机器学习**：Scikit-learn
- **数据可视化**：Matplotlib, Seaborn, Plotly
- **数据库连接**：PyHive
- **模型序列化**：Joblib
- **前端**：HTML, CSS, JavaScript, Bootstrap

## 安装与运行

1. 克隆项目到本地
2. 安装依赖：`pip install -r requirements.txt`
3. 配置Hive连接信息（在app.py中的HIVE_CONFIG字典）
4. 运行应用：`python app.py`
5. 在浏览器中访问：`http://localhost:5000`

## 注意事项

- 确保Hive数据库已配置并可访问
- 数据表应包含以下字段：size, weight, sweetness, crunchiness, juiciness, ripeness, acidity, quality
- 系统会自动创建models目录用于保存训练模型
- 首次使用时需要先训练模型才能进行预测 