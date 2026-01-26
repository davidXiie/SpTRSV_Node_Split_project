# 说明



## 项目程序功能说明

### 1. main_workflow.py

暂时没写，后面按需补全

### 2. generate_synthetic_matrix.py

- **输入**: 矩阵维度、稀疏度参数
- **处理**: 生成合成的L矩阵并转换为DAG格式，创建下三角依赖关系
- **输出**: DAG JSON文件和对应的可视化图像文件

### 3. experiment_runner.py

- **输入**: DAG文件路径、MEC结果路径
- **处理**: 运行MEC编译器计算节点最早完成时间，执行物理调度器模拟（默认PE=10），跟踪任务执行过程
- **输出**: MEC结果文件、详细的跟踪日志、实验摘要CSV文件

### 4. graph_splitter.py

- **输入**: DAG文件（input_data目录下的longtail_*_dag.json文件）
- **处理**: 根据父节点数量阈值（默认20）将大节点拆分为多个PARTIAL节点和一个FUSION节点
- **输出**: 拆分后的DAG文件（保存到input_data_split目录）

### 5. mec_compiler_resource.py

- **输入**: 拆分后的DAG文件（input_data_split目录下的*_split.json文件）
- **处理**: 计算资源感知型MEC（考虑NFU资源限制），模拟单PE串行累加和NFU资源竞争
- **输出**: MEC资源感知结果文件（JSON格式）和调试文本文件

### 6. scheduler_hetero.py

- **输入**: DAG文件、MEC资源感知结果
- **处理**: 实现异构调度算法，包含LC推进、边任务释放、Slack抢占、PC循环等机制，支持PE/NFU资源管理
- **输出**: 详细的调度跟踪日志、性能摘要CSV文件、操作序列文件
