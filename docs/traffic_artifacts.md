# 真实交通模型制品与本机运行

平台使用NYC TLC Green 2024年1月的6×6网格、30分钟帧和已训练B1种子42、L=3检查点。每个样本使用此前90分钟历史，预测下一30分钟；batch8是八个独立的一步窗口。

## 环境

完整原项目使用Python3.12及pyproject依赖。独立数值交通路径可以单独安装`requirements-traffic.txt`，无需配置LLM API密钥。开发验证使用既有Python3.9.6环境；完整会话应用依赖仍按原项目安装。

## 真实制品目录

设置`TRAFFIC_EXPERIMENT_ROOT`为包含以下文件的实验根目录：

```text
model/traffic_net.py（以及其原模型模块）
data/processed/dataset_metadata.json
data/processed/normalization_train_only.npz
data/processed/nyc_tlc_green_2024_01_6x6.npz
results/B1_seed42_L3/best.pt
```

结果对照还使用`results/B1_seed42_L3/test_predictions.npz`。平台严格加载真实模型，缺失制品时返回明确错误；不会生成替代预测或伪装实验结果。

已有实验目录可以原样导出至新的本地目录：

```bash
python scripts/export_traffic_artifacts.py --source /path/to/real/experiment --destination data/traffic_artifacts
```

脚本记录文件SHA256，不改变源数据、统计量或检查点。导出结果是本地运行数据，`data/`不进入Git提交。配置环境变量后运行：

```bash
TRAFFIC_EXPERIMENT_ROOT=/path/to/traffic_artifacts PYTHONPATH=. python -m uvicorn traffic_server:app --host 127.0.0.1 --port 18763
```

服务路径和完整联合编排操作流程见后续平台说明。旧`/traffic/ui`仍可用于原固定链路复核，新的平台页面展示应用、能力实例、联合计划、全局资源和运行反馈。
