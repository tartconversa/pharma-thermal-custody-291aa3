# 药品冷链交接中的温控经历仓

`sourcepack` 是合成业务资料及通用资料检查器；`coldchain` 是在此之上的业务入仓、
事实建模与集市查询能力。字段、单位和业务记录的含义见 docs/资料口径.md，完整性目录为
data/catalog.json。不得把文件自检成功解释成业务结论成立。仓内只陈述温控经历与证据
缺口，**不判定疗效，也不替代药品放行审批**。实际入仓、补正与查询方式见
docs/使用说明.md。

## 环境

Python 3.11.2；仓库依赖已在 requirements.txt 固定版本。

    python3 -m pip install -r requirements.txt
    python3 -m sourcepack

检查入口读取资料目录，核对文件摘要、字段类型、记录键、引用与行数，不进行估计、推荐、批准或结算。新增业务资料可使用相同字段契约，目录摘要只对应当前保存的输入文件。
资料摘要按 CRLF 登记，检查器比对前对行尾归一化，以兼容不同平台的 LF 检出，仍能发现实质字节篡改。

## 冷链数据仓（coldchain）

- 原始文件内容寻址归档并保留接收/修订历史；每次接收生成不可变版本，支持钉版时间旅行。
- 事实：交接四态与数量争议、包装归属、责任区段（仅已确认接管）、温控原子区间
  （正常/超限/冲突/未知，未知区分没交接、没装载、没读数、无规则）。
- 集市：批次质量（盒·秒）、承运方责任、跨午夜日集市、当前可用量；质量与责任来自同一份区间事实。
- 补正接管时间联动重建受影响支流的区间、可用量与双方集市；与拆分矛盾时待核而非偷移。
- 重复文件去重、中断重跑幂等、拆分支流数量守恒且不继承未知经历。

快速开始：

    python3 -I run_tests.py
    python3 -I coldchain/cli.py --db warehouse/cc.duckdb init
    python3 -I coldchain/cli.py --db warehouse/cc.duckdb ingest --pack packs/base --received-at 2026-10-17T18:00:00+08:00
    python3 -I coldchain/cli.py --db warehouse/cc.duckdb query gaps --flow F1A

## 已有检查与语法构建

    python3 -I run_tests.py
    python3 -m compileall -q sourcepack coldchain tests

`run_tests.py` 在隔离模式（-I 语义）下运行全部测试，避免资料目录中同名模块被导入。
运行资料检查不需要访问外部业务系统。
