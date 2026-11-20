"""药品冷链温控经历仓。

模块划分：
- schema.py：DuckDB 表结构（原始留存、版本、事实、集市、复核引用）
- engine.py：纯口径引擎，不接触数据库，可独立单测
- warehouse.py：入仓、补正、版本化重建与查询 API
- cli.py：命令行入口
"""

__all__ = ["Warehouse", "engine", "schema"]
__version__ = "1.0.0"
