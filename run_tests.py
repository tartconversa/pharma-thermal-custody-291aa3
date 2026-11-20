#!/usr/bin/env python3
"""隔离模式测试入口：python3 -I run_tests.py

-I 会忽略 PYTHONPATH 与当前目录，这里在导入业务包前显式加入仓库根，
保证解释器不会从资料目录加载被植入的同名模块。
"""
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

if __name__ == "__main__":
    loader = unittest.TestLoader()
    suite = loader.discover(str(ROOT / "tests"), pattern="test_*.py")
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)
