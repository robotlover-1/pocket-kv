#!/bin/bash
# 在项目根目录中运行基准测试脚本的包装脚本
cd "$(dirname "$0")"
cd benchmarks/scripts
exec python3 "$@"
