#!/bin/bash
# 全市场 MACD 计算 + 信号分析（5489只 × 4周期 × 3复权 = 65868行）
#
# 预估耗时:
#   计算阶段: ~2.5小时（日线最慢，周/月/年较快）
#   分析阶段: ~1.5小时
#   合计: ~4小时
#
# 用法: bash research/research_macd/run_full_market.sh

set -e
cd "$(dirname "$0")/../.."

echo "=========================================="
echo " 全市场 MACD 计算 + 信号分析"
echo " 开始时间: $(date '+%Y-%m-%d %H:%M:%S')"
echo "=========================================="

TOTAL_START=$SECONDS

# ── Phase 1: 计算 MACD（12个组合）──
echo ""
echo "===== Phase 1: MACD 计算 (4周期 × 3复权) ====="

for freq in daily weekly monthly yearly; do
  for adj in bfq qfq hfq; do
    echo ""
    echo "--- compute: $freq/$adj | $(date '+%H:%M:%S') ---"
    python research/research_macd/compute_stock_macd.py --freq $freq --adj $adj 2>&1 | grep -E "\[INFO\].*完成"
  done
done

COMPUTE_ELAPSED=$(( SECONDS - TOTAL_START ))
echo ""
echo "===== Phase 1 完成 | 计算耗时: ${COMPUTE_ELAPSED}秒 ($(( COMPUTE_ELAPSED / 60 ))分钟) ====="

# ── Phase 2: 信号分析（全量全模式）──
ANALYZE_START=$SECONDS
echo ""
echo "===== Phase 2: 信号分析 (全市场 × 12组合) ====="
echo "--- 开始: $(date '+%H:%M:%S') ---"

python research/research_macd/analyze_stock_macd.py 2>&1 | grep -E "\[INFO\]"

ANALYZE_ELAPSED=$(( SECONDS - ANALYZE_START ))
TOTAL_ELAPSED=$(( SECONDS - TOTAL_START ))

echo ""
echo "=========================================="
echo " 全部完成"
echo " 计算阶段: ${COMPUTE_ELAPSED}秒 ($(( COMPUTE_ELAPSED / 60 ))分钟)"
echo " 分析阶段: ${ANALYZE_ELAPSED}秒 ($(( ANALYZE_ELAPSED / 60 ))分钟)"
echo " 总耗时:   ${TOTAL_ELAPSED}秒 ($(( TOTAL_ELAPSED / 60 ))分钟)"
echo " 结束时间: $(date '+%Y-%m-%d %H:%M:%S')"
echo "=========================================="
