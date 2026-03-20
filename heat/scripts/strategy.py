"""
热度轮转策略核心类

基于东财人气排名（popularity_rank）的均值回归策略：
- 热度跌至近期低谷时买入（heat_position 接近 1）
- 热度回升至近期高位时卖出（heat_position 接近 0）
- 全仓操作，资金无空窗期轮转
- 支持单仓位（n_positions=1）和多仓位（n_positions=2~3）

参考来源：task/162-热度策略回测/03_heat_rotation_no_timeout.py

性能优化：
- price_lookup / idx_lookup 通过 prepare_lookups() 预构建，
  优化器多次调用时只需构建一次，大幅提升批量回测速度
"""
import logging
import numpy as np
import pandas as pd
from datetime import date

logger = logging.getLogger(__name__)

# 初始资金（模块级常量，用于绩效计算）
INITIAL_CAPITAL = 1_000_000


class HeatRotationStrategy:
    """
    热度轮转策略类

    使用方式（单次运行）：
        strategy = HeatRotationStrategy()
        result = strategy.run(data_bundle, params)

    使用方式（批量优化，性能更好）：
        lookups = strategy.prepare_lookups(data_bundle)
        hp_data = strategy._compute_heat_position(heat_df, trading_days, lookback)
        result = strategy.run_with_precomputed(hp_data, params, lookups)
    """

    # 默认参数配置
    DEFAULT_PARAMS = {
        'lookback': 20,           # 热度位置回看窗口（天），越大越稳定，越小越灵敏
        'buy_threshold': 0.80,    # 买入阈值：heat_position >= 此值时才是候选股
        'sell_threshold': 0.20,   # 卖出阈值：heat_position <= 此值时触发卖出
        'max_hold_days': 9999,    # 最大持仓天数（9999 = 无超时，仅靠热度信号卖出）
        'min_deal_amount': 5e7,   # 最低日成交额（元），过滤流动性差的小票
        'n_positions': 1,         # 同时持仓数量（1=单仓，2~3=多仓均分）
        'sort_by': 'rank_surge',  # 选股排序字段：'rank_surge' 或 'heat_position'
    }

    def prepare_lookups(self, data_bundle: dict) -> dict:
        """
        准备查询字典（性能关键路径）

        优先复用 data_bundle 中已预建的 price_lookup/idx_lookup（由 load_data_bundle() 提供），
        若不存在则回退到本地构建。

        Args:
            data_bundle: load_data_bundle() 返回的数据包

        Returns:
            lookups dict，包含:
                'price_lookup':  (stock_code, date) → {qfq_close, close}
                'idx_lookup':    date → index_close
                'td_list':       已排序的交易日列表
                'td_map':        date → 交易日索引
                'price_df':      原始 price_df（用于 orders_df 构建）
        """
        price_df = data_bundle['price_df']
        trading_days = data_bundle['trading_days']

        # 优先复用 data_bundle 中预构建的查询表（load_data_bundle 已预构建）
        if 'price_lookup' in data_bundle and 'idx_lookup' in data_bundle:
            price_lookup = data_bundle['price_lookup']
            idx_lookup = data_bundle['idx_lookup']
            logger.debug("复用预构建价格查询表")
        else:
            logger.info("构建价格查询表（首次）...")
            index_df = data_bundle['index_df']
            arr = price_df[['stock_code', 'date', 'qfq_close', 'close']].to_numpy()
            price_lookup = {
                (str(arr[i, 0]), arr[i, 1]): {
                    'qfq_close': float(arr[i, 2]),
                    'close': float(arr[i, 3]),
                }
                for i in range(len(arr))
            }
            idx_lookup = index_df.set_index('date')['index_close'].to_dict()

        td_list = sorted(trading_days)
        td_map = {d: i for i, d in enumerate(td_list)}

        return {
            'price_lookup': price_lookup,
            'idx_lookup': idx_lookup,
            'td_list': td_list,
            'td_map': td_map,
            'price_df': price_df,
        }

    def run(self, data_bundle: dict, params: dict = None) -> dict:
        """
        运行回测（单次，完整流程）

        Args:
            data_bundle: load_data_bundle() 返回的数据包
            params:      策略参数，不传则使用 DEFAULT_PARAMS

        Returns:
            dict，包含 trades/equity_curve/metrics/orders_df
        """
        p = dict(self.DEFAULT_PARAMS)
        if params:
            p.update(params)

        heat_df = data_bundle['heat_df']
        trading_days = data_bundle['trading_days']

        lookups = self.prepare_lookups(data_bundle)
        hp_data = self._compute_heat_position(heat_df, trading_days, p['lookback'])
        return self.run_with_precomputed(hp_data, p, lookups)

    def run_with_precomputed(self, hp_data: tuple, params: dict, lookups: dict) -> dict:
        """
        使用预计算数据运行回测（优化器专用，避免重复构建 price_lookup）

        Args:
            hp_data:  (heat_position, rank_pivot, deal_pivot, rank_surge) 元组
            params:   策略参数
            lookups:  prepare_lookups() 返回的查询字典

        Returns:
            dict，包含 trades/equity_curve/metrics/orders_df
        """
        p = dict(self.DEFAULT_PARAMS)
        if params:
            p.update(params)

        heat_position, rank_pivot, deal_pivot, rank_surge = hp_data
        buy_threshold = p['buy_threshold']
        sell_threshold = p['sell_threshold']
        max_hold_days = p['max_hold_days']
        min_deal_amount = p['min_deal_amount']
        n_positions = p['n_positions']
        sort_by = p['sort_by']

        if n_positions <= 1:
            trade_df, equity_df, final_capital = self._simulate_single(
                heat_position, rank_pivot, deal_pivot, rank_surge,
                lookups, buy_threshold, sell_threshold, max_hold_days,
                min_deal_amount, sort_by
            )
        else:
            trade_df, equity_df, final_capital = self._simulate_multi(
                heat_position, rank_pivot, deal_pivot, rank_surge,
                lookups, buy_threshold, sell_threshold, max_hold_days,
                min_deal_amount, n_positions, sort_by
            )

        metrics = self.compute_metrics(trade_df, equity_df)
        orders_df = self._build_orders_df(trade_df, lookups['price_df'])

        return {
            'trades': trade_df,
            'equity_curve': equity_df,
            'metrics': metrics,
            'orders_df': orders_df,
        }

    def _compute_heat_position(self, heat_df: pd.DataFrame,
                                trading_days: list, lookback: int) -> tuple:
        """
        计算热度相对位置矩阵（向量化）

        heat_position = (当前rank - N日最低rank) / (N日最高rank - N日最低rank)
        - 值接近 1.0 → rank 处于近期最高 → 热度处于近期最低 → 买入信号
        - 值接近 0.0 → rank 处于近期最低 → 热度处于近期最高 → 卖出信号

        rank_surge = 当前rank / 过去N日平均rank
        - 值越大 → 该股相对自身常态"冷"了越多 → 候选排序优先

        Args:
            heat_df:      热度排名 DataFrame
            trading_days: 交易日列表
            lookback:     回看窗口（天）

        Returns:
            (heat_position, rank_pivot, deal_pivot, rank_surge) 四个 DataFrame
        """
        logger.info(f"计算热度位置矩阵 (lookback={lookback})...")

        td_set = set(trading_days)
        heat_td = heat_df[heat_df['date'].isin(td_set)].copy()

        # 透视表：行=日期，列=股票代码，值=rank
        rank_pivot = heat_td.pivot_table(index='date', columns='stock_code', values='rank')
        rank_pivot = rank_pivot.sort_index()

        # 成交额透视表（用于流动性过滤）
        deal_pivot = heat_td.pivot_table(index='date', columns='stock_code', values='deal_amount')
        deal_pivot = deal_pivot.sort_index()

        # 滚动最高/最低 rank（min_periods=lookback//2 保证预热期后有值）
        rolling_max = rank_pivot.rolling(window=lookback, min_periods=lookback // 2).max()
        rolling_min = rank_pivot.rolling(window=lookback, min_periods=lookback // 2).min()

        # heat_position 计算
        range_val = rolling_max - rolling_min
        range_val = range_val.replace(0, np.nan)
        heat_position = (rank_pivot - rolling_min) / range_val

        # rank_surge：越大表示该股偏离常态越远
        rolling_mean = rank_pivot.rolling(window=lookback, min_periods=lookback // 2).mean()
        rolling_mean = rolling_mean.replace(0, np.nan)
        rank_surge = rank_pivot / rolling_mean

        logger.info(f"  矩阵大小: {heat_position.shape[0]} 天 × {heat_position.shape[1]} 只股票")
        return heat_position, rank_pivot, deal_pivot, rank_surge

    def _simulate_single(self, heat_position, rank_pivot, deal_pivot, rank_surge,
                          lookups: dict,
                          buy_threshold, sell_threshold, max_hold_days,
                          min_deal_amount, sort_by='rank_surge') -> tuple:
        """
        单仓位轮转回测引擎

        逐日模拟（信号日=T，执行日=T+1）：
        1. 记录当日净值
        2. 检查持仓卖出条件（热度回升或超时）
        3. 无持仓时，全市场选最冷门股票买入
        4. 买卖均在次日收盘执行

        Args:
            lookups: prepare_lookups() 预构建的查询字典（性能关键）

        Returns:
            (trade_df, equity_df, final_capital) 元组
        """
        price_lookup = lookups['price_lookup']
        idx_lookup = lookups['idx_lookup']
        td_list = lookups['td_list']
        td_map = lookups['td_map']

        trades = []
        equity_records = []
        capital = INITIAL_CAPITAL

        holding = False
        hold_code = None
        hold_entry_date = None
        hold_entry_qfq = None
        hold_entry_actual = None
        hold_start_idx = None
        position_capital = None

        last_equity = INITIAL_CAPITAL
        no_candidate_days = 0

        for i, today in enumerate(td_list):
            # 1. 记录当日净值
            if holding:
                p = price_lookup.get((hold_code, today))
                if p:
                    current_equity = position_capital * (p['qfq_close'] / hold_entry_qfq)
                    last_equity = current_equity
                else:
                    current_equity = last_equity  # 停牌，沿用上日
            else:
                current_equity = capital

            equity_records.append({
                'date': today,
                'equity': current_equity,
                'index_close': idx_lookup.get(today, np.nan),
                'holding': hold_code if holding else None,
            })

            # 预热期：跳过无 heat_position 的日期
            if today not in heat_position.index:
                continue

            # 期末处理
            if i + 1 >= len(td_list):
                if holding:
                    p = price_lookup.get((hold_code, today))
                    if p:
                        ret = (p['qfq_close'] - hold_entry_qfq) / hold_entry_qfq
                        capital = position_capital * (1 + ret)
                        trades.append({
                            'stock_code': hold_code,
                            'entry_date': hold_entry_date,
                            'exit_date': today,
                            'entry_price': hold_entry_actual,
                            'exit_price': p['close'],
                            'return': ret,
                            'hold_days': i - hold_start_idx,
                            'exit_reason': 'end_of_period',
                        })
                        holding = False
                break

            next_td = td_list[i + 1]

            # 2. 卖出判断
            should_sell = False
            sell_reason = None

            if holding:
                hp_today = np.nan
                if hold_code in heat_position.columns:
                    hp_today = heat_position.loc[today, hold_code]

                hold_days = i - hold_start_idx

                if not np.isnan(hp_today) and hp_today <= sell_threshold:
                    should_sell = True
                    sell_reason = 'heat_recovered'
                elif hold_days >= max_hold_days:
                    should_sell = True
                    sell_reason = 'max_hold'

            # 3. 执行卖出（次日收盘）
            if should_sell:
                exit_price = price_lookup.get((hold_code, next_td))
                if exit_price:
                    ret = (exit_price['qfq_close'] - hold_entry_qfq) / hold_entry_qfq
                    capital = position_capital * (1 + ret)
                    trades.append({
                        'stock_code': hold_code,
                        'entry_date': hold_entry_date,
                        'exit_date': next_td,
                        'entry_price': hold_entry_actual,
                        'exit_price': exit_price['close'],
                        'return': ret,
                        'hold_days': td_map[next_td] - hold_start_idx,
                        'exit_reason': sell_reason,
                    })
                    holding = False
                    hold_code = None
                # 次日停牌则保持持仓，下一日重试

            # 4. 买入判断（无持仓时）
            if not holding:
                hp_series = heat_position.loc[today].dropna()
                candidates = hp_series[hp_series >= buy_threshold]

                if len(candidates) == 0:
                    no_candidate_days += 1
                    continue

                # 流动性过滤
                if min_deal_amount > 0 and today in deal_pivot.index:
                    deal_today = deal_pivot.loc[today].reindex(candidates.index)
                    candidates = candidates[deal_today.fillna(0) >= min_deal_amount]

                if len(candidates) == 0:
                    no_candidate_days += 1
                    continue

                # 次日必须有价格（未停牌）
                valid_codes = [c for c in candidates.index if (c, next_td) in price_lookup]
                if not valid_codes:
                    no_candidate_days += 1
                    continue
                candidates = candidates.loc[valid_codes]

                # 选股排序
                best_code = self._select_best(candidates, rank_surge, today, sort_by)

                # 执行买入（次日收盘）
                buy_price = price_lookup[(best_code, next_td)]
                holding = True
                hold_code = best_code
                hold_entry_date = next_td
                hold_entry_qfq = buy_price['qfq_close']
                hold_entry_actual = buy_price['close']
                hold_start_idx = td_map[next_td]
                position_capital = capital

        trade_df = pd.DataFrame(trades) if trades else pd.DataFrame()
        equity_df = pd.DataFrame(equity_records)

        return trade_df, equity_df, capital

    def _simulate_multi(self, heat_position, rank_pivot, deal_pivot, rank_surge,
                         lookups: dict,
                         buy_threshold, sell_threshold, max_hold_days,
                         min_deal_amount, n_positions, sort_by='rank_surge') -> tuple:
        """
        多仓位轮转回测引擎（n_positions >= 2）

        规则：
        - 维护最多 n_positions 个持仓槽位，资金均分
        - 有空仓时买入，按排序从高到低填满
        - 任一仓位 heat_position <= sell_threshold 触发卖出该仓位
        - 卖出当日同时产生新买入信号（次日执行）

        Args:
            lookups: prepare_lookups() 预构建的查询字典（性能关键）

        Returns:
            (trade_df, equity_df, final_capital) 元组
        """
        price_lookup = lookups['price_lookup']
        idx_lookup = lookups['idx_lookup']
        td_list = lookups['td_list']
        td_map = lookups['td_map']

        # 持仓列表，每个 slot dict:
        # {code, entry_date, entry_qfq, entry_actual, start_idx, capital}
        slots = []
        free_capital = INITIAL_CAPITAL
        trades = []
        equity_records = []
        no_candidate_days = 0

        for i, today in enumerate(td_list):
            # 1. 当日总净值
            total_equity = free_capital
            for slot in slots:
                p = price_lookup.get((slot['code'], today))
                if p:
                    total_equity += slot['capital'] * (p['qfq_close'] / slot['entry_qfq'])
                else:
                    total_equity += slot['capital']

            equity_records.append({
                'date': today,
                'equity': total_equity,
                'index_close': idx_lookup.get(today, np.nan),
                'holding': ','.join([s['code'] for s in slots]) if slots else None,
            })

            if today not in heat_position.index:
                continue

            # 期末强制平仓
            if i + 1 >= len(td_list):
                for slot in list(slots):
                    p = price_lookup.get((slot['code'], today))
                    if p:
                        ret = (p['qfq_close'] - slot['entry_qfq']) / slot['entry_qfq']
                        free_capital += slot['capital'] * (1 + ret)
                        trades.append({
                            'stock_code': slot['code'],
                            'entry_date': slot['entry_date'],
                            'exit_date': today,
                            'entry_price': slot['entry_actual'],
                            'exit_price': p['close'],
                            'return': ret,
                            'hold_days': i - slot['start_idx'],
                            'exit_reason': 'end_of_period',
                        })
                slots = []
                break

            next_td = td_list[i + 1]

            # 2. 检查各持仓是否触发卖出
            to_sell = []
            for slot in list(slots):
                hp_today = np.nan
                if slot['code'] in heat_position.columns:
                    hp_today = heat_position.loc[today, slot['code']]

                hold_days = i - slot['start_idx']

                if (not np.isnan(hp_today) and hp_today <= sell_threshold) or \
                   hold_days >= max_hold_days:
                    to_sell.append(slot)

            # 3. 执行卖出
            for slot in to_sell:
                exit_price = price_lookup.get((slot['code'], next_td))
                if exit_price:
                    ret = (exit_price['qfq_close'] - slot['entry_qfq']) / slot['entry_qfq']
                    recovered = slot['capital'] * (1 + ret)
                    free_capital += recovered

                    hold_days = i - slot['start_idx']
                    reason = 'max_hold' if hold_days >= max_hold_days else 'heat_recovered'

                    trades.append({
                        'stock_code': slot['code'],
                        'entry_date': slot['entry_date'],
                        'exit_date': next_td,
                        'entry_price': slot['entry_actual'],
                        'exit_price': exit_price['close'],
                        'return': ret,
                        'hold_days': td_map[next_td] - slot['start_idx'],
                        'exit_reason': reason,
                    })
                    slots.remove(slot)

            # 4. 填充空仓
            empty_slots = n_positions - len(slots)
            if empty_slots > 0 and free_capital > 0:
                hp_series = heat_position.loc[today].dropna()
                held_codes = {s['code'] for s in slots}
                candidates = hp_series[
                    (hp_series >= buy_threshold) &
                    (~hp_series.index.isin(held_codes))
                ]

                if min_deal_amount > 0 and today in deal_pivot.index:
                    deal_today = deal_pivot.loc[today].reindex(candidates.index)
                    candidates = candidates[deal_today.fillna(0) >= min_deal_amount]

                valid_codes = [c for c in candidates.index if (c, next_td) in price_lookup]
                if valid_codes:
                    candidates = candidates.loc[valid_codes]

                    if sort_by == 'rank_surge' and today in rank_surge.index:
                        surge = rank_surge.loc[today].reindex(candidates.index).dropna()
                        sorted_codes = surge.sort_values(ascending=False).index.tolist()
                    else:
                        sorted_codes = candidates.sort_values(ascending=False).index.tolist()

                    per_slot_capital = free_capital / empty_slots

                    for code in sorted_codes[:empty_slots]:
                        buy_price = price_lookup[(code, next_td)]
                        slots.append({
                            'code': code,
                            'entry_date': next_td,
                            'entry_qfq': buy_price['qfq_close'],
                            'entry_actual': buy_price['close'],
                            'start_idx': td_map[next_td],
                            'capital': per_slot_capital,
                        })
                        free_capital -= per_slot_capital
                else:
                    no_candidate_days += 1

        trade_df = pd.DataFrame(trades) if trades else pd.DataFrame()
        equity_df = pd.DataFrame(equity_records)

        return trade_df, equity_df, free_capital

    def _select_best(self, candidates: pd.Series, rank_surge: pd.DataFrame,
                      today, sort_by: str) -> str:
        """
        从候选股中选出最优股票

        Args:
            candidates: 满足条件的候选股 Series（index=stock_code，value=heat_position）
            rank_surge: rank_surge 矩阵
            today:      当日日期
            sort_by:    'rank_surge'（推荐）或 'heat_position'

        Returns:
            选中的 stock_code
        """
        if sort_by == 'rank_surge' and today in rank_surge.index:
            surge_today = rank_surge.loc[today].reindex(candidates.index).dropna()
            if len(surge_today) > 0:
                return surge_today.idxmax()
        return candidates.idxmax()

    def _build_orders_df(self, trade_df: pd.DataFrame,
                          price_df: pd.DataFrame) -> pd.DataFrame:
        """
        将交易明细转换为 OrderBasedEngine 所需的 orders.csv 格式

        格式：date (YYYYMMDD), code (ts_code), action (buy/sell),
              price (前复权价), amount (买入金额，卖出时为空)

        股票代码映射：6位数字 → ts_code
            600/601/603/605/688/689 开头 → .SH
            其他 → .SZ

        Args:
            trade_df: 交易明细 DataFrame
            price_df: 行情数据（含 ts_code）

        Returns:
            orders DataFrame
        """
        if len(trade_df) == 0:
            return pd.DataFrame(columns=['date', 'code', 'action', 'price', 'amount'])

        # 构建 stock_code → ts_code 映射
        code_map = {}
        if len(price_df) > 0 and 'ts_code' in price_df.columns:
            for row in price_df[['stock_code', 'ts_code']].drop_duplicates().itertuples(index=False):
                code_map[row.stock_code] = row.ts_code

        def to_ts_code(code6):
            if code6 in code_map:
                return code_map[code6]
            if code6.startswith(('600', '601', '603', '605', '688', '689')):
                return f"{code6}.SH"
            return f"{code6}.SZ"

        orders = []
        for trade in trade_df.itertuples(index=False):
            code6 = trade.stock_code
            ts_code = to_ts_code(code6)

            entry_date_str = trade.entry_date.strftime('%Y%m%d') \
                if hasattr(trade.entry_date, 'strftime') else str(trade.entry_date).replace('-', '')
            orders.append({
                'date': entry_date_str,
                'code': ts_code,
                'action': 'buy',
                'price': getattr(trade, 'entry_price', ''),
                'amount': INITIAL_CAPITAL,
            })

            exit_date_str = trade.exit_date.strftime('%Y%m%d') \
                if hasattr(trade.exit_date, 'strftime') else str(trade.exit_date).replace('-', '')
            orders.append({
                'date': exit_date_str,
                'code': ts_code,
                'action': 'sell',
                'price': getattr(trade, 'exit_price', ''),
                'amount': '',
            })

        return pd.DataFrame(orders)

    def compute_metrics(self, trade_df: pd.DataFrame,
                         equity_df: pd.DataFrame) -> dict:
        """
        计算策略绩效指标

        Args:
            trade_df:  交易明细 DataFrame
            equity_df: 日净值曲线 DataFrame

        Returns:
            绩效指标 dict（total_return, annual_return, max_drawdown, sharpe, win_rate 等）
        """
        if len(trade_df) == 0 or len(equity_df) == 0:
            return {}

        eq = equity_df.copy()
        eq = eq[eq['equity'] > 0].copy()

        if len(eq) == 0:
            return {}

        # 策略净值
        eq['nav'] = eq['equity'] / INITIAL_CAPITAL
        eq['daily_return'] = eq['nav'].pct_change().fillna(0)

        # 沪深300净值
        first_row = eq[eq['index_close'].notna()]
        if len(first_row) == 0:
            return {}
        first_idx = first_row.iloc[0]['index_close']
        eq['index_nav'] = eq['index_close'] / first_idx
        eq['index_daily_return'] = eq['index_nav'].pct_change().fillna(0)

        n_days = len(eq)

        # 策略绩效
        total_return = (eq['nav'].iloc[-1] - 1) * 100
        annual_return = ((eq['nav'].iloc[-1]) ** (250 / n_days) - 1) * 100

        peak = eq['nav'].expanding().max()
        drawdown = (eq['nav'] - peak) / peak
        max_drawdown = drawdown.min() * 100

        # 夏普比率（年化，无风险利率 2%）
        rf_daily = 0.02 / 250
        daily_excess = eq['daily_return'] - rf_daily
        sharpe = (daily_excess.mean() / daily_excess.std() * np.sqrt(250)) \
            if daily_excess.std() > 0 else 0

        # 基准绩效
        idx_nav = eq['index_nav'].dropna()
        index_return = (idx_nav.iloc[-1] - 1) * 100 if len(idx_nav) > 0 else 0
        index_annual = ((idx_nav.iloc[-1]) ** (250 / n_days) - 1) * 100 if len(idx_nav) > 0 else 0

        idx_peak = idx_nav.expanding().max()
        idx_dd = (idx_nav - idx_peak) / idx_peak
        idx_max_dd = idx_dd.min() * 100 if len(idx_dd) > 0 else 0

        # 交易统计
        total_trades = len(trade_df)
        wins = len(trade_df[trade_df['return'] > 0]) if 'return' in trade_df.columns else 0
        losses = len(trade_df[trade_df['return'] < 0]) if 'return' in trade_df.columns else 0
        win_rate = wins / total_trades * 100 if total_trades > 0 else 0
        avg_return = trade_df['return'].mean() * 100 if 'return' in trade_df.columns else 0
        avg_hold = trade_df['hold_days'].mean() if 'hold_days' in trade_df.columns else 0
        avg_win = trade_df[trade_df['return'] > 0]['return'].mean() * 100 if wins > 0 else 0
        avg_loss = trade_df[trade_df['return'] < 0]['return'].mean() * 100 if losses > 0 else 0
        profit_factor = abs(avg_win / avg_loss) if avg_loss != 0 else float('inf')

        return {
            'total_return': total_return,
            'annual_return': annual_return,
            'max_drawdown': max_drawdown,
            'sharpe': sharpe,
            'index_return': index_return,
            'index_annual': index_annual,
            'index_max_dd': idx_max_dd,
            'excess_total': total_return - index_return,
            'excess_annual': annual_return - index_annual,
            'total_trades': total_trades,
            'wins': wins,
            'losses': losses,
            'win_rate': win_rate,
            'avg_return': avg_return,
            'avg_hold': avg_hold,
            'avg_win': avg_win,
            'avg_loss': avg_loss,
            'profit_factor': profit_factor,
        }
