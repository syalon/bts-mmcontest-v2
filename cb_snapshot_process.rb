#!/usr/bin/env ruby

# todo:
    # 如何确定强清单?
    # 主流币／bitAssets组 - 主流币、bit 资产的24小时均价计算失败（24小时内没有与BTS的成交记录）

require 'time'
require 'json'
require 'bigdecimal'

require_relative 'cb_config'
require_relative 'cb_get_transaction_stat'

PRINT_BLOCK_RESULT = false

def process_snapshots( snapshot_path )
  # puts date
  valid_trading_pairs = $trading_pairs.select { |_, trading_pair| trading_pair[:reward] > 0 }

  _trading_pair_order_books = {}
  valid_trading_pairs.each do |t_p_id, t_p|
    _trading_pair_order_books[t_p_id] = {
        buys: [], sells: [],
        buys_group: {}, sells_group: {}, # 买卖深度/档位
        asset_names: t_p[:asset_names],
        asset_ids: t_p[:asset_ids]
    }
  end

  daily_trading_group_data = {}
  valid_trading_pairs.each do |t_p_id, t_p|
    daily_trading_group_data[t_p_id] = {
        buys_group: {}, sells_group: {}, # 买卖深度 得分
        asset_names: t_p[:asset_names],
        asset_ids: t_p[:asset_ids]
    }
  end

  daily_trader_scores = {}
  valid_trading_pairs.each do |t_p_id, t_p|
    daily_trader_scores[t_p_id] = {
        buys: {}, sells: {},
        asset_names: t_p[:asset_names],
        asset_ids: t_p[:asset_ids]
    }
  end

  daily_trader_rewards = {}
  valid_trading_pairs.each do |t_p_id, t_p|
    daily_trader_rewards[t_p_id] = {
        buys: {}, sells: {},
        asset_names: t_p[:asset_names],
        asset_ids: t_p[:asset_ids]
    }
  end
  _coin_group_data = daily_trading_group_data.clone

  blocks = 0
  Dir.foreach(snapshot_path) do |file|
    next if file == '.' or file == '..'
    blocks += 1
    puts blocks if blocks % 100 == 0

    order_books = {}
    valid_trading_pairs.each do |t_p_id, t_p|
      order_books[t_p_id] = {
          buys: [], sells: [],
          buys_group: {}, sells_group: {}, # 买卖深度/档位
          asset_names: t_p[:asset_names],
          asset_ids: t_p[:asset_ids]
      }
    end

    ## 处理block中的挂单
    order_strs = IO.readlines( File.join snapshot_path, file )
    order_strs.each do |order_str|
      #{
      # "id":"1.7.890509",
      # "expiration":"2020-10-14T23:20:23",
      # "seller":"1.2.3833",
      # "for_sale":1000000000,
      # "sell_price":{
      #   "base":{"amount":1000000000,"asset_id":"1.3.1517"},
      #   "quote":{"amount":"10000000000","asset_id":"1.3.0"}},
      # "deferred_fee":100,
      # "deferred_paid_fee":{"amount":0,"asset_id":"1.3.0"}
      #}
      order = JSON.parse( order_str )
      next unless check_order(order)

      asset_base_id  = order["sell_price"]["base"]["asset_id"]  # asset for sale
      asset_quote_id = order["sell_price"]["quote"]["asset_id"] # asset to buy

      trading_pair_idx = get_trading_pair_idx(asset_base_id, asset_quote_id)
      order_book = order_books[trading_pair_idx]
      next if order_book.nil?

      ## add orders to trading pair's order book
      asset_base_amount = order["sell_price"]["base"]["amount"]
      asset_quota_amount= order["sell_price"]["quote"]["amount"]
      trader = order["seller"]
      price  = Rational( asset_base_amount, asset_quota_amount )
      # amount_for_sale = BigDecimal order['for_sale']  # asset id is sell_price.base.asset_id

      ## 约定：以小oid来作为买卖的对象
      if asset_base_id < asset_quote_id
        # e.g 1.3.0 < 1.3.113, means sells BTS for bitCNY
        order_book_side = order_book[:sells]
        price = 1 / price
      else
        # e.g 1.3.113 > 1.3.0, means buys BTS with bitCNY
        order_book_side = order_book[:buys]
      end

      o = {
          :trader => trader, :price => price,
          :asset_amount => order[:asset_amount],
          :asset_amount_as_bts => order[:asset_amount_as_bts]
      }
      order_book_side.push o
    end

    order_books.delete_if do |_, book| book[:buys].empty? or book[:sells].empty? end
    order_books.each do |_, book|
      # 对方挂单价格
      opponent_price = book[:buys][0][:price]

      ## 统计挂单档位
      [:sells, :buys].each do |side|
        j = 1
        book[side].each do |order|
          j+=1
          order[:distance] = ( order[:price] - opponent_price ).abs / [ opponent_price, order[:price]].max
          order[:group]    = distance_to_group( order[:distance] )

          upper_bound = $group_bounds[order[:group]].to_r
          lower_bound = $group_bounds[order[:group]-1].to_r

          order[:weight] = order[:asset_amount] * ( 1 + (upper_bound-order[:distance]) / (upper_bound-lower_bound) )
        end
        book[side].delete_if { |order| order[:group] > 6 }

        opponent_price = book[:sells][0][:price]
      end
    end # end order_books.each
    order_books.delete_if do |_, book| book[:buys].empty? or book[:sells].empty? end

    if PRINT_BLOCK_RESULT
      puts
      puts '=========  挂单  ============'
      puts "---------sells-------------"
      printf "%-15s%15s%15s%8s%25s\n" % %w[asset_amount price distance group weight]
      order_books[5][:sells].each do |order|
        printf "%-15s%15.10f%15.5f%8i%25.5f\n" % [order[:asset_amount], order[:price].to_f, order[:distance].to_f, order[:group], order[:weight].to_f]
      end
      puts "---------------------------"
      order_books[5][:buys].each do |order|
        printf "%-15s%15.10f%15.5f%8i%25.5f\n" % [order[:asset_amount], order[:price].to_f, order[:distance].to_f, order[:group], order[:weight].to_f]
      end
      puts "----------buys-------------"
    end

    ## 统计每档 深度 及 总权重
    order_books.each do |_, book|
      [:sells, :buys].each do |action|
        group_stat = book[:"#{[action, 'group'].join('_')}"]

        book[action].each do |order|
          order_group = order[:group]  # 档位

          unless group_stat.has_key? order_group
            group_stat[order_group] = {
                :asset_amount => 0,
                :asset_amount_as_bts_sum => 0,
                :weight_sum => 0
            }
          end

          group_stat[order_group][:asset_amount] += order[:asset_amount]
          group_stat[order_group][:asset_amount_as_bts_sum] += order[:asset_amount_as_bts]
          group_stat[order_group][:weight_sum] += order[:weight]
        end
      end
    end

    ## 计算交易对 每个档位对应的score： 成交量/深度 * 档位分成比例
    order_books.each do |t_p_idx, book|
      [:sells, :buys].each do |side|
        side_order_groups = book[:"#{[side, 'group'].join('_')}"]

        trading_type = $trading_pairs[t_p_idx][:trading_type]
        target_depth = $tp_reward_config[trading_type][:pair_config][:target_depth]
        side_daily_trading_group = daily_trading_group_data[t_p_idx][:"#{[side, 'group'].join('_')}"]
        side_order_groups.each do |group, group_stat|
          # group_stat: {asset_amount_as_bts_sum, weight_sum}
          group_stat[:score] = BigDecimal.new($group_reward_percent[group] * [1, Rational(group_stat[:asset_amount_as_bts_sum], target_depth)].min, 20)

          if not side_daily_trading_group.has_key? group
            side_daily_trading_group[group] = {:score => group_stat[:score]}
          else
            side_daily_trading_group[group][:score] += group_stat[:score]
          end
        end
      end
    end

    if PRINT_BLOCK_RESULT
      puts
      puts '=========  深度  ============'
      puts "---------sells-------------"
      printf "%-15s%8s%25.5s%15.5s\n" % %w[asset_amount group weight score]
      order_books[5][:sells_group].each do |group, group_stat|
        printf "%-15s%8i%25.5f%15.5f\n" % [group_stat[:asset_amount], group, group_stat[:weight_sum].to_f, group_stat[:score].to_f]
      end
      puts "---------------------------"
      order_books[5][:buys_group].each do |group, group_stat|
        printf "%-15s%8i%25.5f%15.5f\n" % [group_stat[:asset_amount], group, group_stat[:weight_sum].to_f, group_stat[:score].to_f]
      end
      puts "----------buys-------------"
    end

    # 交易对 组内分配（计算每个订单应该获得的收益比例）
    order_books.each do |t_p_idx, book|
      [:sells, :buys].each do |side|
        side_order_groups = book[:"#{[side, 'group'].join('_')}"]

        book[side].each do |order|
          side_daily_trader_scores = daily_trader_scores[t_p_idx][side]

          group_stat = side_order_groups[order[:group]]
          order[:score] = order[:weight] * group_stat[:score] / group_stat[:weight_sum]
          unless side_daily_trader_scores.has_key? order[:trader]
            side_daily_trader_scores[order[:trader]] = {score: BigDecimal(0), group: order[:group]}
          end

          side_daily_trader_scores[order[:trader]][:score] += order[:score]
        end
      end
    end

    if PRINT_BLOCK_RESULT
      puts
      puts '======  个人交易评分  ========='
      puts "---------sells-------------"
      printf "%-15s%15.5s\n" % %w[trader score]
      daily_trader_scores[5][:sells].each do |trader, ts|
        printf "%-15s%15.5f\n" % [trader, ts[:score].to_f]
      end
      puts "---------------------------"
      daily_trader_scores[5][:buys].each do |trader, ts|
        printf "%-15s%15.5f\n" % [trader, ts[:score].to_f]
      end
      puts "----------buys-------------"
      puts
    end
  end # end Dir.foreach

  # 计算每个 档位 的奖励，后续对个人的奖励基于此来分配
  daily_trading_group_data.each do |t_p_idx, trading_stat|
    trading_type = $trading_pairs[t_p_idx][:trading_type]
    trading_group_config = $tp_reward_config[trading_type][:group_config]
    # 存储的是**交易对组** 根据交易费贡献所得到的奖励总额，及对应的scale倍数
    group_reward = $tp_reward_config[trading_type][:group_reward]
    # 交易对 买卖盘 收益比率
    ratio_sum = trading_group_config[:buys_reward_ratio] + trading_group_config[:sells_reward_ratio]

    [:sells, :buys].each do |side|
      side_reward_ratio = trading_group_config[:"#{[side, 'reward_ratio'].join('_')}"]
      side_groups = trading_stat[:"#{[side, 'group'].join('_')}"]
      # **交易对** 的奖励*scale倍数
      trading_pair_real_reward = $trading_pairs[t_p_idx][:reward] * group_reward[:fill_percent]
      side_groups.each do |_, group|
        # 档位得分 * 交易对实际奖励 * 买卖盘占比 / block数量
        group[:reward] = (group[:score] * trading_pair_real_reward * Rational(side_reward_ratio, ratio_sum) / blocks)#.to_i
      end
    end
  end

  total_reward = 0
  daily_trader_scores.each do |t_p_idx, trader_score|
    [:sells, :buys].each do |side|
      group_data = daily_trading_group_data[t_p_idx][:"#{[side, 'group'].join('_')}"]

      trader_score[side].each do |trader, score__group|
        group_reward_sum = group_data[score__group[:group]][:reward]
        group_score_sum  = group_data[score__group[:group]][:score]

        daily_trader_rewards[t_p_idx][side][trader] = ( score__group[:score] / group_score_sum * group_reward_sum)#.to_i
        total_reward += daily_trader_rewards[t_p_idx][side][trader]
      end
    end
  end

  puts
  puts "============================================="
  puts "Rewards"
  puts "Total %0.5f BTS" % (total_reward.to_f)
  puts

  daily_trader_rewards.each do |_, rewards|
    next if rewards[:sells].empty?

    puts "============================================="
    puts "%s / %s markets" % rewards[:asset_names]
    puts "--seller-------------------------reward(BTS)-"
    rewards[:sells].sort_by { |acc, reward| -reward }.each do |acc, reward|
      next if reward < 0.00001 # skip data < 0.00001 BTS
      printf "%-30s%15.5f\n" % [acc, reward.to_f]
    end
    puts "--buyer--------------------------reward(BTS)-"
    rewards[:buys].sort_by { |acc, reward| -reward }.each do |acc, reward|
      next if reward < 0.00001 # skip data < 0.00001 BTS
      printf "%-30s%15.5f\n" % [acc, reward.to_f]
    end
  end
end

# 1. 检查挂单是否是所允许交易对
# 2. 检查交易对金额是否满足最小要求
# 3. 计算资产的实际挂单量
def check_order(order)
  asset_base_id     = order["sell_price"]["base"]["asset_id"]  # asset for sale
  asset_quota_id    = order["sell_price"]["quote"]["asset_id"] # asset to buy
  asset_base_amount = order["sell_price"]["base"]["amount"].to_i
  asset_quota_amount= order["sell_price"]["quote"]["amount"].to_i
  for_sale_amount   = Rational order["for_sale"]

  asset_base   = $all_assets[asset_base_id]
  asset_quota  = $all_assets[asset_quota_id]

  trading_type = get_trading_type(asset_base_id, asset_quota_id)
  return false unless trading_type

  trading_pair_config = $tp_reward_config[trading_type][:pair_config]
  min_order_size = trading_pair_config[:min_order_size]

  ## 判断挂单是否满足最小挂单量
  if trading_type == :gateway_bit
    # gateway / bit 交易对需要判断的是Gateway资产的挂单量
    if asset_base[:type] == 0x2     # gateway asset
      asset_to_judge = asset_base
      asset_amount = for_sale_amount
    else
      asset_to_judge = asset_quota  # bit asset, transform to gateway asset amount
      asset_amount = for_sale_amount * asset_quota_amount / asset_base_amount
    end

    min_order_size = min_order_size[asset_to_judge[:coin]] * (10 ** asset_to_judge[:precision])

    # 按照24h小时均价转换 - 已处理资产精度
    asset_amount_as_bts = (asset_amount / 10 ** asset_to_judge[:precision]) * $avg_price_24h[asset_base_id][:real_price]
  else
    # BTS 交易对, 判断BTS资产数量
    if asset_base[:type] == 0x1     # bts asset
      asset_amount = for_sale_amount
    else                            # transform to bts amount
      asset_amount = for_sale_amount * asset_quota_amount / asset_base_amount
    end
    asset_amount_as_bts = asset_amount / 10 ** 5

    min_order_size = min_order_size * (10 ** 5)
  end

  # 资产买卖的数额（未除精度）
  # todo: 考虑是否转换为int（abit在#70做了转换），转换后会损失精度，同时导致weight计算不准确。
  order[:asset_amount] = asset_amount.to_i

  # 已转为真实数量BTS
  order[:asset_amount_as_bts] = asset_amount_as_bts

  asset_amount >= min_order_size
end

if __FILE__ == $0
  get_transaction_stat__24h(File.join Dir.getwd, 'test/filled_orders/' )
  process_snapshots(File.join Dir.getwd, 'test/2020/2020-07-02')
end