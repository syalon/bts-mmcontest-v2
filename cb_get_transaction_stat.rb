#!/usr/bin/env ruby

require 'time'
require 'json'
# require 'bigdecimal'

require_relative 'cb_config'

def get_transaction_stat__24h(filled_orders_dir)
  $avg_price_24h = {}  # 24h {主流币,bitAssets}/BTS 交易对成交均价
  $trading_pairs  = {}  # 交易对成交量,  未排除小成交额交易 !!!

  $all_assets.keys.each do |asset_id_1|
    ## 初始化24小时均价
    $avg_price_24h[asset_id_1] = {
        asset_name: $all_assets[asset_id_1][:name],
        asset_amount: 0r,  # should be divided by 10 ** precision
        bts_amount: 0r,    # should be divided by 10 ** precision
        real_price: -1r
    }

    ## 初始化24小时成交量
    asset_idx_1  = $all_assets[asset_id_1][:idx]
    asset_type_1 = $all_assets[asset_id_1][:type]
    asset_name_1 = $all_assets[asset_id_1][:name]
    $all_assets.keys.each do |asset_id_2|
      asset_idx_2  = $all_assets[asset_id_2][:idx]
      asset_type_2 = $all_assets[asset_id_2][:type]
      asset_name_2 = $all_assets[asset_id_2][:name]

      # 排除同类型资产
      next if asset_type_1 == asset_type_2 or $trading_pairs[asset_idx_1 | asset_idx_2]

      # 初始化交易对统计信息
      $trading_pairs[asset_idx_1 | asset_idx_2] = {
          "#{asset_id_1}" => 0r, # 交易量
          "#{asset_id_2}" => 0r, # 交易量
          :asset_ids      => [asset_id_1, asset_id_2],
          :asset_names    => [asset_name_1, asset_name_2],
          :trading_type   => get_trading_type(asset_id_1, asset_id_2),
          :fees           => {}, # 已除精度
          :fees_as_bts    => {}, # 已除精度
          :fees_sum_as_bts=> 0r, # 已除精度
          :reward         => 0r, # 已除精度
          :sells          => [],
          :buys           => []  # pair: small oid / big oid. sell small one, buy big one
      }
    end
  end
  $avg_price_24h['1.3.0'][:real_price] = 1r

  Dir.foreach(filled_orders_dir) do |file|
    next if file == '.' or file == '..'
    next unless file =~ /\.filled$/i
    
    IO.readlines( File.join(filled_orders_dir, file) ).each do |str|
      # {
      #  "id":"2.18.5178",
      #  "fee":{"amount":364,"asset_id":"1.3.113"},
      #  "order_id":"1.7.224763961","account_id":"1.2.414074",
      #  "pays":{"amount":1003602,"asset_id":"1.3.2635"},
      #  "receives":{"amount":364408,"asset_id":"1.3.113"},
      #  "fill_price":{"base":{"amount":72620000,"asset_id":"1.3.113"},
      #  "quote":{"amount":200000000,"asset_id":"1.3.2635"}},
      #  "is_maker":false
      # }
      transaction = JSON.parse(str)

      pays_asset_id = transaction['pays']['asset_id']
      pays_asset_amount = transaction['pays']['amount'].to_i
      receives_asset_id = transaction['receives']['asset_id']
      receives_asset_amount = transaction['receives']['amount'].to_i

      trading_type = get_trading_type(pays_asset_id, receives_asset_id)
      next if trading_type.nil?

      # 记录24h交易对成交量
      trading_pair = get_trading_pair(pays_asset_id, receives_asset_id)
      trading_pair[pays_asset_id] += pays_asset_amount
      trading_pair[receives_asset_id] += receives_asset_amount

      # 记录 交易对 手续费 贡献:
      #   - 买方支付购买资产的 市场手续费(若有)， 手续费以购买资产计，后续需转换成BTS
      fee = transaction['fee']
      fee_asset_id = fee['asset_id']
      trading_pair_fees = trading_pair[:fees]
      if trading_pair_fees[fee_asset_id].nil?
        trading_pair_fees[fee_asset_id] = 0r
      end
      trading_pair_fees[fee_asset_id] += fee['amount'].to_i.to_r * $all_assets[fee_asset_id][:mkt_fees_ratio]

      # 记录24h成交价数据(maker, taker 金额一致, 因此只需记录maker的数据)
      if transaction["is_maker"] && (trading_type == :bts_gateway || trading_type == :bts_bit)
        if pays_asset_id == '1.3.0'
          $avg_price_24h[receives_asset_id][:bts_amount]   += pays_asset_amount
          $avg_price_24h[receives_asset_id][:asset_amount] += receives_asset_amount
        else
          $avg_price_24h[pays_asset_id][:bts_amount]   += receives_asset_amount
          $avg_price_24h[pays_asset_id][:asset_amount] += pays_asset_amount
        end
      end

    end
  end

  # 计算24h均价
  $avg_price_24h.each do |asset_id, stat|
    if stat[:real_price] == -1
      stat[:asset_amount] = Rational(stat[:asset_amount], 10 ** $all_assets[asset_id][:precision])
      stat[:bts_amount]   = Rational(stat[:bts_amount], 10 ** 5)
      stat[:real_price]   = stat[:bts_amount] / stat[:asset_amount] # per asset
    end

    ###############
    if stat[:real_price] == -1
      asset_name = $all_assets[asset_id][:name]
      STDERR.puts "未找到<#{asset_name} / BTS>成交记录，设置资产 #{asset_id} 价格为 0 BTS。"
      # todo 未找到成交记录的资产 相互之间的交易对非法(因为无法衡量实际的BTS深度)
      stat[:real_price] = 0r
    end
    ###############
  end

  puts '=' * 45
  puts "Asset Avg(24h) Price"
  printf "%-20s%25s\n" % %w[Asset AvgPrice(BTS)]
  $avg_price_24h.each do |asset_id, stat|
    printf "%-20s%25.5f\n" % [$all_assets[asset_id][:name], stat[:real_price]]
  end
  puts '=' * 45

  # transform to **real** volume
  $trading_pairs.each do |_, trading_pair|
    asset_id_1 = trading_pair[:asset_ids][0]
    asset_id_2 = trading_pair[:asset_ids][1]
    asset_1_precision = $all_assets[asset_id_1][:precision]
    asset_2_precision = $all_assets[asset_id_2][:precision]

    # 成交量
    trading_pair[asset_id_1] = Rational(trading_pair[asset_id_1], 10 ** asset_1_precision)
    trading_pair[asset_id_2] = Rational(trading_pair[asset_id_2], 10 ** asset_2_precision)

    # 市场手续费
    trading_pair[:fees].keys.each do |fee_asset_id|
      fee_asset_precision = $all_assets[fee_asset_id][:precision]
      fee_real_amount = Rational(trading_pair[:fees][fee_asset_id], 10 ** fee_asset_precision)
      fee_asset_price = $avg_price_24h[fee_asset_id][:real_price]
      fee_as_bts      = fee_real_amount * fee_asset_price

      trading_pair[:fees][fee_asset_id] = fee_real_amount
      trading_pair[:fees_as_bts][fee_asset_id] = fee_as_bts
      trading_pair[:fees_sum_as_bts] += fee_as_bts
    end

  end

  puts "Gateway Donates Market Fees"
  printf "%-10s%20s%15s\n" % %w[Asset DonateAmount DonateRatio]
  $trading_pairs.each do |_, trading_pair|
    if trading_pair[:fees_sum_as_bts] <= 0
      next
    end
    trading_pair[:fees].each do |asset_id, fee|
      if $all_assets[asset_id][:type] == 0x2 # gateway asset
        asset_name = $all_assets[asset_id][:name]
        asset_precision = $all_assets[asset_id][:precision]
        donate_ratio = $gateway_assets[asset_id][:mkt_fees_donate_ratio]
        real_fee   = fee.to_f * donate_ratio / $all_assets[asset_id][:mkt_fees_ratio]
        printf "%-10s%20.#{asset_precision}f%15.2f\n" % [asset_name, real_fee, donate_ratio]
      end
    end
  end
  puts "=" * 45

  puts "Trading Pair Market Fees"
  printf "%-15s%25s%25s%15s%15s%15s\n" % ['TradingPair', 'Fee1'.center(25), 'Fee2'.center(25), 'Fee1AsBTS', 'Fee2AsBTS', 'FeesSumAsBTS']
  $trading_pairs.each do |_, trading_pair|
    if trading_pair[:fees_sum_as_bts] <= 0
      next
    end
    fee_asset_id_1 = trading_pair[:asset_ids][0]
    fee_asset_id_2 = trading_pair[:asset_ids][1]

    fee_asset_1 = $all_assets[fee_asset_id_1]
    fee_asset_2 = $all_assets[fee_asset_id_2]

    fee_value_1 = sprintf("%15.#{fee_asset_1[:precision]}f %-9s", trading_pair[:fees][fee_asset_id_1].to_f, fee_asset_1[:name] )
    fee_value_2 = sprintf("%15.#{fee_asset_2[:precision]}f %-9s", trading_pair[:fees][fee_asset_id_2].to_f, fee_asset_2[:name] )

    fee_value_as_bts_1 = sprintf("%15.5f", trading_pair[:fees_as_bts][fee_asset_id_1].to_f)
    fee_value_as_bts_2 = sprintf("%15.5f", trading_pair[:fees_as_bts][fee_asset_id_2].to_f)

    fee_value_sum_as_bts = sprintf("%15.5f", trading_pair[:fees_sum_as_bts].to_f)

    printf "%-15s%25s%25s%15s%15s%15s\n" %
               [trading_pair[:asset_names].join('/'), fee_value_1, fee_value_2,
                fee_value_as_bts_1, fee_value_as_bts_2, fee_value_sum_as_bts]
  end
  puts "=" * 45


  ## 计算交易对封顶奖励
  trading_group_reward_sum = {
      :bts_gateway => 0r,
      :bts_bit     => 0r,
      :gateway_bit => 0r
  }
  $trading_pairs.each do |_, trading_pair|
    trading_type = trading_pair[:trading_type]
    trading_group_config = $tp_reward_config[trading_type]
    trading_pair_config  = trading_group_config[:pair_config]

    # 交易对最小成交量(以BTS计)，低于阈值的不计算奖励
    min_trading_vol = trading_pair_config[:min_trading_vol]

    case trading_type
    when :gateway_bit
      # 网关资产 / 智能资产
      # 兑换成BTS计算，取成交量的最大值。todo: double confirm
      asset_id_1 = trading_pair[:asset_ids][0]
      asset_id_2 = trading_pair[:asset_ids][1]

      asset_1_amount_as_bts = trading_pair[asset_id_1] * $avg_price_24h[asset_id_1][:real_price]
      asset_2_amount_as_bts = trading_pair[asset_id_2] * $avg_price_24h[asset_id_2][:real_price]

      actual_trading_vol = [asset_1_amount_as_bts, asset_2_amount_as_bts].max
    else # 与BTS产生的交易对
      actual_trading_vol = trading_pair['1.3.0']
    end

    if actual_trading_vol < min_trading_vol
      # STDERR.puts "交易对#{trading_pair[:asset_names]}成交量为 #{actual_trading_vol.to_f} BTS, 低于阈值#{min_trading_vol}, 奖励为0"
      next
    end

    reward_by_fees = trading_pair[:fees_sum_as_bts] * trading_pair_config[:fee_return_ratio]
    reward_max     = trading_pair_config[:max_reward]
    reward         = [reward_by_fees, reward_max].min

    trading_pair[:reward] = reward
    trading_group_reward_sum[trading_type] += reward.to_r
  end

  trading_group_reward_sum.each do |trading_type, reward_sum|
    group_config = $tp_reward_config[trading_type][:group_config]
    group_reward = $tp_reward_config[trading_type][:group_reward]

    group_max_reward = group_config[:max_reward]
    group_base_reward = group_config[:base_reward]
    if reward_sum >= group_max_reward
      group_reward[:fill_percent] = Rational group_max_reward, reward_sum
    elsif reward_sum >= group_base_reward
      group_reward[:fill_percent] = 1r
    elsif reward_sum.positive?
      group_reward[:fill_percent] = Rational group_base_reward, reward_sum
    else
      group_reward[:fill_percent] = 0r
    end

    group_reward[:sum] = reward_sum
  end
end

if __FILE__ == $0
  date = ARGV[0] or Time.now.utc.to_s.split(' ')[0]
  base_dir = '/home/ubuntu/bts_delay_node/witness_node_data_dir/ugly-snapshots/2020/' + date
  get_transaction_stat__24h(File.join base_dir, 'filled_orders')
  p $avg_price_24h
end