#!/usr/bin/env ruby

require 'json'
require 'http'
require 'pp'

require_relative 'cb_snapshot_process'

def query_proposal(proposal_id)
  resp = HTTP.post("https://api.weaccount.cn/rpc", :body=>'{"jsonrpc": "2.0", "method": "get_objects", "params": [["' + proposal_id + '"]], "id": 1}') rescue nil
  raise 'fetch data error' if resp.nil?
  json = JSON.parse(resp.to_s) rescue nil
  raise 'parse json error' if json.nil?
  result = json['result']
  raise 'api call error' if result.nil?
  proposal = result.first
  raise "proposal does not exist: ##{proposal_id}" if proposal.nil? || !proposal.is_a?(::Hash)
  return proposal
end

def verify_onchain(transfer_list, proposal)
  # # TODO: config
  # resp = HTTP.post("https://api.weaccount.cn/rpc", :body=>'{"jsonrpc": "2.0", "method": "get_proposed_transactions", "params": ["1.2.100876"], "id": 1}') rescue nil
  # raise 'fetch data error' if resp.nil?
  # json = JSON.parse(resp.to_s) rescue nil
  # raise 'parse json error' if json.nil?
  # result = json['result']
  # raise 'api call error' if result.nil?

  # result.each do |proposal
  # ...
  # end

  # {"id"=>"1.10.59609",
  #  "expiration_time"=>"2020-07-23T23:55:00",
  #  "proposed_transaction"=>
  #   {"ref_block_num"=>0,
  #    "ref_block_prefix"=>0,
  #    "expiration"=>"2020-07-23T23:55:00",
  #    "operations"=>
  #     [[0,
  #       {"fee"=>{"amount"=>86869, "asset_id"=>"1.3.0"},
  #        "from"=>"1.2.100876",
  #        "to"=>"1.2.1158950",
  #        "amount"=>{"amount"=>88873382, "asset_id"=>"1.3.0"},
  #        "extensions"=>[]}],
  #      [0,
  #       {"fee"=>{"amount"=>86869, "asset_id"=>"1.3.0"},
  #        "from"=>"1.2.100876",
  #        "to"=>"1.2.660681",
  #        "amount"=>{"amount"=>2144483, "asset_id"=>"1.3.0"},
  #        "extensions"=>[]}],
  #      ...
  #      [0,
  #       {"fee"=>{"amount"=>86869, "asset_id"=>"1.3.0"},
  #        "from"=>"1.2.100876",
  #        "to"=>"1.2.1693506",
  #        "amount"=>{"amount"=>67845, "asset_id"=>"1.3.0"},
  #        "extensions"=>[]}]],
  #    "extensions"=>[]},
  #  "required_active_approvals"=>["1.2.100876"],
  #  "available_active_approvals"=>["1.2.413040", "1.2.422024"],
  #  "required_owner_approvals"=>[],
  #  "available_owner_approvals"=>[],
  #  "available_key_approvals"=>[],
  #  "proposer"=>"1.2.413040",
  #  "fail_reason"=>""}

  # => uid => reward value
  user_rewards_hash_local = {}
  user_rewards_hash_chain = {}

  transfer_list.each do |item|
    acc, value = *item
    user_rewards_hash_local[acc] ||= 0
    user_rewards_hash_local[acc] += value.to_i
  end

  proposal['proposed_transaction']['operations'].each do |op|
    raise 'invalid proposal, invalid operation.' if op.first.to_i != 0
    opdata = op.last
    raise 'invalid proposal, invalid asset.' if opdata['amount']['asset_id'] != '1.3.0'
    acc = opdata['to']
    value = opdata['amount']['amount'].to_i
    user_rewards_hash_chain[acc] ||= 0
    user_rewards_hash_chain[acc] += value.to_i  
  end

  # => compare
  raise 'verify failed...' if user_rewards_hash_local.size != user_rewards_hash_chain.size
  user_rewards_hash_chain.each{|acc, value| raise 'verify failed...' if user_rewards_hash_local[acc] != value}
  user_rewards_hash_local.each{|acc, value| raise 'verify failed...' if user_rewards_hash_chain[acc] != value}
  
  return true
end

if __FILE__ == $0
  date = ARGV[0]
  raise 'miss date arg.' if date.nil?

  proposal_id = ARGV[1]
  raise 'miss proposal_id arg.' if proposal_id.nil?

  # => 1. Calc local data
  base_dir = '/home/ubuntu/bts_delay_node/witness_node_data_dir/ugly-snapshots/2020/' + date

  rewards_data_path = base_dir + 'data.rewards'
  transfer_list = JSON.parse(open(rewards_data_path, 'rb'){|fr| fr.read}) rescue nil
  if transfer_list.nil?
    start_time = Time.now
    puts "Calc dir #{base_dir} ..."
    get_transaction_stat__24h(base_dir)
    transfer_list = process_snapshots(base_dir)
    open(rewards_data_path, 'wb'){|fw| fw.write transfer_list.to_json}
    puts "Start time: #{start_time}"
    puts "End time: #{Time.now}. Duration: #{ (Time.now - start_time) / 60 } mins."
  else
    puts "Use cached reward data ..."
  end

  # => 2. Verify
  puts "Verify dir #{base_dir} ..."
  proposal = query_proposal(proposal_id)
  verify_onchain(transfer_list, proposal)
  puts "Proposal data verification passed, #{date} OK OK OK !!!"
end
