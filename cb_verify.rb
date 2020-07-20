#!/usr/bin/env ruby

require 'json'
require 'http'
require 'pp'
require 'logger'

require_relative 'cb_snapshot_process'

class MyLogger

  attr_reader   :loggers

  def initialize(log_filename = nil, appname = 'mm-contest-v2')
    @loggers = []
    @loggers << Logger.new(log_filename) if log_filename
    @loggers << Logger.new(STDOUT)
    @loggers.each do |log|
      log.datetime_format = '%Y-%m-%d %H:%M:%S'
      log.progname = appname if appname
    end
  end

  def debug(msg)
    log_to_file :debug, msg
  end

  def info(msg)
    log_to_file :info, msg
  end

  def warn(msg)
    log_to_file :warn, msg
  end

  def error(msg)
    log_to_file :error, msg
  end

  def fatal(msg)
    log_to_file :fatal, msg
  end

  def unknown(msg)
    log_to_file :unknown, msg
  end

  private
  
  def log_to_file(type, msg)
    @loggers.each{|log| log.send(type, msg) if log}
  end

end

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

def verify_onchain(log, transfer_list, proposal)
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
  success = true

  keys_local = user_rewards_hash_local.keys
  keys_chain = user_rewards_hash_chain.keys
  keys_all = (keys_local + keys_chain).uniq
  keys_only_in_local = keys_local - keys_chain
  keys_only_in_chain = keys_chain - keys_local

  keys_all.each do |acc|
    vlocal = user_rewards_hash_local[acc]
    vchain = user_rewards_hash_chain[acc]
    if vlocal != vchain
      success = false
      log.error format("%-30s %10s(local: %s != proposal: %s)", "check #{acc} reward ...", "ERROR", vlocal.to_s, vchain.to_s)
    else
      log.info format("%-30s %10s", "check #{acc} reward ...", "OK")
    end
  end

  keys_only_in_local.each do |acc|
    success = false
    log.error format("%-30s %10s(only local: %s)", "check #{acc} reward ...", "ERROR", user_rewards_hash_local[acc]) 
  end

  keys_only_in_chain.each do |acc|
    success = false
    log.error format("%-30s %10s(only proposal: %s)", "check #{acc} reward ...", "ERROR", user_rewards_hash_chain[acc]) 
  end
  
  return success
end

if __FILE__ == $0

  date = ARGV[0]
  raise 'miss date arg.' if date.nil?

  proposal_id = ARGV[1]
  raise 'miss proposal_id arg.' if proposal_id.nil?

  # => 1. Calc local data
  base_dir = '/home/ubuntu/bts_delay_node/witness_node_data_dir/ugly-snapshots/2020/' + date

  output_dir = File.expand_path("..", __FILE__) + '/'

  rewards_data_path = output_dir + date + '.cache.rewards'

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

  log = MyLogger.new(output_dir + date + ".verify.log")

  # => 2. Verify
  log.info "Verify dir #{base_dir} ..."
  log.info '-'.chr * 30
  proposal = query_proposal(proposal_id)
  if verify_onchain(log, transfer_list, proposal)
    log.info '-'.chr * 30
    log.info "Proposal data verification passed, #{date} OK OK OK !!!"
  else
    log.info '-'.chr * 30
    log.error "Proposal data verification failed, #{date} ERROR !!!"
  end
end
