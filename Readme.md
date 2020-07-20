
## 概述
Bitshares MarketMaking Contest script for  https://bitsharestalk.org/index.php?topic=29665.msg342265

## How to verify the reward data
1. Run this branch (https://github.com/viaNull/bitshares-core/tree/bts-mmcontest-v2 ) delayed node (https://github.com/bitshares/bitshares-core/wiki/Delayed-Node) to generate snapshots (update "config.ini" as follows)

`ugly-snapshot-markets = [["1.3.0","1.3.113"],["1.3.0","1.3.121"],["1.3.0","1.3.120"],["1.3.0","1.3.1325"],["1.3.0","1.3.2241"],["1.3.0","1.3.3926"],["1.3.0","1.3.4157"],["1.3.0","1.3.4198"],["1.3.0","1.3.5286"],["1.3.0","1.3.5542"],["1.3.0","1.3.5589"],["1.3.0","1.3.2598"],["1.3.0","1.3.3715"],["1.3.0","1.3.4199"],["1.3.0","1.3.4760"],["1.3.0","1.3.2635"],["1.3.0","1.3.4106"],["1.3.113","1.3.2241"],["1.3.113","1.3.3926"],["1.3.113","1.3.4157"],["1.3.113","1.3.4198"],["1.3.113","1.3.5286"],["1.3.113","1.3.5542"],["1.3.113","1.3.5589"],["1.3.113","1.3.2598"],["1.3.113","1.3.3715"],["1.3.113","1.3.4199"],["1.3.113","1.3.4760"],["1.3.113","1.3.2635"],["1.3.113","1.3.4106"],["1.3.121","1.3.2241"],["1.3.121","1.3.3926"],["1.3.121","1.3.4157"],["1.3.121","1.3.4198"],["1.3.121","1.3.5286"],["1.3.121","1.3.5542"],["1.3.121","1.3.5589"],["1.3.121","1.3.2598"],["1.3.121","1.3.3715"],["1.3.121","1.3.4199"],["1.3.121","1.3.4760"],["1.3.121","1.3.2635"],["1.3.121","1.3.4106"],["1.3.120","1.3.2241"],["1.3.120","1.3.3926"],["1.3.120","1.3.4157"],["1.3.120","1.3.4198"],["1.3.120","1.3.5286"],["1.3.120","1.3.5542"],["1.3.120","1.3.5589"],["1.3.120","1.3.2598"],["1.3.120","1.3.3715"],["1.3.120","1.3.4199"],["1.3.120","1.3.4760"],["1.3.120","1.3.2635"],["1.3.120","1.3.4106"],["1.3.1325","1.3.2241"],["1.3.1325","1.3.3926"],["1.3.1325","1.3.4157"],["1.3.1325","1.3.4198"],["1.3.1325","1.3.5286"],["1.3.1325","1.3.5542"],["1.3.1325","1.3.5589"],["1.3.1325","1.3.2598"],["1.3.1325","1.3.3715"],["1.3.1325","1.3.4199"],["1.3.1325","1.3.4760"],["1.3.1325","1.3.2635"],["1.3.1325","1.3.4106"]]`

2. In the delay_node's data dir, cd to `ugly-snapshots/2020/<date>`
    - create folder `filled_orders` and `snapshots`
    - mv files with name \*.filled to folder `filled_orders`
    - mv file with name \*.snapshot to folder `snapshots`
    
    You can use the following command to move files in case error occurs:
  
    ```
    find . -maxdepth 1 -name *.filled | xargs -i mv {} filled_orders/
    find . -maxdepth 1 -name *.snapshot | xargs -i mv {} snapshots/
    ```

3. Edit the Ruby script `cb_snapshot_process.rb`, scoll down to the end of this file, change variable `base_dir` to the actual data dir.

4. Run the Ruby script `cb_snapshot_process.rb` to calculate rewards.

5. Run the Ruby script `cb_verify.rb date proposal_id` to verify rewards data.

## Author

[ChenBin](!https://github.com/Chen188)
[syalon](https://github.com/syalon)

## Related

This Project built from [bts-mmcontest-scripts](https://github.com/abitmore/bts-mmcontest-scripts) by [Abit](https://github.com/abitmore)
