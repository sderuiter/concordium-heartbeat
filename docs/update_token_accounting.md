# Token Accounting

Token accounting is the process of accounting for mints, burns and transfers for CIS-2 tokens. These tokens are not stored anywhere on-chain. Instead, account holdings can only be deduced from the `logged events`. Therefore it is very important that logged events are stored correctly, with no omissions and duplications. Also, the order in which logged events are applied, matters, as you can't burn or transfer tokens you do not own. 

!!! Links
    [CIS-2 Specification](http://proposals.concordium.software/CIS/cis-2.html#logged-events)

### Collections

#### tokens_logged_events
``` py
{
  "_id": "5351380-<8586,0>-45000000-fe0445000000010043a0c163c7f7a8e58ba325fde7b504153592ded8507f75264fb4fc4b30000002-updated-23-0-0",
  "logged_event": "fe0445000000010043a0c163c7f7a8e58ba325fde7b504153592ded8507f75264fb4fc4b30000002",
  "result": {
    "tag": 254,
    "token_id": "45000000",
    "token_amount": "1",
    "to_address": "3TXeDWoBvHQpn7uisvRP8miX47WF4tpkKBidM4MLsDPeshUTs8"
  },
  "tag": 254,
  "event_type": "mint_event",
  "block_height": 5351380,
  "tx_hash": "2c0a2e67766c41c8d4c2484db5a9804f937ab862f8528639522d2cb1bb152e18",
  "tx_index": 23,
  "ordering": 1,
  "token_address": "<8586,0>-45000000",
  "contract": "<8586,0>"
}
```

#### tokens_token_addresses
``` py
{
  "_id": "<8590,0>-13cd0959b713a9eb484e99a19eca0c9469654d937192857390bc11268299243f",
  "contract": "<8590,0>",
  "token_id": "13cd0959b713a9eb484e99a19eca0c9469654d937192857390bc11268299243f",
  "token_amount": "1",
  "metadata_url": "https://nft.provenance-tags.com/13CD0959B713A9EB484E99A19ECA0C9469654D937192857390BC11268299243F",
  "last_height_processed": 5547541,
  "token_holders": {
    "44W9aorqnxQukP7xfwo1vNYkMowbxvkX1fJopAQQbtqa5BSEg2": "1"
  }
}
```

#### tokens_tags
``` py
{
  "_id": "USDT",
  "contracts": [
    "<9341,0>"
  ],
  "tag_template": false,
  "single_use_contract": true,
  "logo_url": "https://cryptologos.cc/logos/tether-usdt-logo.svg?v=025",
  "decimals": {
    "$numberLong": "6"
  },
  "owner": "Arabella",
  "module_name": "cis2-bridgeable"
}
```

#### tokens_accounts
``` py
{
  "_id": "3yuQ1JpjrY1Mk8Ftj5M5i3NiTViRuKAWNMPUMBaSHnubCMkw2M",
  "tokens": {
    "<9354,0>-": {
      "token_address": "<9354,0>-",
      "contract": "<9354,0>",
      "token_id": "",
      "token_amount": "2000000"
    }
  }
}
```

The starting point is reading the helper document `token_accounting_last_processed_block`. If this value is either not present or set to -1, all token_addresses (and associated token_accounts) will be reset.

!!! Reset
    In order to reset token accounting for all tokens, set helper document `token_accounting_last_processed_block` to -1. 


This value indicates the last block that was processed for logged events. Hence, if we start at logged events after this block, there is no double counting. 


We collect all logged events from the collection `tokens_logged_events` with the following query:

``` py
{"block_height": {"$gt": token_accounting_last_processed_block}}
.sort(
    [
        ("block_height", ASCENDING),
        ("tx_index", ASCENDING),
        ("ordering", ASCENDING),
    ]
)
```

If there are `logged_events` to process, we sort the events into a dict `events_by_token_address`, keyed on token_address and contains an ordered list of logged events related to this token_address.

``` py                    
events_by_token_address: dict[str, list] = {}
for log in result:
    events_by_token_address[log.token_address] = events_by_token_address.get(log.token_address, [])
    events_by_token_address[log.token_address].append(log)
```

We then loop through all `token_addresses` that have logged events to process and call `token_accounting_for_token_address`. 

The method `token_accounting_for_token_address` first deterines the need to create a new token address (when we are starting over with token accounting, setting `token_accounting_last_processed_block` to -1, or if the `token_address` doesn't exist).

Then for this `token_address`, we loop through all logged events that need processing, and call `execute_logged_event`. 

This method preforms the neccesary actions on the `token_address_as_class` variable. Once all logged events for a `token_address` are executed, we save the result back to the collection `tokens_token_addresses`. 

Note that calculations only stored on the `token_address` object in the collection `tokens_token_addresses`. After the logged events are executed and saved, the holder information is copied from the `token_address` to the collection `tokens_accounts`, to enrure there is consistency.

Finally, after all logged events are processed for all token addresses, write back to the helper collection for `_id`: `token_accounting_last_processed_block` the block_height of the last logged event. 