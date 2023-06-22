# Send To Mongo
The purpose of this method is to take all `Mongo queues` and send them to their respective collections. 

* `queue.blocks` to `Collection.blocks`
* `queue.transactions` to `Collection.transactions`
* `queue.involved_all` to `Collection.involved_accounts_all`
* `queue.involved_all` to `Collection.involved_accounts_all`
* `queue.involved_transfer` to `Collection.involved_accounts_transfer`
* `queue.involved_contract` to `Collection.involved_contracts`
* `queue.instances` to `Collection.instances`
* `queue.modules` to `Collection.modules`
* `queue.block_per_day` to `Collection.blocks_per_day`
* `queue.logged_events` to `Collection.tokens_logged_events`
* `queue.token_addresses_to_redo_accounting` to helper document `redo_token_addresses`
* `queue.provenance_contracts_to_add` to `Collection.tokens_tags` for specific Provenance token tag. This is used only if there is a new Provenance contract detected. 

At the end of every send, the respective queue is emptied. 