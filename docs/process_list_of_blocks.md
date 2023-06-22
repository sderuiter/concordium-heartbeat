# Process List of Blocks

!!!TODO
    special purpose blocks

This method is the workhorse of **Heartbeat**, kicking of the various processes to process a block. The method is used for the regular finalized blocks, as well as special purpose blocks. This is controlled by calling the method with `special_purpose=True`.

TODO: `existing_source_modules`

Steps every block on the queue:

#### Add Block and Txs to Queue
This method takes a block and retrieves its transactions (if any) and updates the block with `transaction_hashes`.
The updated block is sent to the `MongoDB queue.Blocks`.

For each block with transactions, we call `generate_indices_based_on_transactions`.

##### generate_indices_based_on_transactions
In short, this method applies rules to determine whether specific indexes need to be stored for these transactions, but also stored CIS-2 logged events.
!!! Note
    This is discussed in detail in [Token Accounting](update_token_accounting.md).

To get to this result, we call `classify_transaction` for the transaction. This classifies the transaction and returns a `ClassificationResult` object, containing `sender`, `receiver`, `tx_hash`, `memo`, `amount`, `type`, `contents`, `accounts_involved_all`, `accounts_involved_transfer`, `contracts_involved`, `list_of_contracts_involved`.

We use this return object to update our collections. 

* `Mongo queue.involved_all` is used for all transactions
* `Mongo queue.involved_transfer` is used for all transactions
* `Mongo queue.involved_contract` is used for all transactions that involve a smart contract. Note that, as transactions with smart contract often involve many calls do various instances, a transaction will be stored for each individual smart contract.
* `Mongo queue.instances` is used to upsert information about the instance.
* `Mongo queue.modules` is used to upsert information about the module.

#### Lookout for Payday
This method looks at a block and determines if the special events make it a payday block. If so, we will store in the `helpers` collection a document with `_id`: `last_known_payday`, that contains details about the payday block we have just encountered. Note that this helper document is used by the `payday-MAINNET` repo. This repo checks periodically the last payday as stored in collection `paydays`. If the helper document is newer, the payday process is started there. 
If we are processing multiple days worth of blocks here, we might end up overwriting the helper document with new payday information before the payday repo has had a chance to process it. Therefore the bool `payday_not_yet_processed` is introduced to prevent this.

#### Lookout for End of Day
This method looks at a block and determines if it's the last block of the day. If found, it prepares a dictionary with day information (height, slot time, hash, for first and last block of the day) and stores this in `Mongo queue.block_per_day`.
