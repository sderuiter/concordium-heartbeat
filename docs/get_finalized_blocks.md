# Get Finalized Blocks

This methods runs forever, with a sleep of 1 sec. The goal is to get **finalized blocks** (by calling `get_finalized_block_at_height` on `grpcclient`) from the chosen `net`. 

If found, these blocks are appeneded to the queue `finalized_block_infos_to_process`. 

The method batches blocks to be processed up to `MAX_BLOCKS_PER_RUN`.

### Logic
1. Every second we start with a request counter of 0. This counter is compared to `MAX_BLOCKS_PER_RUN` to determine if we have reached the batch size.
2. Next, we retrieve the current stored value in the `helpers` collection for `_id`: `heartbeat_last_processed_block`.
3. Entering the while loop if we think we need to look for new finalized blocks.
4. We increment `heartbeat_last_processed_block_height` with 1 to get the next finalized block. 
5. Check if this new height isn't already in the queue `finalized_block_infos_to_process` to be processed, which can occur if we search for blocks too quickly, while processing hasn't finished yet. 
6. We then retrieve the finalized block at the height `heartbeat_last_processed_block_height`. If such a block exists, it is appended to the queue. 
