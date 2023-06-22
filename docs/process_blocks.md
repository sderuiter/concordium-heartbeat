# Process Blocks
This method takes the queue `finalized_block_infos_to_process` and sends this queue to `process_list_of_blocks`. 

When we return, the method `log_last_processed_message_in_mongo` gets called to store the helper document `heartbeat_last_processed_block`.