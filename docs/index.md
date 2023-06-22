# Welcome to MkDocs

For full documentation visit [mkdocs.org](https://www.mkdocs.org).

## Commands

* `mkdocs new [dir-name]` - Create a new project.
* `mkdocs serve` - Start the live-reloading docs server.
* `mkdocs build` - Build the documentation site.
* `mkdocs -h` - Print help message and exit.

## Project layout

    mkdocs.yml    # The configuration file.
    docs/
        index.md  # The documentation homepage.
        ...       # Other markdown pages, images and other files.

## Main loop
loop.create_task(heartbeat.get_finalized_blocks())

loop.create_task(heartbeat.process_blocks())
loop.create_task(heartbeat.send_to_mongo())
loop.create_task(heartbeat.update_token_accounting())

loop.create_task(heartbeat.get_special_purpose_blocks())
loop.create_task(heartbeat.process_special_purpose_blocks())

loop.create_task(heartbeat.get_redo_token_addresses())
loop.create_task(heartbeat.special_purpose_token_accounting())

loop.create_task(heartbeat.update_nodes_from_dashboard())
loop.create_task(heartbeat.update_involved_accounts_all_top_list())