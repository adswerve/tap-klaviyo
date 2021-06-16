#!/usr/bin/env/python

import json
import os
import sys
# import singer
# from tap_klaviyo.utils import get_incremental_pull, get_full_pulls, get_all_pages
from singer import get_logger, utils as singer_utils
# from streams import Stream
# from discovery import discover, do_discover #probably wanna add vanilla_discover for the case when no catalog is present or throw error
# from sync import do_sync

LOGGER = get_logger()

CREDENTIALS_KEYS = ["api_key"]
REQUIRED_CONFIG_KEYS = ["start_date"] + CREDENTIALS_KEYS
ENDPOINTS = {
    'global_exclusions': 'https://a.klaviyo.com/api/v1/people/exclusions',
    'lists': 'https://a.klaviyo.com/api/v1/lists',
    'metrics': 'https://a.klaviyo.com/api/v1/metrics',
    'metric': 'https://a.klaviyo.com/api/v1/metric/',
    'campaigns': 'https://a.klaviyo.com/api/v1/campaigns'
}
EVENT_MAPPINGS = {
    "Received Email": "receive",
    "Clicked Email": "click",
    "Opened Email": "open",
    "Bounced Email": "bounce",
    "Unsubscribed": "unsubscribe",
    "Marked Email as Spam": "mark_as_spam",
    "Unsubscribed from List": "unsub_list",
    "Subscribed to List": "subscribe_list",
    "Updated Email Preferences": "update_email_preferences",
    "Dropped Email": "dropped_email",
}
FULL_PULLS = [
    "campaigns", "lists", "global_exclusions"
]



@singer_utils.handle_top_exception(LOGGER)
def main():

    args = singer_utils.parse_args(REQUIRED_CONFIG_KEYS)

    if args.discover:
        from discovery import do_discover
        do_discover(args.config['api_key'])

    else:
        if args.catalog:
            pass
        elif args.properties:
            sys.stderr.write("Property selections: DEPRECATED, Please use --catalog instead")
        else:
            print("Catalog file required, please use --catalog <path/to/catalog/file>")
            exit(1)

        from discovery import discover
        from sync import do_sync
        catalog = args.catalog.to_dict()  # if args.catalog else discover(args.config['api_key'])

        state = args.state if args.state else {"bookmarks": {}}
        do_sync(args.config, state, catalog)


if __name__ == '__main__':
    main()
