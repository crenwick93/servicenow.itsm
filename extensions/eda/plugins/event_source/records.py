# -*- coding: utf-8 -*-
#
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)

DOCUMENTATION = r"""
module: records
short_description: Event Driven Ansible source for ServiceNow records.
description:
  - Poll the ServiceNow API for any new records in a table, using them as a source for Event Driven Ansible.
  - This plugin can use the same environment variables as the rest of the ServiceNow collection to
    configure the instance connection.

author:
  - ServiceNow ITSM Collection Contributors (@ansible-collections)

extends_documentation_fragment:
  - servicenow.itsm.query
  - servicenow.itsm.instance

options:
  table:
    description:
      - ServiceNow table to query for new records.
    required: true
  interval:
    description:
      - The number of seconds to wait before performing another query.
    required: false
    default: 5
  updated_since:
    description:
      - Specify the time that a record must be updated after to be considered new and captured by this plugin.
      - This value should be a date string in the format of "%Y-%m-%d %H:%M:%S".
      - If not specified, the plugin will use the current UTC time as a default. This means any records updated
        after the plugin started will be captured.
    required: false
"""

EXAMPLES = r"""
- name: Watch for new records
  hosts: localhost
  sources:
    - name: Watch for updated change requests
      servicenow.itsm.records:
        instance:
          host: https://dev-012345.service-now.com
          username: ansible
          password: ansible
        table: change_request
        interval: 1
        updated_since: "2025-08-13 12:00:00"  # UTC

  rules:
    - name: New record created
      condition: event.sys_id is defined
      action:
        debug:
"""

import asyncio
from typing import Any, Dict, Optional
import datetime
import logging
import sys
import os

# Ensure module_utils imports resolve in EDA
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../.."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from plugins.module_utils.instance_config import get_combined_instance_config
from plugins.module_utils import client, table
from plugins.module_utils.query import construct_sysparm_query_from_query
from ansible.errors import AnsibleParserError

logger = logging.getLogger(__name__)

_DATETIME_FMT = "%Y-%m-%d %H:%M:%S"


def _fmt(ts: datetime.datetime) -> str:
    return ts.strftime(_DATETIME_FMT)


def _utcnow_seconds() -> datetime.datetime:
    return datetime.datetime.utcnow().replace(microsecond=0)


class RecordsSource:
    def __init__(self, queue: asyncio.Queue, args: Dict[str, Any]):
        self.queue = queue
        self.instance_config = get_combined_instance_config(
            config_from_params=args.get("instance")
        )

        self.table_name: str = args.get("table")
        self.list_query: Optional[Dict[str, str]] = self._format_list_query(args)
        self.interval: int = int(args.get("interval", 5))
        self.updated_since: datetime.datetime = self._parse_string_to_datetime(
            args.get("updated_since")
        )

        self.snow_client = client.Client(**self.instance_config)
        self.table_client = table.TableClient(self.snow_client)

        # Track last emitted sys_updated_on per sys_id (across cycles)
        self.previously_reported_records: Dict[str, str] = {}

    def _format_list_query(self, args: Dict[str, Any]) -> Optional[Dict[str, str]]:
        if args.get("sysparm_query"):
            return {"sysparm_query": args.get("sysparm_query")}

        if not args.get("query"):
            return None

        try:
            return {
                "sysparm_query": construct_sysparm_query_from_query(args.get("query"))
            }
        except ValueError as e:
            raise AnsibleParserError(f"Unable to parse query: {e}")

    def _build_bounded_query(
        self, since: datetime.datetime, until: datetime.datetime
    ) -> Dict[str, str]:
        """
        Build a sysparm_query bounded to (since, until] and sorted ascending.
        If the user supplied a sysparm_query, AND the bounds to it.
        """
        lower = _fmt(since)
        upper = _fmt(until)
        bounds = f"sys_updated_on>{lower}^sys_updated_on<={upper}"
        ordering = "ORDERBYsys_updated_on^ORDERBYsys_id"

        if self.list_query and self.list_query.get("sysparm_query"):
            base = self.list_query["sysparm_query"].strip()
            has_orderby = "ORDERBY" in base or "ORDERBYDESC" in base
            combined = f"{base}^{bounds}"
            if not has_orderby:
                combined = f"{combined}^{ordering}"
            return {"sysparm_query": combined}

        return {"sysparm_query": f"{bounds}^{ordering}"}

    async def start_polling(self):
        while True:
            await self._poll_for_records()
            logger.info("Sleeping for %s seconds", self.interval)
            await asyncio.sleep(self.interval)

    async def _poll_for_records(self):
        # Bound this cycle to (updated_since, poll_start]
        poll_start = _utcnow_seconds()

        logger.info(
            "Polling for new records in %s since %s (until %s)",
            self.table_name,
            self.updated_since,
            poll_start,
        )

        bounded_query = self._build_bounded_query(self.updated_since, poll_start)

        # Within-cycle de-dupe: latest sys_updated_on per sys_id this cycle
        emitted_this_poll: Dict[str, str] = {}

        newest_seen = self.updated_since

        for record in self.table_client.list_records(self.table_name, bounded_query):
            await self._process_record(record, emitted_this_poll)

            try:
                ts = self._parse_string_to_datetime(record.get("sys_updated_on"))
                if ts > newest_seen:
                    newest_seen = ts
            except Exception:
                # Ignore malformed timestamps for advancing since
                pass

        # Remember last version per sys_id emitted this cycle
        self.previously_reported_records = emitted_this_poll

        # Advance since monotonically, never into the future
        next_since = max(newest_seen, poll_start)
        if next_since > self.updated_since:
            logger.debug(
                "Advancing since timestamp: %s -> %s", self.updated_since, next_since
            )
            self.updated_since = next_since
        else:
            logger.debug(
                "Since timestamp unchanged (next_since=%s <= updated_since=%s)",
                next_since,
                self.updated_since,
            )

    async def _process_record(
        self, record: Dict[str, Any], emitted_this_poll: Dict[str, str]
    ):
        sys_id = record.get("sys_id")
        updated_on = record.get("sys_updated_on")
        if not sys_id or not updated_on:
            return

        rec_ts = self._parse_string_to_datetime(updated_on)
        # Enforce lower bound strictly inside the cycle; upper bound is handled by the query
        if rec_ts <= self.updated_since:
            return

        # Within-cycle de-dupe (same version of same record in one poll)
        if emitted_this_poll.get(sys_id) == updated_on:
            return

        # Cross-cycle de-dupe (same version already emitted last cycle)
        if self.previously_reported_records.get(sys_id) == updated_on:
            return

        # Record emission and remember latest version seen this cycle
        emitted_this_poll[sys_id] = updated_on
        await self.queue.put(record)

    def _parse_string_to_datetime(
        self, date_string: Optional[str] = None
    ) -> datetime.datetime:
        """
        Parse ServiceNow UTC 'YYYY-MM-DD HH:MM:SS' into naive datetime (UTC).
        When None, return current UTC at second precision.
        """
        if date_string is None:
            return _utcnow_seconds()

        try:
            return datetime.datetime.strptime(date_string, _DATETIME_FMT)
        except ValueError:
            raise ValueError(f"Invalid date string: {date_string}")


# Entrypoint from ansible-rulebook
async def main(queue: asyncio.Queue, args: Dict[str, Any]):
    records_source = RecordsSource(queue, args)
    await records_source.start_polling()
