import sys
import json
import pika
import time
from . import NotModified, ConcurrentUpdate
from .ermrest_catalog import ErmrestCatalog


class PollingErmrestCatalog(ErmrestCatalog):
    """Persistent handle for an ERMrest catalog.

       Provides a higher-level state_change_once() idiom to
       efficiently find candidate rows, transform them, and apply
       updates.

       Provides a higher-level blocking_poll() idiom to efficiently
       poll a catalog, using AMQP to optimize polling where possible.
       (AMQP is currently limited to clients on localhost of catalog
       in practice.)

       These features can be composed to implement condition-action
       agents with domain-specific logic, e.g.

       catalog = ErmrestCatalog(...)
       idle_etag = None

       def look_for_work():
           global idle_etag
           idle_etag, batch = catalog.state_change_once(
               # claim up to 5 items per batch
               '/entity/Foo/state=actionable?limit=5',
               '/attributegroup/Foo/id;state',
               lambda row: {'id': row['id'], 'state': 'claimed'},
               idle_etag
           )
           for candidate, update in batch:
               # assume we have free reign on claimed candidates
               # using state=claimed as a semaphore
               revision = candidate.copy()
               revision['state'] = update['state']
               ... # do agent work
               revision['state'] = 'complete'
               catalog.put('/entity/Foo', [revision])

       catalog.blocking_poll(look_for_work)

    """

    def __init__(self, scheme, server, catalog_id, credentials={}, caching=True, session_config=None):
        """Create ERMrest catalog binding.

           Arguments:
             scheme: 'http' or 'https'
             server: server FQDN string
             catalog_id: e.g. '1'
             credentials: credential secrets, e.g. cookie
             caching: whether to retain a GET response cache

        """
        ErmrestCatalog.__init__(self, scheme, server, catalog_id, credentials, caching, session_config)
        self.amqp_server = server
        self.amqp_connection = None
        self.notice_exchange = "ermrest_changes"

    def _amqp_bind(self):
        """Bind or rebind to AMQP for change notice monitoring."""
        if self.amqp_connection is not None:
            try:
                self.amqp_connection.close()
            except:
                pass

        self.amqp_connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=self.amqp_server
            )
        )

        # listening channel for ermrest change notifications
        self.notice_channel = self.amqp_connection.channel()
        try:
            # newer pika API
            self.notice_channel.exchange_declare(self.notice_exchange, exchange_type='fanout')
            self.notice_queue_name = self.notice_channel.queue_declare('', exclusive=True).method.queue
            self.notice_channel.queue_bind(self.notice_queue_name, self.notice_exchange)
        except TypeError as te:
            # try older API as fallback
            self.notice_channel.exchange_declare(exchange=self.notice_exchange, type='fanout')
            self.notice_queue_name = self.notice_channel.queue_declare(exclusive=True).method.queue
            self.notice_channel.queue_bind(exchange=self.notice_exchange, queue=self.notice_queue_name)
        sys.stderr.write('ERMrest change-notice channel open.\n')

    @staticmethod
    def _run_notice_event(look_for_work):
        """Consume all available work before returning."""
        while True:
            try:
                found = look_for_work()
                if not found:
                    break
            except ConcurrentUpdate as e:
                # retry if we had a race-condition while claiming work
                sys.stderr.write('Handling ErmrestConcurrentUpdate exception...\n')
                pass

    def blocking_poll(self, look_for_work, polling_seconds=600, coalesce_seconds=0.1):
        """Use ERMrest change-notice monitoring to optimize polled work processing.

           Client-provided look_for_work function finds actual work in
           ERMrest and processes it. We only optimize the scheduling
           of this work.

           Run look_for_work() whenever there *might* be more work in
           ERMrest.

           If look_for_work() returns True, assume there is more work.

           If look_for_work() returns non-True, wait for ERMrest
           change-notice or polling_seconds timeout before looking
           again (whichever comes first).

           On any change-monitoring communication error, assume there
           might be more work and restart the monitoring process.

           Other exceptions abort the blocking_poll() call.

        """
        last_run_time = None
        retry_amqp = False
        while True:
            try:
                self._amqp_bind()
                polling_gen = self.notice_channel.consume(self.notice_queue_name, exclusive=True,
                                                          inactivity_timeout=polling_seconds)
                coalesce_gen = self.notice_channel.consume(self.notice_queue_name, exclusive=True,
                                                           inactivity_timeout=coalesce_seconds)
                retry_amqp = True

                # run once to catch up on historical changes since before we opened our channel
                self._run_notice_event(look_for_work)

                # follow channel with polling_seconds periodic wakeup even when idle
                for result in polling_gen:
                    # ... and delay for up to coalesce_seconds to combine multiple notices into one wakeup
                    sys.stderr.write('Woke up on %s.\n' % ('change-notice' if result else 'poll timeout'))
                    while next(coalesce_gen)[0] is not None:
                        pass

                    # catch up on changes since last time we looked
                    self._run_notice_event(look_for_work)

            except pika.exceptions.ConnectionClosed as e:
                # do our best without AMQP by falling back on polling
                now = time.time()
                if not retry_amqp:
                    sys.stderr.write('Using basic polling due to AMQP communication problems.\n')
                    if last_run_time and not retry_amqp:
                        time.sleep(max(0, polling_seconds - (now - last_run_time)))
                    last_run_time = now
                    self._run_notice_event(look_for_work)

            except Exception as e:
                sys.stderr.write('Got error %s in main event loop.' % e)
                raise

    def state_change_once(self, query_datapath, update_datapath, row_transform_func, idle_etag=None):
        """Perform generic conditional state update via GET-PUT sequence.

           Arguments:
             query_datapath: a query for candidate rows
             update_datapath: an update to consume update rows
             row_transform_func: maps candidate to update rows
             idle_etag: no-op if table is still in this state

           Returns: (idle_etag, [(candidate, update)...])
             idle_etag: value to thread to future calls
             [(candidate, update)...]: each row that was updated

           Exceptions from the transform or update process will abort
           without returning results.

           1. GET query_datapath to get candidate row(s)
           2. apply row_transform_func(row) to get updated content
           3. PUT update_datapath to deliver transformed content
              -- discards rows transformed to None

           Uses opportunistic concurrency control with ETag, If-Match,
           etc. for safety.

        """
        try:
            before = self.get(query_datapath, raise_not_modified=True)
        except NotModified as e:
            before = self._cache[self._server_uri + query_datapath]

        if idle_etag is not None:
            if before.headers['etag'] == idle_etag:
                sys.stderr.write('No new state to process.\n')
                return idle_etag, []

        rows_before = before.json()

        if not rows_before:
            sys.stderr.write('No candidate rows found.\n')

        plan = [(row, row_transform_func(row)) for row in rows_before]
        plan = [(candidate, update) for candidate, update in plan if update is not None]

        if not plan:
            sys.stderr.write('No row updates requested.\n')
            return before.headers.get('etag'), []

        after = self.put(
            update_datapath,
            json=[update for candidate, update in plan],
            guard_response=before
        )
        sys.stderr.write('Updated %d rows in catalog:\n' % len(after.json()))
        json.dump(plan, sys.stderr, indent=2)
        sys.stderr.write('\n')
        return after.headers.get('etag'), plan
