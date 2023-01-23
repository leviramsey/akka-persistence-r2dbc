Single node CosmosDB Postgres

- Schema: creates successfully
- For now, treating 'azure-cosmos' as a synonym for postgres...
- EventsBySlicePubSubSpec times out (20 seconds)
  * tests are designed for a local DB, so loosen timeout to 200 seconds
  * ...fails after 41 seconds with a LoggingTestKit timeout, so loosen that
  * seems to only be able to hit 30-35 persists/second and the test needs >50 to fire, so I'll set the threshold to 30...
  * ...would you accept 25 per second?
  * ...tyvm

Multi-node CosmosDB Postgres
- Schema: creates successfully
- Still treating as synonym for postgres...
- Lots of test breakage:

```
Failed to persist event type [java.lang.String] with sequence number [1] for persistenceId [TestEntity-29|entity-1]
Caused by: io.r2dbc.postgresql.ExceptionFactory$PostgresqlNonTransientResourceException: subqueries are not supported within INSERT queries
```

20230123: starting this up again, with `db-timestamp-monotonic-increasing = on`

* All core tests pass: huzzah!
* All projection tests pass: double huzzah!

Summary of Differences from `main` (`9fbf8a2539`)
=================================================
* the different dialect exists strictly to allow the test configs to be clean: the underlying dialect is postgres
* `db-timestamp-monotonic-increasing` is set `on`.  Since the DB timestamps are almost certainly not globally monotonic increasing, this entails making a bit of a bet that we can configure the projections and queries to emit in order and not lose events
* some test timeouts and thresholds for throughput are tweaked: the existing tests tend to assume at least LAN-level connectivity; these testing runs are crossing the internet to Azure from a residential connection.
* DDL for distributing tables on Citus/CosmosDB Postgres
