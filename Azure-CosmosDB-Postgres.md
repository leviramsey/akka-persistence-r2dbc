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
