Single node CosmosDB Postgres

- Schema: creates successfully
- For now, treating 'azure-cosmos' as a synonym for postgres...
- EventsBySlicePubSubSpec times out (20 seconds)
  * tests are designed for a local DB, so loosen timeout to 200 seconds
  * ...fails after 41 seconds with a LoggingTestKit timeout, so loosen that
  * seems to only be able to hit 30-35 persists/second and the test needs >50 to fire, so I'll set the threshold to 30...
  * ...would you accept 25 per second?
  * ...tyvm
