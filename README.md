# hbase-custom-replication-endpoint-example
## Overview
HBase 0.98.9 added the ability to create custom replication endpoints. This is an example about how to create and use a custom replication endpoint.

## Usage
### Cluster
1. `mvn clean package`
2. Copy jar to lib directory on each region server
3. Restart region servers
4. ...

### Unit Tests
1. `mvn clean test`

## References
* https://issues.apache.org/jira/browse/HBASE-11367
* https://issues.apache.org/jira/browse/HBASE-11992
* https://issues.apache.org/jira/browse/HBASE-12254
