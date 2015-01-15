# HBase Custom Replication Endpoint Example
## Overview
HBase 0.98.9 added the ability to create custom replication endpoints. This is an example about how to create and use a custom replication endpoint.

## Usage
### Cluster
1. `mvn clean package`
2. Copy jar to lib directory on each region server
3. Restart region servers
4. `java -cp $(hbase classpath):target/hbase-custom-replication-endpoint-example-1.0-SNAPSHOT.jar com.avalonconsult.hbase.utility.AddCustomReplicationEndpoint --peer-name customreplicationendpoint --class-name "com.avalonconsult.hbase.CustomReplicationEndpoint"`

### Unit Tests
1. `mvn clean test`

## Adding a Custom Replication Endpoint
### HBase Shell
Currently not supported.

### Java
```java
try(ReplicationAdmin replicationAdmin = new ReplicationAdmin(utility.getConfiguration())) {
    ReplicationPeerConfig peerConfig = new ReplicationPeerConfig()
          .setClusterKey(ZKUtil.getZooKeeperClusterKey(utility.getConfiguration()))
          .setReplicationEndpointImpl(TestWrapperCustomReplicationEndpoint.class.getName());

    Map<TableName, List<String>> tableCfs = new HashMap<>();
    List<String> cfs = new ArrayList<>();
    cfs.add(COLUMN_FAMILY);
    tableCfs.put(TABLE_NAME, cfs);

    replicationAdmin.addPeer(PEER_NAME, peerConfig, tableCfs);
}
```

## References
* https://issues.apache.org/jira/browse/HBASE-11367
* https://issues.apache.org/jira/browse/HBASE-11992
* https://issues.apache.org/jira/browse/HBASE-12254
