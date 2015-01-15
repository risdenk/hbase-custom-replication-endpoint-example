# HBase Custom Replication Endpoint Example
## Overview
HBase 0.98.9 added the ability to create custom replication endpoints. This is an example about how to create and use a custom replication endpoint.

## Usage
### Cluster
1. `mvn clean package`
2. Copy jar to lib directory on each region server
3. Restart region servers
4. `java -cp $(hbase classpath):target/hbase-custom-replication-endpoint-example-1.0-SNAPSHOT.jar com.avalonconsult.hbase.utility.AddCustomReplicationEndpoint --peer-name customreplicationendpoint --class-name com.avalonconsult.hbase.CustomReplicationEndpoint`

Using the `hbase shell` enter the following commands:
```
create 'test-table', {NAME => 'cf', REPLICATION_SCOPE=>'1'}
put 'test-table', 'a', 'cf:q', 'v'
disable 'test-table'
drop 'test-table'
```

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

## Notes
* addPeer methods that are deprecated are currently used by `hbase shell`
* Why is there a tableCFs map? Why not stick with the string?
* This method currently calls `getTableCfsStr(tableCfs)` to convert the tableCfs map to a string.
```java
public void addPeer(String id, ReplicationPeerConfig peerConfig, Map<TableName, ? extends Collection<String>> tableCfs) throws ReplicationException
```
* ReplicationAdmin should have a method signature:
```java
public void addPeer(String id, ReplicationPeerConfig peerConfig, String tableCFs) throws ReplicationException
```
* There could be a ReplicationAdmin method like the following:
```java
public void addPeer(String id, String clusterKey, String replicationEndpointClassname, String tableCFs) throws ReplicationException {
  this.replicationPeers.addPeer(
    id,
    new ReplicationPeerConfig()
      .setClusterKey(clusterKey)
      .setReplicationEndpointImpl(replicationEndpointClassname),
    tableCFs
  );
}
```
* This would enable an easy modification of HBase shell add_peer command to support adding a Custom Replication Endpoint peer.

## References
* https://issues.apache.org/jira/browse/HBASE-11367
* https://issues.apache.org/jira/browse/HBASE-11992
* https://issues.apache.org/jira/browse/HBASE-12254
