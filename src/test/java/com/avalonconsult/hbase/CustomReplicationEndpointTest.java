package com.avalonconsult.hbase;

import com.avalonconsult.hbase.utility.AddCustomReplicationEndpoint;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.replication.ReplicationAdmin;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class CustomReplicationEndpointTest {
  private static final int WAIT_TIMEOUT = 60000;

  private static final String PEER_NAME = "customreplicationendpoint";

  private static final TableName TABLE_NAME = TableName.valueOf("test");
  private static final String ROWKEY = "rk";
  private static final byte[] ROWKEY_BYTES = Bytes.toBytes(ROWKEY);
  private static final String COLUMN_FAMILY = "cf";
  private static final byte[] COLUMN_FAMILY_BYTES = Bytes.toBytes(COLUMN_FAMILY);
  private static final String QUANTIFIER = "q";
  private static final byte[] QUANTIFIER_BYTES = Bytes.toBytes(QUANTIFIER);
  private static final String VALUE = "v";
  private static final byte[] VALUE_BYTES = Bytes.toBytes(VALUE);

  private HBaseTestingUtility utility;
  private int numRegionServers;

  @Before
  public void setUp() throws Exception {
    Configuration hbaseConf = HBaseConfiguration.create();

    // Configuration settings taken from TestReplicationBase for testing
    hbaseConf.setFloat("hbase.regionserver.logroll.multiplier", 0.0003f);
    hbaseConf.setInt("replication.source.size.capacity", 10240);
    hbaseConf.setLong("replication.source.sleepforretries", 100);
    hbaseConf.setInt("hbase.regionserver.maxlogs", 10);
    hbaseConf.setLong("hbase.master.logcleaner.ttl", 10);
    hbaseConf.setInt("zookeeper.recovery.retry", 1);
    hbaseConf.setInt("zookeeper.recovery.retry.intervalmill", 10);
    hbaseConf.setBoolean("dfs.support.append", true);
    hbaseConf.setLong(HConstants.THREAD_WAKE_FREQUENCY, 100);
    hbaseConf.setInt("replication.stats.thread.period.seconds", 5);
    hbaseConf.setBoolean("hbase.tests.use.shortcircuit.reads", false);
    hbaseConf.setLong("replication.sleep.before.failover", 2000);
    hbaseConf.setInt("replication.source.maxretriesmultiplier", 10);

    // Ensure that replication is enabled
    hbaseConf.setBoolean(HConstants.REPLICATION_ENABLE_KEY, true);

    utility = new HBaseTestingUtility(hbaseConf);
    utility.startMiniCluster();

    numRegionServers = utility.getHBaseCluster().getRegionServerThreads().size();
  }

  /**
   * Adds the peer
   * @throws IOException
   * @throws ReplicationException
   */
  private void addPeer() throws IOException, ReplicationException {
    Map<TableName, List<String>> tableCfs = new HashMap<>();
    List<String> cfs = new ArrayList<>();
    cfs.add(COLUMN_FAMILY);
    tableCfs.put(TABLE_NAME, cfs);

    AddCustomReplicationEndpoint.addPeer(utility.getConfiguration(), PEER_NAME, TestWrapperCustomReplicationEndpoint.class, tableCfs);
  }

  /**
   * Check whether the class has been constructed and started
   * @throws Exception
   */
  private void waitForCustomReplicationEndpointCreation() throws Exception {
    Waiter.waitFor(utility.getConfiguration(), WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return TestWrapperCustomReplicationEndpoint.constructedCount.get() == numRegionServers;
      }
    });

    Waiter.waitFor(utility.getConfiguration(), WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return TestWrapperCustomReplicationEndpoint.startedCount.get() == numRegionServers;
      }
    });
  }

  /**
   * Creates a table and ensures it has replication enabled
   * @throws IOException
   */
  private void createTestTable() throws IOException {
    try(HBaseAdmin hBaseAdmin = utility.getHBaseAdmin()) {
      HTableDescriptor hTableDescriptor = new HTableDescriptor(TABLE_NAME);
      HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(COLUMN_FAMILY);

      // Ensure that replication is enabled for column family
      hColumnDescriptor.setScope(HConstants.REPLICATION_SCOPE_GLOBAL);

      hTableDescriptor.addFamily(hColumnDescriptor);
      hBaseAdmin.createTable(hTableDescriptor);
    }

    utility.waitUntilAllRegionsAssigned(TABLE_NAME);
  }

  /**
   * Adds data to the previously created HBase table
   * @throws IOException
   */
  private void addData() throws IOException {
    try(HTable hTable = new HTable(utility.getConfiguration(), TABLE_NAME)) {
      Put put = new Put(ROWKEY_BYTES);
      put.add(COLUMN_FAMILY_BYTES, QUANTIFIER_BYTES, VALUE_BYTES);
      hTable.put(put);
    }
  }

  /**
   * Wait and check to make sure that replication has occurred
   * @throws Exception
   */
  private void waitForCustomReplicationEndpointReplication() throws Exception {
    Waiter.waitFor(utility.getConfiguration(), WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return TestWrapperCustomReplicationEndpoint.replicatedCount.get() == numRegionServers;
      }
    });
  }

  /**
   * Removes the peer
   * @throws IOException
   * @throws ReplicationException
   */
  private void removePeer() throws IOException, ReplicationException {
    try(ReplicationAdmin replicationAdmin = new ReplicationAdmin(utility.getConfiguration())) {
      replicationAdmin.removePeer(PEER_NAME);
    }

  }

  /**
   * Wait and check to make sure that shutdown has occurred
   * @throws Exception
   */
  private void waitForCustomReplicationEndpointShutdown() throws Exception {
    Waiter.waitFor(utility.getConfiguration(), WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return TestWrapperCustomReplicationEndpoint.stoppedCount.get() == numRegionServers;
      }
    });
  }

  @Test
  public void testCustomReplicationEndpoint() throws Exception {
    // Initialize counts to 0 before test
    TestWrapperCustomReplicationEndpoint.constructedCount.set(0);
    TestWrapperCustomReplicationEndpoint.startedCount.set(0);
    TestWrapperCustomReplicationEndpoint.stoppedCount.set(0);
    TestWrapperCustomReplicationEndpoint.replicatedCount.set(0);

    addPeer();

    waitForCustomReplicationEndpointCreation();

    assertEquals(numRegionServers, TestWrapperCustomReplicationEndpoint.constructedCount.get());
    assertEquals(numRegionServers, TestWrapperCustomReplicationEndpoint.startedCount.get());

    createTestTable();

    assertEquals(0, TestWrapperCustomReplicationEndpoint.replicatedCount.get());

    addData();

    waitForCustomReplicationEndpointReplication();

    assertEquals(numRegionServers, TestWrapperCustomReplicationEndpoint.replicatedCount.get());

    removePeer();

    waitForCustomReplicationEndpointShutdown();

    assertEquals(numRegionServers, TestWrapperCustomReplicationEndpoint.stoppedCount.get());
  }

  @After
  public void tearDown() throws Exception {
    if(utility != null) {
      utility.shutdownMiniCluster();
    }
  }
}