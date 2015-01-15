package com.avalonconsult.hbase.utility;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.replication.ReplicationAdmin;
import org.apache.hadoop.hbase.replication.BaseReplicationEndpoint;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created by risdenk on 1/14/15.
 */
public class AddCustomReplicationEndpoint {
  /**
   * Adds the peer
   *
   * @throws java.io.IOException
   * @throws org.apache.hadoop.hbase.replication.ReplicationException
   */
  @VisibleForTesting
  static void addPeer(
      Configuration conf,
      String peerName,
      Class<? extends BaseReplicationEndpoint> clazz,
      Map<TableName, List<String>> tableCfs
  ) throws IOException, ReplicationException {
    /*
     * TODO
     * Is there a way to add a custom endpoint replication without the
     * ReplicationAdmin? It looks like there is no way to specify a class with
     * add_peer from the `hbase shell` or with a HBase configuration property
     */
    try (ReplicationAdmin replicationAdmin = new ReplicationAdmin(conf)) {
      ReplicationPeerConfig peerConfig = new ReplicationPeerConfig()
          .setClusterKey(ZKUtil.getZooKeeperClusterKey(conf))
          .setReplicationEndpointImpl(clazz.getName());

      replicationAdmin.addPeer(peerName, peerConfig, tableCfs);
    }
  }

  public static void main(String[] args) throws Exception {
    Options options = new Options();
    options.addOption(
        "p",
        "--peer-name",
        true,
        "HBase peer id"
    );
    options.addOption(
        "c",
        "--class-name",
        true,
        "Class name for custom replication endpoint. Must extend BaseReplicationEndpoint"
    );

    CommandLineParser parser = new BasicParser();
    CommandLine cmd = parser.parse(options, args);

    Configuration conf = HBaseConfiguration.create();

    String peerName = cmd.getOptionValue("p");
    String className = cmd.getOptionValue("c");

    Class<?> clazz = Class.forName(className);
    if (!clazz.isInstance(BaseReplicationEndpoint.class)) {
      throw new IllegalArgumentException(
          "Class: " + className + " does not extend BaseReplicationEndpoint"
      );
    }

    // TODO handle passing tableCfs
    addPeer(
        conf,
        peerName,
        clazz.asSubclass(BaseReplicationEndpoint.class),
        null
    );
  }
}
