package com.avalonconsult.hbase.utility;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.replication.ReplicationAdmin;
import org.apache.hadoop.hbase.replication.ReplicationEndpoint;
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
  public static void addPeer(
      Configuration conf,
      String peerName,
      Class<? extends ReplicationEndpoint> clazz,
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
    Option peerNameOption = OptionBuilder
        .withLongOpt("peer-name")
        .hasArg()
        .withArgName("peer-name")
        .isRequired()
        .withDescription("HBase peer id")
        .create("p");

    Option classNameOption = OptionBuilder
        .withLongOpt("class-name")
        .hasArg()
        .withArgName("class-name")
        .isRequired()
        .withDescription(
            "Class name for custom replication endpoint. The class must be " +
                "on the classpath and must extend BaseReplicationEndpoint")
        .create("c");

    Options options = new Options();
    options.addOption(peerNameOption);
    options.addOption(classNameOption);

    CommandLineParser parser = new BasicParser();
    CommandLine cmd;
    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("AddCustomReplicationEndpoint", options);
      System.exit(1);
      return;
    }

    Configuration conf = HBaseConfiguration.create();

    String peerName = cmd.getOptionValue("p");
    String className = cmd.getOptionValue("c");

    /*
     * TODO handle tableCfs
     * Can be handled with `hbase shell` command set_peer_tableCFs
     * Defaulting to null which will be all column families
     */
    Map<TableName, List<String>> tableCfs = null;

    addPeer(
        conf,
        peerName,
        Class.forName(className).asSubclass(ReplicationEndpoint.class),
        tableCfs
    );
  }
}
