package com.avalonconsult;

import org.apache.hadoop.hbase.replication.BaseReplicationEndpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

/**
 * Created by risdenk on 1/14/15.
 */
public class CustomReplicationEndpoint extends BaseReplicationEndpoint {
  private static final Logger logger = LoggerFactory.getLogger(CustomReplicationEndpoint.class);

  private static final UUID uuid = UUID.randomUUID();

  public CustomReplicationEndpoint() {
    // TODO implement as required
  }

  @Override
  protected void doStart() {
    // TODO implement as required

    // Required to ensure that HBase knows the endpoint has started
    notifyStarted();
  }

  @Override
  protected void doStop() {
    // TODO implement as required

    // Required to ensure that HBase knows the endpoint has stopped
    notifyStopped();
  }

  @Override
  public UUID getPeerUUID() {
    logger.debug("peerUUID: " + uuid.toString());
    return uuid;
  }

  @Override
  public boolean replicate(ReplicateContext replicateContext) {
    logger.debug("replication count: " + replicateContext.getSize());
    logger.debug("replication entries: " + replicateContext.getEntries());

    // TODO implement as required

    return true;
  }
}
