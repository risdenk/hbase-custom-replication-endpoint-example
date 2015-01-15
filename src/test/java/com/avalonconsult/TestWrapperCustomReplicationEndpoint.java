package com.avalonconsult;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by risdenk on 1/14/15.
 */
public class TestWrapperCustomReplicationEndpoint extends CustomReplicationEndpoint {
  static final AtomicInteger constructedCount = new AtomicInteger();
  static final AtomicInteger startedCount = new AtomicInteger();
  static final AtomicInteger stoppedCount = new AtomicInteger();
  static final AtomicInteger replicatedCount = new AtomicInteger();

  public TestWrapperCustomReplicationEndpoint() {
    super();

    constructedCount.incrementAndGet();
  }

  @Override
  protected void doStart() {
    super.doStart();

    startedCount.incrementAndGet();
  }

  @Override
  protected void doStop() {
    super.doStop();

    stoppedCount.incrementAndGet();
  }

  @Override
  public UUID getPeerUUID() {
    return super.getPeerUUID();
  }

  @Override
  public boolean replicate(ReplicateContext replicateContext) {
    boolean result = super.replicate(replicateContext);

    replicatedCount.incrementAndGet();
    return result;
  }
}
