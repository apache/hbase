package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.hadoop.hbase.zookeeper.ZooKeeperWrapper;
import org.apache.zookeeper.KeeperException;
import org.junit.Test;

public class TestClusterStartupDetection extends MultiMasterTest {

  /**
   * Testing the master's ability to distinguish between a fresh cluster
   * startup and master failover. This is a simple restart of one master.
   */
  @Test(timeout=240000)
  public void testStartupDetectionSimple()
      throws IOException, InterruptedException, KeeperException {
    ZooKeeperWrapper.setNamespaceForTesting();
    header("Starting simple cluster startup detection test");
    final int numMasters = 1;
    final int numRS = 2;

    startMiniCluster(numMasters, numRS);
    ensureMastersAreUp(numMasters);
    assertTrue(miniCluster().getMaster().isClusterStartup());
    final int activeMaster = getActiveMasterIndex();

    shortSleep();

    assertEquals(activeMaster, getActiveMasterIndex());

    header("Killing the master");
    miniCluster().killActiveMaster();
    localCluster().waitOnMasterStop(0);

    killRegionServerWithMeta();

    header("Starting a new master and verifying that a restart is identified");
    HMaster newMaster = miniCluster().startNewMaster();

    assertFalse("Incorrectly identified a cluster restart as a fresh " +
        "cluster startup", newMaster.isClusterStartup());
    waitUntilRegionServersCheckIn(numRS - 1);
  }

  @Test(timeout=240000)
  public void testStartupDetectionOnMasterDelay() throws IOException,
      InterruptedException {
    ZooKeeperWrapper.setNamespaceForTesting();
    header("Starting cluster startup detection test on delayed " +
        "second master startup");
    final int numMasters = 1;
    final int numRS = 1;

    startMiniCluster(numMasters, numRS);

    header("Verifying that the cluster has come up");
    ensureMastersAreUp(numMasters);
    assertTrue(miniCluster().getMaster().isClusterStartup());
    shortSleep();

    // Start another master. This one should not think this is a fresh start.
    header("Starting new master after a delay");
    final HMaster newMaster = miniCluster().startNewMaster();
    assertFalse("The second master started after a delay thinks this is a " +
        "fresh cluster startup", newMaster.isClusterStartup());
    waitUntilRegionServersCheckIn(numRS);
  }

  @Test(timeout=240000)
  public void testStartupDetectionOnMasterFailover() throws IOException,
      InterruptedException {
    ZooKeeperWrapper.setNamespaceForTesting();
    header("Starting cluster startup detection test on master failover");
    final int numMasters = 2;
    final int numRS = 2;

    startMiniCluster(numMasters, numRS);
    ensureMastersAreUp(numMasters);

    // Initially all masters should think that this is a fresh cluster startup.
    for (HMaster master : miniCluster().getMasters()) {
      assertTrue(master.isClusterStartup());
    }

    String oldActiveName;
    {
      final int activeIndex = getActiveMasterIndex();
      final HMaster activeMaster = miniCluster().getMasters().get(activeIndex);
      header("Killing master");
      oldActiveName = activeMaster.getServerName();
      activeMaster.stop("killing master");
      localCluster().waitOnMasterStop(activeIndex);
    }

    assertTrue(miniCluster().waitForActiveAndReadyMaster());
    final int newActiveIndex = getActiveMasterIndex();
    final HMaster newActiveMaster =
        miniCluster().getMasters().get(newActiveIndex);

    assertTrue("Expected the new active master to be different but got " +
        oldActiveName + " again",
        !oldActiveName.equals(newActiveMaster.getName()));

    // The new active master must be aware that this is not a fresh cluster
    // startup anymore.
    assertFalse("The new active master incorrectly thinks that this is a " +
        "fresh cluster startup.", newActiveMaster.isClusterStartup());
    waitUntilRegionServersCheckIn(numRS);
  }

}
