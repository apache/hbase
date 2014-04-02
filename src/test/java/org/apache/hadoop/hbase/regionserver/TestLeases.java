package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.*;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.LeaseException;
import org.apache.hadoop.hbase.LeaseListener;
import org.apache.hadoop.hbase.Leases;
import org.apache.hadoop.hbase.Leases.LeaseStillHeldException;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.Test;

public class TestLeases {
  private static final Log LOG = LogFactory.getLog(TestLeases.class);
  private static final int NUM_CALLS = 100;
  private static final int MAX_WAIT = 4;
  private Leases leases;
  private final int leasePeriod = 50; // ms
  private final long wakeFreq = 1; // ms

  @Test
  public void test()
      throws LeaseException, InterruptedException, ExecutionException {
    int cnt = 0;
    for (int i = 0; i < 20; i++) {
      if (testOneInstance()) cnt++;
    }
  }

  public boolean testOneInstance()
      throws LeaseException, InterruptedException, ExecutionException {
    final AtomicInteger expiredLeaseCnt = new AtomicInteger(0);
    final AtomicInteger cancelledLeaseCnt = new AtomicInteger(0);
    LOG.debug(String.format("Creating leases with lease period : %d, wake frequency : %d",
        leasePeriod, (int)wakeFreq));

    this.leases = new Leases(leasePeriod, wakeFreq);
    leases.setDaemon(true);
    leases.setName("Lease Thread");
    leases.start();

    // Simulating a bunch of add scanner calls
    final Random rand = new Random();
    final Random rand2 = new Random();
    final Map<Integer, Boolean> leaseIds = new ConcurrentHashMap<Integer, Boolean>();
    int numLeasesCreated = 0;
    for (int i = 0; i < NUM_CALLS; i++) {
      int leaseId = rand2.nextInt();
      try {
        leases.createLease(String.valueOf(leaseId),
            new MockLeaseListener(String.valueOf(leaseId), expiredLeaseCnt));
        numLeasesCreated++;
        leaseIds.put(leaseId, true);

        // Testing the LeaseStillHeldException case on one of the attempts.
        if (numLeasesCreated == NUM_CALLS/2) {
          try {
            leases.createLease(String.valueOf(leaseId),
                new MockLeaseListener(String.valueOf(leaseId), expiredLeaseCnt));
          } catch (LeaseStillHeldException e) {
            // It works
            LOG.debug("Inserting duplicate lease id resulted in " +
                "LeaseStillHeldException");
            continue;
          }
          assertTrue("Duplicate attempt of lease creation.", false);
        }

        // Testing the
        if (numLeasesCreated == NUM_CALLS/3) {
          try {
            leases.renewLease("invalid_lease_id");
          } catch (LeaseException e) {
            // It works
            LOG.debug("Renewing invalid id throws " +
                "LeaseException");
            continue;
          }
          assertTrue("Duplicate attempt of lease creation.", false);
        }
      } catch (LeaseStillHeldException e) {
        continue;
      }
    }

    for (int i = 0; i < NUM_CALLS / 2; i++) {
      final int waitTime = rand.nextInt(MAX_WAIT * 10);
      Threads.sleep(waitTime);
      int idx = rand.nextInt(leaseIds.size());
      int leaseId = leaseIds.keySet().toArray(new Integer[0])[idx];
      if (idx % 2 == 0) {
        try {
          leases.cancelLease(String.valueOf(leaseId));
          LOG.debug("Lease cancelled :" + leaseId);
        } catch (LeaseException e) {
          continue;
        } catch (Exception e) {
          e.printStackTrace();
        }
        cancelledLeaseCnt.addAndGet(1);
        leaseIds.remove(leaseId);
      } else {
        try {
          leases.renewLease(String.valueOf(leaseId));
        } catch (LeaseException e) {
        }
      }
    }

    // wait for leases to drain all the leases.
    leases.closeAfterLeasesExpire();
    leases.join();
    LOG.debug("LeaseIds.size() : " + leaseIds.size() +
        ", expiredLeaseCnt.get() : " + expiredLeaseCnt.get() +
        ", cancelledLeaseCnt.get() : " + cancelledLeaseCnt.get() +
        ", numLeasesCreated : " + numLeasesCreated);
    assertTrue("LeaseIds.size() : " + leaseIds.size() +
        ", expiredLeaseCnt.get() : " + expiredLeaseCnt.get() +
        ", cancelledLeaseCnt.get() : " + cancelledLeaseCnt.get() +
        ", numLeasesCreated : " + numLeasesCreated,
        expiredLeaseCnt.get() + cancelledLeaseCnt.get() >=
          numLeasesCreated);
    if (expiredLeaseCnt.get() + cancelledLeaseCnt.get() >
          numLeasesCreated) {
      // This is the case where the ConcurrentHashMap gives unpredicatable
      // behavior because we aren't guarding readers and writers from each other
      return false;
    }
    return true;
  }

  private static class MockLeaseListener extends LeaseListener {
    private final AtomicInteger leaseCnt;
    private final String leaseName_visible;
    MockLeaseListener(String leaseName, AtomicInteger leaseCnt) {
      super(leaseName);
      leaseName_visible = leaseName;
      this.leaseCnt = leaseCnt;
    }

    @Override
    public void leaseExpired() {
      LOG.debug("Expiring lease : " + leaseName_visible );
      this.leaseCnt.addAndGet(1);
    }
  }
}
