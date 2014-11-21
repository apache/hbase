package org.apache.hadoop.hbase.consensus.server;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.hbase.consensus.rpc.AppendRequest;
import org.apache.hadoop.hbase.consensus.rpc.AppendResponse;
import org.apache.hadoop.hbase.consensus.rpc.VoteRequest;
import org.apache.hadoop.hbase.consensus.rpc.VoteResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MarkerFactory;

import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class InstrumentedConsensusServiceImpl extends ConsensusServiceImpl {
  private final Logger LOG = LoggerFactory.getLogger(InstrumentedConsensusServiceImpl.class);

  private ConcurrentHashMap<String, Double> packetDropRateMap = new ConcurrentHashMap<String, Double>();
  private ConcurrentHashMap<String, Long> packetDelayMap = new ConcurrentHashMap<String, Long>();
  private ConcurrentHashMap<String, Boolean> hiccupMap = new ConcurrentHashMap<String, Boolean>();
  private Random random = new Random(System.currentTimeMillis());
  private RelayThread relayThread;
  private String identity = "None";
  private AtomicLong normalPacketDropCount = new AtomicLong(0);
  private AtomicLong hiccupPacketDropCount = new AtomicLong(0);

  protected InstrumentedConsensusServiceImpl() {
    relayThread = new RelayThread(this);
    relayThread.start();
  }

  public synchronized void setIdentity(String str) {
    identity = str;
  }

  public synchronized String getIdentity() {
    return identity;
  }

  public enum PacketDropStyle {
    NONE,
    ALWAYS,
    RANDOM
  };

  public class RelayThread extends Thread {
    private InstrumentedConsensusServiceImpl cs;
    private DelayQueue<DelayedRequest> queue = new DelayQueue<DelayedRequest>();

    public class DelayedRequest implements Delayed {
      private long    deadline;
      private Object  request;

      public DelayedRequest(Object request, long delay) {
        deadline = delay + System.currentTimeMillis();
        this.request = request;
      }

      @Override
      public long getDelay(TimeUnit unit) {
        return unit.convert(deadline - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
      }

      public long getDeadline() {
        return deadline;
      }

      public int compareTo(Delayed other) {
        long diff = getDelay(TimeUnit.MILLISECONDS) - other.getDelay(TimeUnit.MILLISECONDS);
        return diff < 0 ? -1 : (diff > 0 ? +1 : 0);
      }

      public Object request() {
        return request;
      }
    }

    public RelayThread(InstrumentedConsensusServiceImpl cs) {
      this.cs = cs;
    }

    public void run() {
      while (true) {
        try {
          DelayedRequest request = queue.take();
          LOG.info("-------- [" + getIdentity() + "] draining request " + request.request());
          if (request.request() instanceof AppendRequest) {
            cs.reallyAppendEntries((AppendRequest) request.request());
          } else if (request.request() instanceof VoteRequest) {
            cs.reallyRequestVote((VoteRequest) request.request());
          } else {
            LOG.error(MarkerFactory.getMarker("FATAL"),
                    "Incorrect request type found : " + request.request());
            System.exit(1);
          }
        } catch (InterruptedException ex) {
          LOG.warn("RelayThread is interrupted; time to die!");
          return;
        } catch (Exception ex) {
          LOG.warn("Caught exception:\n" + ex);
        }
      }
    }

    public <T extends Object> T queueRequest(T request, long delay) {
      try {
        LOG.info("-------- [" + getIdentity() + "] queueing request " + request + " with delay " + delay + " ms");
        queue.offer(new DelayedRequest((Object)request, delay));
      } catch (NullPointerException ex) {
      }
      return request;
    }
  }

  public void setPacketDelay(long delay) {
    packetDelayMap.clear();
    packetDelayMap.put("*", delay);
  }

  public void setPacketDelay(String src, long delay) {
    packetDelayMap.put(src, delay);
  }

  public long getPacketDelay(String src) {
    Long delay = packetDelayMap.get(src);
    if (delay == null) {
      delay = packetDelayMap.get("*");
      if (delay == null) {
        return 0L;
      } else {
        return delay;
      }
    } else {
      return delay;
    }
  }

  public synchronized void setHiccup(
      final String src, boolean inHiccup
  ) {
    hiccupMap.put(src, inHiccup);
  }

  public synchronized boolean inHiccup(final String src) {
    Boolean inHiccup = hiccupMap.get(src);
    return inHiccup != null ? (boolean) inHiccup : false;
  }

  public synchronized void setPacketDropRate(
      final String src, final double rate
  ) {
    packetDropRateMap.put(src, rate);
  }

  public synchronized void setPacketDropStyle(final PacketDropStyle style) {
    packetDropRateMap.clear();
    if (style == null) {
      return;
    }
    switch (style) {
      case ALWAYS:
        packetDropRateMap.put("*", 2.0);
        break;
      case RANDOM:
        packetDropRateMap.put("*", 0.5);
        break;
      case NONE:
        packetDropRateMap.put("*", 0.0);
        break;
    }
  }

  private Double getPacketDropRate(String src) {
    Double dropRate = packetDropRateMap.get(src);
    if (dropRate == null) {
      return packetDropRateMap.get("*");
    } else {
      return dropRate;
    }
  }

  public ListenableFuture<AppendResponse> reallyAppendEntries(AppendRequest appendRequest) {
    return super.appendEntries(appendRequest);
  }

  public ListenableFuture<VoteResponse> reallyRequestVote(VoteRequest voteRequest) {
    return super.requestVote(voteRequest);
  }

  public long getNormalPacketDropCount() {
    return normalPacketDropCount.get();
  }

  public long getHiccupPacketDropCount() {
    return hiccupPacketDropCount.get();
  }

  public long getPacketDropCount() {
    return getHiccupPacketDropCount() + getNormalPacketDropCount();
  }

  @Override
  public ListenableFuture<AppendResponse> appendEntries(AppendRequest appendRequest) {
    appendRequest.createAppendResponse();
    String src = appendRequest.getLeaderId().getHostId();
    if (inHiccup(src)) {
        LOG.debug("-------- [" + getIdentity() + "] Dropping packet due to hiccup: " + appendRequest);
        hiccupPacketDropCount.incrementAndGet();
        return appendRequest.getResponse();
    }
    Double dropRate = getPacketDropRate(src);
    if (dropRate != null && random.nextDouble() < dropRate) {
        LOG.debug("-------- [" + getIdentity() + "] Dropping packet " + appendRequest);
        normalPacketDropCount.incrementAndGet();
        return appendRequest.getResponse();
    }
    return relayThread.queueRequest(appendRequest, getPacketDelay(src)).getResponse();
  }

  @Override
  public ListenableFuture<VoteResponse> requestVote(VoteRequest voteRequest) {
    voteRequest.createVoteResponse();
    String src = voteRequest.getAddress();
    if (inHiccup(src)) {
        LOG.debug("-------- [" + getIdentity() + "] Dropping packet due to hiccup: " + voteRequest);
        hiccupPacketDropCount.incrementAndGet();
        return voteRequest.getResponse();
    }
    Double dropRate = getPacketDropRate(src);
    if (dropRate != null && random.nextDouble() < dropRate) {
        normalPacketDropCount.incrementAndGet();
        LOG.debug("-------- [" + getIdentity() + "] Dropping packet " + voteRequest);
        return voteRequest.getResponse();
    }
    return relayThread.queueRequest(voteRequest, getPacketDelay(src)).getResponse();
  }
}
