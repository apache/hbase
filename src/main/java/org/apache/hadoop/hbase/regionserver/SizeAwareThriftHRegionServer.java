package org.apache.hadoop.hbase.regionserver;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.MultiAction;
import org.apache.hadoop.hbase.client.MultiPut;
import org.apache.hadoop.hbase.client.MultiPutResponse;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.TMultiResponse;

import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class SizeAwareThriftHRegionServer extends ThriftHRegionServer {

  private final AtomicLong writeQueueSizebytes = new AtomicLong(0);
  private final long maxWriteSize;

  public SizeAwareThriftHRegionServer(HRegionServer server) {
    super(server);
    this.maxWriteSize = server.getConfiguration().getLong(
        HConstants.MAX_CALL_QUEUE_MEMORY_SIZE_STRING, HConstants.MAX_CALL_QUEUE_MEMORY_SIZE);
  }

  @Override
  public ListenableFuture<Void> put(final byte[] regionName, final Put put) {
    if (writeQueueSizebytes.get() < maxWriteSize) {
      final long heapSize = put.heapSize();
      writeQueueSizebytes.addAndGet(heapSize);
      ListenableFuture<Void> f = super.put(regionName, put);
      Futures.addCallback(f, new DecrementWriteSizeCallback(heapSize));
      return f;
    } else {
      return Futures.immediateFailedFuture(new RegionOverloadedException());
    }
  }

  @Override
  public ListenableFuture<Integer> putRows(final byte[] regionName, final List<Put> puts) {
    if (writeQueueSizebytes.get() < maxWriteSize) {
      long heapSize = 0;
      for (Put p : puts) {
        heapSize += p.heapSize();
      }
      writeQueueSizebytes.addAndGet(heapSize);
      ListenableFuture<Integer> f = super.putRows(regionName, puts);
      Futures.addCallback(f, new DecrementWriteSizeCallback(heapSize));
      return f;
    } else {
      return Futures.immediateFailedFuture(new RegionOverloadedException());
    }
  }

  @Override
  public ListenableFuture<TMultiResponse> multiAction(final MultiAction multi) {
    if (writeQueueSizebytes.get() < maxWriteSize) {
      long heapSize = 0;
      if (multi.getPuts() != null) {
        for (List<Put> puts : multi.getPuts().values()) {
          for (Put p : puts) {
            heapSize += p.heapSize();
          }
        }
      }
      if (multi.getDeletes() != null) {
        for (List<Delete> deletes:multi.getDeletes().values()) {
          for (Delete d:deletes) {
            heapSize += d.getRow().length * d.size();
          }
        }
      }

      writeQueueSizebytes.addAndGet(heapSize);
      ListenableFuture<TMultiResponse> f = super.multiAction(multi);
      Futures.addCallback(f, new DecrementWriteSizeCallback(heapSize));
      return f;
    } else {
      return Futures.immediateFailedFuture(new RegionOverloadedException());
    }
  }

  @Override
  public ListenableFuture<MultiPutResponse> multiPut(final MultiPut puts) {
    if (writeQueueSizebytes.get() < maxWriteSize) {
      long heapSize = 0;
      for (List<Put> putList : puts.getPuts().values()) {
        for (Put p : putList) {
          heapSize += p.heapSize();
        }
      }
      writeQueueSizebytes.addAndGet(heapSize);
      ListenableFuture<MultiPutResponse> f = super.multiPut(puts);
      Futures.addCallback(f, new DecrementWriteSizeCallback(heapSize));
      return f;
    } else {
      return Futures.immediateFailedFuture(new RegionOverloadedException());
    }
  }

  @Override
  public ListenableFuture<Void> processDelete(final byte[] regionName, final Delete delete)  {
    if (writeQueueSizebytes.get() < maxWriteSize) {
      long heapSize = delete.size() * delete.getRow().length;
      writeQueueSizebytes.addAndGet(heapSize);
      ListenableFuture<Void> f = super.processDelete(regionName, delete);
      Futures.addCallback(f, new DecrementWriteSizeCallback(heapSize));
      return f;
    } else {
      return Futures.immediateFailedFuture(new RegionOverloadedException());
    }
  }

  @Override
  public ListenableFuture<Integer> processListOfDeletes(final byte[] regionName, final List<Delete> deletes) {
    if (writeQueueSizebytes.get() < maxWriteSize) {
      long heapSize = 0;
      for (Delete d:deletes) {
        heapSize += d.size() * d.getRow().length;
      }
      writeQueueSizebytes.addAndGet(heapSize);
      ListenableFuture<Integer> f = super.processListOfDeletes(regionName, deletes);
      Futures.addCallback(f, new DecrementWriteSizeCallback(heapSize));
      return f;
    } else {
      return Futures.immediateFailedFuture(new RegionOverloadedException());
    }
  }

  private class DecrementWriteSizeCallback<V> implements FutureCallback<V> {
    private final long heapSize;

    public DecrementWriteSizeCallback(long heapSize) {
      this.heapSize = heapSize;
    }

    @Override public void onSuccess(@Nullable V aVoid) {
      writeQueueSizebytes.addAndGet(-1 * heapSize);
    }

    @Override public void onFailure(Throwable throwable) {
      writeQueueSizebytes.addAndGet(-1 * heapSize);
    }
  }
}
