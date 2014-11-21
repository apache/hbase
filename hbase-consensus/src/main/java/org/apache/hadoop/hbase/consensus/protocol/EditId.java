package org.apache.hadoop.hbase.consensus.protocol;

import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;
import org.apache.hadoop.hbase.HConstants;

@ThriftStruct
public final class EditId implements Comparable<EditId> {

  private final long term;
  private final long index;

  @ThriftConstructor
  public EditId(
    @ThriftField(1) final long term,
    @ThriftField(2) final long index) {
    this.term = term;
    this.index = index;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof EditId)) {
      return false;
    }

    EditId editId = (EditId) o;

    if (index != editId.index || term != editId.term) {
      return false;
    }

    return true;
  }

  @ThriftField(1)
  public long getTerm() {
    return term;
  }

  @ThriftField(2)
  public long getIndex() {
    return index;
  }

  @Override
  public int compareTo(EditId o) {

    if (this.term < o.term) {
      return -1;
    } else if (this.term == o.term) {
      if (this.index < o.index) {
        return -1;
      } else if (this.index > o.index) {
        return 1;
      }
      return 0;
    }

    return 1;
  }

  public static EditId getElectionEditID(EditId current, int termDelta, int indexDelta) {

    long currentTerm = current.getTerm();

    // if the current term is the seed term (-2) then set it to undefined (-1)
    // so that the new election term is >= 0. This is assuming that the termDelta
    // is +ve, which should always be the case as use minimum rank as 1
    if (currentTerm == HConstants.SEED_TERM) {
      currentTerm = HConstants.UNDEFINED_TERM_INDEX;
    }

    return new EditId(currentTerm + termDelta , current.getIndex() + indexDelta);
  }

  public static EditId getNewAppendEditID(final EditId current) {
    return new EditId(current.getTerm(), current.getIndex() + 1);
  }

  @Override
  public String toString() {
    return "{term = " + term + ", index = " + index + "}";
  }

  @Override
  public EditId clone() {
    return new EditId(term, index);
  }

  @Override
  public int hashCode() {
    int result = (int) (term ^ (term >>> 32));
    result = 31 * result + (int) (index ^ (index >>> 32));
    return result;
  }
}
