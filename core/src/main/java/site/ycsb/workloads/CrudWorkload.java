package site.ycsb.workloads;

import java.util.HashMap;
import site.ycsb.ByteIterator;
import site.ycsb.DB;

/**
 * Workload that processes sequences of CRUD operations.
 */
public class CrudWorkload extends CoreWorkload {
  /**
   * Perform a sequence of CRUD operations.
   */
  @Override
  public boolean doInsert(DB db, Object threadstate) {
    CoreWorkloadThreadState threadState = (CoreWorkloadThreadState) threadstate;

    long keynum = keysequence.nextValue().longValue();

    doTransactionInsert(db, threadState, keynum);
    doTransactionRead(db, threadState, keynum);
    doTransactionUpdate(db, threadState, keynum);
    doTransactionDelete(db, threadState, keynum);

    threadState.incrementOps();

    return true;
  }

  /** {@inheritDoc} */
  @Override
  public void doTransactionInsert(DB db, CoreWorkloadThreadState threadState, long keynum) {
    String dbkey = CoreWorkload.buildKeyName(keynum, zeropadding, orderedinserts);

    HashMap<String, ByteIterator> values = buildValues(dbkey);

    if (!isBatched) {
      db.insert(table, dbkey, values);
    } else {
      threadState.getBatchKeysList().add(dbkey);
      threadState.getBatchValuesList().add(values);

      if (threadState.isBatchPrepared()) {
        db.batchInsert(table, threadState.getBatchKeysList(), threadState.getBatchValuesList());
        threadState.getBatchKeysList().clear();
        threadState.getBatchValuesList().clear();
      }
    }
  }

  /**
   * Performs a delete transaction.
   *
   * @param db Database.
   * @param threadState Thread state.
   * @param keynum Keynum.
   */
  public void doTransactionDelete(DB db, CoreWorkloadThreadState threadState, long keynum) {
    String keyname = CoreWorkload.buildKeyName(keynum, zeropadding, orderedinserts);

    db.delete(table, keyname);
  }
}
