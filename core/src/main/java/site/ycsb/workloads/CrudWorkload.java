package site.ycsb.workloads;

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
