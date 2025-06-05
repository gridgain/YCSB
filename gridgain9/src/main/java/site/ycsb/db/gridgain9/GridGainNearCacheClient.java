package site.ycsb.db.gridgain9;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.ignite.table.TableViewOptions;
import org.apache.ignite.table.Tuple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import site.ycsb.ByteIterator;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.db.ignite3.IgniteClient;

/**
 * A benchmark with enabled GG9 Near Cache on KeyValueView.
 */
public class GridGainNearCacheClient extends IgniteClient {
  /** */
  private static final Logger LOG = LogManager.getLogger(GridGainNearCacheClient.class);

  /**
   * Options to initialize table views with Near Cache enabled.
   */
  protected static TableViewOptions tableViewOptions;

  @Override
  public void initProperties(Properties properties) throws DBException {
    super.initProperties(properties);

    tableViewOptions = Utils.parseTableViewOptions(properties);
  }

  @Override
  protected void initViews() {
    for (String tableName : tableNames) {
      LOG.info("Using KV view and Record view with Near Cache for table {}", tableName);

      kvViews.add(ignite.tables().table(tableName).keyValueView(tableViewOptions));
      rViews.add(ignite.tables().table(tableName).recordView(tableViewOptions));
    }
  }

  @Override
  public Status delete(String table, String key) {
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status batchRead(String table, List<String> keys, List<Set<String>> fields, List<Map<String,
      ByteIterator>> results) {
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    try {
      Tuple tKey = Tuple.create(1).set(PRIMARY_COLUMN_NAME, key);

      // Explicit transactions with NearCache are not supported.
      Tuple tValue = getKvView(key).get(null, tKey);

      if (tValue == null) {
        return Status.NOT_FOUND;
      }

      return Status.OK;
    } catch (Exception e) {
      LOG.error("Error reading key: {}", key, e);

      return Status.ERROR;
    }
  }

  @Override
  public Status batchInsert(String table, List<String> keys, List<Map<String, ByteIterator>> values) {
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    return Status.NOT_IMPLEMENTED;
  }
}
