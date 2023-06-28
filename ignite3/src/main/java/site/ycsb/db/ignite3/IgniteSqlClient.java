package site.ycsb.db.ignite3;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.ignite.sql.Session;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import site.ycsb.ByteIterator;
import site.ycsb.DBException;
import site.ycsb.Status;

/**
 * Ignite3 SQL API client.
 */
public class IgniteSqlClient extends IgniteAbstractClient {

  private static final Logger LOG = LogManager.getLogger(IgniteSqlClient.class);

  protected static Session session;

  private static final AtomicInteger SQL_INIT_COUNT = new AtomicInteger(0);

  @Override
  public void init() throws DBException {
    super.init();

    SQL_INIT_COUNT.incrementAndGet();

    synchronized (IgniteSqlClient.class) {
      if (session == null) {
        session = node.sql().createSession();
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    return Status.NOT_IMPLEMENTED;
  }

  /** {@inheritDoc} */
  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    return Status.NOT_IMPLEMENTED;
  }

  /** {@inheritDoc} */
  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    try {
      List<String> columns = new ArrayList<>(Collections.singletonList(PRIMARY_COLUMN_NAME));
      List<String> insertValues = new ArrayList<>(Collections.singletonList(key));

      for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
        columns.add(entry.getKey());
        insertValues.add(entry.getValue().toString());
      }

      String columnsString = columns.stream()
          .map(e -> "'" + e + "'")
          .collect(Collectors.joining(", "));
      String valuesString = insertValues.stream()
          .map(e -> "'" + e + "'")
          .collect(Collectors.joining(", "));

      String insertStatement = String.format("INSERT INTO %s (%s) VALUES (%s);", table, columnsString, valuesString);

      if (table.equals(cacheName)) {
        if (debug) {
          LOG.info(insertStatement);
        }

        session.execute(null, insertStatement).close();
      } else {
        throw new UnsupportedOperationException("Unexpected table name: " + table);
      }

      return Status.OK;
    } catch (Exception e) {
      LOG.error(String.format("Error inserting key: %s", key), e);

      return Status.ERROR;
    }
  }

  /** {@inheritDoc} */
  @Override
  public Status delete(String table, String key) {
    String deleteStatement = String.format(
        "DELETE FROM %s WHERE %S = %S", table, PRIMARY_COLUMN_NAME, key
    );

    try {
      if (debug) {
        LOG.info(deleteStatement);
      }

      session.execute(null, deleteStatement);

      return Status.OK;
    } catch (Exception e) {
      LOG.error(String.format("Error deleting key: %s ", key), e);
    }

    return Status.ERROR;
  }

  @Override
  public void cleanup() throws DBException {
    synchronized (IgniteSqlClient.class) {
      int currInitCount = SQL_INIT_COUNT.decrementAndGet();

      if (currInitCount <= 0) {
        try {
          session.close();
          session = null;
        } catch (Exception e) {
          throw new DBException(e);
        }
      }
    }

    super.cleanup();
  }
}
