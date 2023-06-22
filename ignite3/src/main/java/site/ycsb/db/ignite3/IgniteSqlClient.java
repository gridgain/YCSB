package site.ycsb.db.ignite3;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.sql.Session;
import org.apache.ignite.sql.Statement;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import site.ycsb.ByteIterator;
import site.ycsb.Status;

public class IgniteSqlClient extends IgniteAbstractClient {

  private static final Logger LOG = LogManager.getLogger(IgniteSqlClient.class);

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
      List<String> columns = Arrays.asList(PRIMARY_COLUMN_NAME);
      List<String> insertValues = Arrays.asList(key);

      for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
        columns.add(entry.getKey());
        insertValues.add(entry.getValue().toString());
      }

      String insertStatement = "INSERT INTO" + table + "(" +
          String.join(", ", columns) + ") VALUES (" +
          String.join(", ", insertValues) + ");";

      if (table.equals(cacheName)) {
        // TODO: reuse a single session object
        try (Session ses = node.sql().createSession()) {
          if (debug) {
            LOG.info(insertStatement);
          }

          ses.execute(null, insertStatement).close();
        }
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
      try (Session ses = node.sql().createSession()) {
        if (debug) {
          LOG.info(deleteStatement);
        }

        ses.execute(null, deleteStatement);
      }

      return Status.OK;
    } catch (Exception e) {
      LOG.error(String.format("Error deleting key: %s ", key), e);
    }

    return Status.ERROR;
  }
}
