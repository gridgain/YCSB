package site.ycsb.db.ignite3;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import site.ycsb.ByteIterator;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

/**
 * Ignite3 JDBC client.
 */
public class IgniteJdbcClient extends AbstractSqlClient {
  public static final Logger LOG = LogManager.getLogger(IgniteJdbcClient.class);

  /**
   * Use separate connection per thread since sharing a single Connection object is not recommended.
   */
  private static final ThreadLocal<Connection> CONN = new ThreadLocal<>();

  /** {@inheritDoc} */
  @Override
  public void init() throws DBException {
    super.init();

    if (hosts == null) {
      throw new DBException(String.format(
          "Required property \"%s\" missing for Ignite Cluster",
          HOSTS_PROPERTY));
    }

    if (hosts.contains(",")) {
      throw new DBException("JDBC supports only 1 server host");
    }

    String url = "jdbc:ignite:thin://" + hosts + (hosts.contains(":") ? "" : ":" + DEFAULT_PORT);
    try {
      CONN.set(DriverManager.getConnection(url));
    } catch (Exception e) {
      throw new DBException(e);
    }
  }

  /** {@inheritDoc} */
  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    try {
      PreparedStatement stmt = prepareReadStatement(CONN.get(), table);

      stmt.setString(1, key);

      try (ResultSet rs = stmt.executeQuery()) {
        if (!rs.next()) {
          return Status.NOT_FOUND;
        }

        if (fields == null || fields.isEmpty()) {
          fields = new HashSet<>();
          fields.addAll(FIELDS);
        }

        for (String column : fields) {
          String val = rs.getString(FIELDS.indexOf(column) + 1);

          if (val != null) {
            result.put(column, new StringByteIterator(val));
          }
        }
      }
    } catch (Exception e) {
      LOG.error("Error reading key" + key, e);

      return Status.ERROR;
    }

    return Status.OK;
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
      if (table.equals(cacheName)) {
        PreparedStatement stmt = prepareInsertStatement(CONN.get(), table, values);

        setStatementValues(stmt, key, values);

        stmt.executeUpdate();
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
    // TODO: implement
    return Status.NOT_IMPLEMENTED;
  }

  /** {@inheritDoc} */
  @Override
  public void cleanup() throws DBException {
    Connection conn0 = CONN.get();
    try {
      if (conn0 != null && !conn0.isClosed()) {
        conn0.close();
        CONN.remove();
      }
    } catch (Exception e) {
      throw new DBException(e);
    }

    super.cleanup();
  }
}
