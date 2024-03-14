package site.ycsb.db.ignite;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import site.ycsb.ByteIterator;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

/**
 * Ignite2 JDBC client.
 */
public class IgniteJdbcClient extends IgniteAbstractClient {
  static {
    accessMethod = "jdbc";
  }

  public static final Logger LOG = LogManager.getLogger(IgniteJdbcClient.class);

  /** JDBC connection. */
  private Connection connection;

  /** Prepared statement for reading values. */
  private PreparedStatement psRead;

  /** Prepared statement for inserting values. */
  private PreparedStatement psInsert;

  /** Prepared statement for deleting values. */
  private PreparedStatement psDelete;

  /** {@inheritDoc} */
  @Override
  public void init() throws DBException {
    super.init();

    String hostsStr;

    if (useEmbeddedIgnite) {
      Set<String> addrs = new HashSet<>();
      cluster.cluster().nodes().forEach(clusterNode -> addrs.addAll(clusterNode.addresses()));
      hostsStr = String.join(",", addrs);
    } else {
      hostsStr = hosts;
    }

    String url = "jdbc:ignite:thin://" + hostsStr;

    try {
      connection = DriverManager.getConnection(url);
    } catch (Exception e) {
      throw new DBException(e);
    }

    List<String> columns = new ArrayList<>(Collections.singletonList(PRIMARY_COLUMN_NAME));
    columns.addAll(FIELDS);
    String columnsStr = String.join(", ", columns);
    String valuesStr = String.join(", ", Collections.nCopies(columns.size(), "?"));

    String psReadStr = String.format("SELECT * FROM %s WHERE %s = ?",
        cacheName, PRIMARY_COLUMN_NAME);

    String psInsertStr = String.format("INSERT INTO %s (%s) VALUES (%s)",
        cacheName, columnsStr, valuesStr);

    String psDeleteStr = String.format("DELETE * FROM %s WHERE %s = ?",
        cacheName, PRIMARY_COLUMN_NAME);

    try {
      psRead = connection.prepareStatement(psReadStr);
      psInsert = connection.prepareStatement(psInsertStr);
      psDelete = connection.prepareStatement(psDeleteStr);
    } catch (Exception e) {
      throw new DBException(e);
    }
  }

  /** {@inheritDoc} */
  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    try {
      PreparedStatement stmt = psRead;

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
          //+2 because indexes start from 1 and 1st one is key field
          String val = rs.getString(FIELDS.indexOf(column) + 2);

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
    try {
      if (table.equals(cacheName)) {
        try (Statement stmt = connection.createStatement()) {

          List<String> updateValuesList = new ArrayList<>();
          for (Entry<String, ByteIterator> entry : values.entrySet()) {
            updateValuesList.add(String.format("%s='%s'", entry.getKey(), entry.getValue().toString()));
          }

          String sql = String.format("UPDATE %s SET %s WHERE %s = '%s'",
              cacheName, String.join(", ", updateValuesList), PRIMARY_COLUMN_NAME, key);

          stmt.executeUpdate(sql);
        }
      } else {
        throw new UnsupportedOperationException("Unexpected table name: " + table);
      }

      return Status.OK;
    } catch (Exception e) {
      LOG.error(String.format("Error updating key: %s", key), e);

      return Status.ERROR;
    }
  }

  /** {@inheritDoc} */
  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    try {
      if (table.equals(cacheName)) {
        PreparedStatement stmt = psInsert;

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
    try {
      if (table.equals(cacheName)) {
        PreparedStatement stmt = psDelete;

        stmt.setString(1, key);

        stmt.executeUpdate();
      } else {
        throw new UnsupportedOperationException("Unexpected table name: " + table);
      }

      return Status.OK;
    } catch (Exception e) {
      LOG.error(String.format("Error deleting key: %s", key), e);

      return Status.ERROR;
    }
  }

  /** {@inheritDoc} */
  @Override
  public void cleanup() throws DBException {
    Connection conn0 = connection;
    try {
      if (conn0 != null && !conn0.isClosed()) {
        if (!psRead.isClosed()) {
          psRead.close();
        }
        if (!psInsert.isClosed()) {
          psInsert.close();
        }
        if (!psDelete.isClosed()) {
          psDelete.close();
        }

        conn0.close();
      }
    } catch (Exception e) {
      throw new DBException(e);
    }

    super.cleanup();
  }

  /**
   * Set values for the prepared statement object.
   *
   * @param statement Prepared statement object.
   * @param key Key field value.
   * @param values Values.
   */
  static void setStatementValues(PreparedStatement statement, String key, Map<String, ByteIterator> values)
      throws SQLException {
    int i = 1;

    statement.setString(i++, key);

    for (String fieldName : FIELDS) {
      statement.setString(i++, values.get(fieldName).toString());
    }
  }
}
