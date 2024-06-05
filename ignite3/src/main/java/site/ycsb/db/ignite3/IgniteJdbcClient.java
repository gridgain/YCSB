package site.ycsb.db.ignite3;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
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
  private Connection connection;

  /** Prepared statement for reading values. */
  private PreparedStatement readPreparedStatement;

  /** Prepared statement for inserting values. */
  private PreparedStatement insertPreparedStatement;

  /** Prepared statement for deleting values. */
  private PreparedStatement deletePreparedStatement;

  /** {@inheritDoc} */
  @Override
  public void init() throws DBException {
    super.init();

    String hostsStr = useEmbeddedIgnite ?
        node.clusterNodes().stream()
            .map(clusterNode -> clusterNode.address().host())
            .collect(Collectors.joining(",")) :
        hosts;

    //workaround for https://ggsystems.atlassian.net/browse/IGN-23887
    //use only one cluster node address for connection
    hostsStr = hostsStr.split(",")[0];

    String url = "jdbc:ignite:thin://" + hostsStr;
    try {
      connection = DriverManager.getConnection(url);

      readPreparedStatement = connection.prepareStatement(readPreparedStatementString);
      insertPreparedStatement = connection.prepareStatement(insertPreparedStatementString);
      deletePreparedStatement = connection.prepareStatement(deletePreparedStatementString);
    } catch (Exception e) {
      throw new DBException(e);
    }
  }

  /** {@inheritDoc} */
  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    try {
      readPreparedStatement.setString(1, key);

      try (ResultSet rs = readPreparedStatement.executeQuery()) {
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
      try (Statement stmt = connection.createStatement()) {
        List<String> updateValuesList = new ArrayList<>();

        for (Entry<String, ByteIterator> entry : values.entrySet()) {
          updateValuesList.add(String.format("%s='%s'", entry.getKey(), entry.getValue().toString()));
        }

        String sql = String.format("UPDATE %s SET %s WHERE %s = '%s'",
            cacheName, String.join(", ", updateValuesList), PRIMARY_COLUMN_NAME, key);

        stmt.executeUpdate(sql);
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
      setStatementValues(insertPreparedStatement, key, values);

      insertPreparedStatement.executeUpdate();

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
      deletePreparedStatement.setString(1, key);

      deletePreparedStatement.executeUpdate();

      return Status.OK;
    } catch (Exception e) {
      LOG.error(String.format("Error deleting key: %s", key), e);

      return Status.ERROR;
    }
  }

  /** {@inheritDoc} */
  @Override
  public void cleanup() throws DBException {
    try {
      if (connection != null && !connection.isClosed()) {
        if (!readPreparedStatement.isClosed()) {
          readPreparedStatement.close();
        }
        if (!insertPreparedStatement.isClosed()) {
          insertPreparedStatement.close();
        }
        if (!deletePreparedStatement.isClosed()) {
          deletePreparedStatement.close();
        }

        connection.close();
      }
    } catch (Exception e) {
      throw new DBException(e);
    }

    super.cleanup();
  }
}
