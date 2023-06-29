package site.ycsb.db.ignite3;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import site.ycsb.ByteIterator;
import site.ycsb.DBException;
import site.ycsb.Status;

/**
 * Ignite3 JDBC benchmark.
 */
public class IgniteJdbcClient extends IgniteAbstractClient {

  private static final AtomicInteger JDBC_INIT_COUNT = new AtomicInteger(0);

  private static Connection conn;

  @Override
  public void init() throws DBException {
    super.init();

    JDBC_INIT_COUNT.incrementAndGet();

    synchronized (IgniteJdbcClient.class) {
      if (conn != null) {
        return;
      }

      try {
        String url = "jdbc:ignite:thin://" + host + ":" + ports;

        conn = DriverManager.getConnection(url);
      } catch (Exception e) {
        throw new DBException(e);
      }
    }
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    // TODO: implement
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    // TODO: implement
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status delete(String table, String key) {
    // TODO: implement
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public void cleanup() throws DBException {
    synchronized (IgniteJdbcClient.class) {
      int currInitCount = JDBC_INIT_COUNT.decrementAndGet();

      if (currInitCount <= 0) {
        try {
          conn.close();
          conn = null;
        } catch (Exception e) {
          throw new DBException(e);
        }
      }
    }

    super.cleanup();
  }
}
