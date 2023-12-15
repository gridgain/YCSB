package site.ycsb.db.ignite3;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import site.ycsb.ByteIterator;
import site.ycsb.DBException;

abstract class AbstractSqlClient extends IgniteAbstractClient {
  /** Prepared statement for reading values. */
  protected static final ThreadLocal<PreparedStatement> READ_PREPARED_STATEMENT = new ThreadLocal<>();

  /** Prepared statement for inserting values. */
  protected static final ThreadLocal<PreparedStatement> INSERT_PREPARED_STATEMENT = new ThreadLocal<>();

  /** SQL string of prepared statement for reading values. */
  protected static String readPreparedStatementString;

  /** SQL string of prepared statement for inserting values. */
  protected static String insertPreparedStatementString;

  /** {@inheritDoc} */
  @Override
  public void init() throws DBException {
    super.init();

    synchronized (AbstractSqlClient.class) {
      if (readPreparedStatementString != null || insertPreparedStatementString != null) {
        return;
      }

      readPreparedStatementString = String.format("SELECT * FROM %s WHERE %s = ?", cacheName, PRIMARY_COLUMN_NAME);

      List<String> columns = new ArrayList<>(Collections.singletonList(PRIMARY_COLUMN_NAME));
      columns.addAll(FIELDS);

      String columnsString = String.join(", ", columns);

      String valuesString = String.join(", ", Collections.nCopies(columns.size(), "?"));

      insertPreparedStatementString = String.format("INSERT INTO %s (%s) VALUES (%s)",
          cacheName, columnsString, valuesString);
    }
  }

  /**
   * Init prepared statement object for inserting values.
   *
   * @param conn Connection.
   */
  static PreparedStatement prepareInsertStatement(Connection conn)
      throws SQLException {
    PreparedStatement statement = INSERT_PREPARED_STATEMENT.get();

    if (statement != null) {
      return statement;
    }

    statement = conn.prepareStatement(insertPreparedStatementString);

    INSERT_PREPARED_STATEMENT.set(statement);

    return statement;
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

    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      statement.setString(i++, entry.getValue().toString());
    }
  }

  /**
   * Init prepared statement object for reading values.
   *
   * @param conn Connection.
   */
  static PreparedStatement prepareReadStatement(Connection conn) throws SQLException {
    PreparedStatement statement = READ_PREPARED_STATEMENT.get();

    if (statement != null) {
      return statement;
    }

    statement = conn.prepareStatement(readPreparedStatementString);

    READ_PREPARED_STATEMENT.set(statement);

    return statement;
  }
}
