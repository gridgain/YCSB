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
  protected static ThreadLocal<PreparedStatement> readPreparedStatement;

  /** Prepared statement for inserting values. */
  protected static ThreadLocal<PreparedStatement> insertPreparedStatement;

  /** {@inheritDoc} */
  @Override
  public void init() throws DBException {
    super.init();

    readPreparedStatement = new ThreadLocal<>();
    insertPreparedStatement = new ThreadLocal<>();
  }

  /**
   * Form a prepared statement SQL command for inserting values.
   *
   * @param table Table.
   * @param values Values.
   */
  static String prepareInsertStatement(String table, Map<String, ByteIterator> values) {
    List<String> columns = new ArrayList<>(Collections.singletonList(PRIMARY_COLUMN_NAME));

    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      columns.add(entry.getKey());
    }

    String columnsString = String.join(", ", columns);
    String valuesString = String.join(", ", Collections.nCopies(columns.size(), "?"));

    return String.format("INSERT INTO %s (%s) VALUES (%s)", table, columnsString, valuesString);
  }

  /**
   * Init prepared statement object for inserting values.
   *
   * @param conn Connection.
   * @param table Table.
   * @param values Values.
   */
  static PreparedStatement prepareInsertStatement(Connection conn, String table, Map<String, ByteIterator> values)
      throws SQLException {
    if (insertPreparedStatement.get() != null) {
      return insertPreparedStatement.get();
    }

    insertPreparedStatement.set(conn.prepareStatement(prepareInsertStatement(table, values)));

    return insertPreparedStatement.get();
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
   * Form a prepared statement SQL command for reading values.
   *
   * @param table Table.
   */
  static String prepareReadStatement(String table) {
    return String.format("SELECT * FROM %s WHERE %s = ?", table, PRIMARY_COLUMN_NAME);
  }

  /**
   * Init prepared statement object for reading values.
   *
   * @param conn Connection.
   * @param table Table.
   */
  static PreparedStatement prepareReadStatement(Connection conn, String table) throws SQLException {
    if (readPreparedStatement.get() != null) {
      return readPreparedStatement.get();
    }

    String readStatement = prepareReadStatement(table);

    readPreparedStatement.set(conn.prepareStatement(readStatement));

    return readPreparedStatement.get();
  }
}
