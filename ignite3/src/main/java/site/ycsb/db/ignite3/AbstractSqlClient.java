package site.ycsb.db.ignite3;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import site.ycsb.ByteIterator;

abstract class AbstractSqlClient extends IgniteAbstractClient {
  /** Prepared statement for reading values. */
  protected static final ThreadLocal<PreparedStatement> readPreparedStatement = new ThreadLocal<>();

  /** Prepared statement for inserting values. */
  protected static final ThreadLocal<PreparedStatement> insertPreparedStatement = new ThreadLocal<>();

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
    PreparedStatement statement = insertPreparedStatement.get();

    if (statement != null) {
      return statement;
    }

    statement = conn.prepareStatement(prepareInsertStatement(table, values));

    insertPreparedStatement.set(statement);

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
    PreparedStatement statement = readPreparedStatement.get();

    if (statement != null) {
      return statement;
    }

    statement = conn.prepareStatement(prepareReadStatement(table));

    readPreparedStatement.set(statement);

    return statement;
  }
}
