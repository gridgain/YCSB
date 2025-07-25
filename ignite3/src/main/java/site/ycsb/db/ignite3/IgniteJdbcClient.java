/**
 * Copyright (c) 2013-2018 YCSB contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License. See accompanying LICENSE file.
 * <p>
 */
package site.ycsb.db.ignite3;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
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
  protected static final ThreadLocal<Connection> CONN = new ThreadLocal<>();

  /** Prepared statement for reading values. */
  private static final ThreadLocal<PreparedStatement> READ_PREPARED_STATEMENT = ThreadLocal
      .withInitial(IgniteJdbcClient::buildReadStatement);

  /** Prepared statement for inserting values. */
  private static final ThreadLocal<PreparedStatement> INSERT_PREPARED_STATEMENT = ThreadLocal
      .withInitial(IgniteJdbcClient::buildInsertStatement);

  /** Prepared statements map for updating 1 of field values. */
  private static final ThreadLocal<Map<String, PreparedStatement>> UPDATE_ONE_FIELD_PREPARED_STATEMENTS = ThreadLocal
      .withInitial(IgniteJdbcClient::buildUpdateOneStatements);

  /** Prepared statement for updating all fields. */
  private static final ThreadLocal<PreparedStatement> UPDATE_ALL_FIELDS_PREPARED_STATEMENT = ThreadLocal
      .withInitial(IgniteJdbcClient::buildUpdateAllStatement);

  /** Prepared statement for deleting values. */
  private static final ThreadLocal<PreparedStatement> DELETE_PREPARED_STATEMENT = ThreadLocal
      .withInitial(IgniteJdbcClient::buildDeleteStatement);

  /** Build prepared statement for reading values. */
  private static PreparedStatement buildReadStatement() {
    try {
      return CONN.get().prepareStatement(readPreparedStatementString);
    } catch (SQLException e) {
      throw new RuntimeException("Unable to prepare statement for SQL: " + readPreparedStatementString, e);
    }
  }

  /** Build prepared statement for inserting values. */
  private static PreparedStatement buildInsertStatement() {
    try {
      return CONN.get().prepareStatement(insertPreparedStatementString);
    } catch (SQLException e) {
      throw new RuntimeException("Unable to prepare statement for SQL: " + insertPreparedStatementString, e);
    }
  }

  /** Build prepared statements map for updating 1 of field values. */
  private static Map<String, PreparedStatement> buildUpdateOneStatements() {
    Map<String, PreparedStatement> updatePreparedStatements = new HashMap<>();

    updateOneFieldPreparedStatementStrings.forEach((field, updateSql) -> {
        try {
          PreparedStatement preparedStatement = CONN.get().prepareStatement(updateSql);
          updatePreparedStatements.put(field, preparedStatement);
        } catch (SQLException e) {
          throw new RuntimeException("Unable to prepare statement for SQL: " + updateSql, e);
        }
      });

    return updatePreparedStatements;
  }

  /** Build prepared statement for updating all fields. */
  private static PreparedStatement buildUpdateAllStatement() {
    try {
      return CONN.get().prepareStatement(updateAllFieldsPreparedStatementString);
    } catch (SQLException e) {
      throw new RuntimeException("Unable to prepare statement for SQL: " + insertPreparedStatementString, e);
    }
  }

  /** Build prepared statement for deleting values. */
  private static PreparedStatement buildDeleteStatement() {
    try {
      return CONN.get().prepareStatement(deletePreparedStatementString);
    } catch (SQLException e) {
      throw new RuntimeException("Unable to prepare statement for SQL: " + deletePreparedStatementString, e);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void init() throws DBException {
    super.init();

    String hostsStr = useEmbeddedIgnite ?
        ignite.clusterNodes().stream()
            .map(clusterNode -> clusterNode.address().host())
            .collect(Collectors.joining(",")) :
        hosts;

    //workaround for https://ggsystems.atlassian.net/browse/IGN-23887
    //use only one cluster node address for connection
    hostsStr = hostsStr.split(",")[0];

    String url = "jdbc:ignite:thin://" + hostsStr;
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
      return get(key, fields, result);
    } catch (Exception e) {
      LOG.error("Error reading key" + key, e);

      return Status.ERROR;
    }
  }

  /** {@inheritDoc} */
  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    try {
      modify(key, values);

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
      put(key, values);

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
      remove(key);

      return Status.OK;
    } catch (Exception e) {
      LOG.error(String.format("Error deleting key: %s", key), e);

      return Status.ERROR;
    }
  }

  /** {@inheritDoc} */
  @Override
  public void cleanup() throws DBException {
    Connection conn0 = CONN.get();
    try {
      if (conn0 != null && !conn0.isClosed()) {
        if (!READ_PREPARED_STATEMENT.get().isClosed()) {
          READ_PREPARED_STATEMENT.get().close();
        }
        if (!INSERT_PREPARED_STATEMENT.get().isClosed()) {
          INSERT_PREPARED_STATEMENT.get().close();
        }
        if (!DELETE_PREPARED_STATEMENT.get().isClosed()) {
          DELETE_PREPARED_STATEMENT.get().close();
        }

        conn0.close();
        CONN.remove();
      }
    } catch (Exception e) {
      throw new DBException(e);
    }

    super.cleanup();
  }

  /**
   * Perform single INSERT operation with Ignite JDBC client.
   *
   * @param key Key.
   * @param values Values.
   */
  protected void put(String key, Map<String, ByteIterator> values) throws SQLException {
    PreparedStatement stmt = INSERT_PREPARED_STATEMENT.get();

    setStatementValues(stmt, key, values);

    stmt.executeUpdate();
  }

  /**
   * Perform single SELECT operation with Ignite JDBC client.
   *
   * @param key Key.
   * @param fields Fields.
   * @param result Result.
   */
  @NotNull
  protected Status get(String key, Set<String> fields, Map<String, ByteIterator> result) throws SQLException {
    PreparedStatement stmt = READ_PREPARED_STATEMENT.get();

    stmt.setString(1, key);

    try (ResultSet rs = stmt.executeQuery()) {
      if (!rs.next()) {
        return Status.NOT_FOUND;
      }

      if (fields == null || fields.isEmpty()) {
        fields = new HashSet<>();
        fields.addAll(this.valueFields);
      }

      for (String column : fields) {
        //+2 because indexes start from 1 and 1st one is key field
        String val = rs.getString(this.valueFields.indexOf(column) + 2);

        if (val != null) {
          result.put(column, new StringByteIterator(val));
        }
      }
    }

    return Status.OK;
  }

  /**
   * Perform single UPDATE operation with Ignite JDBC client.
   *
   * @param key Key.
   * @param values Values.
   */
  protected void modify(String key, Map<String, ByteIterator> values) throws SQLException {
    if (updateAllFields) {
      PreparedStatement stmt = UPDATE_ALL_FIELDS_PREPARED_STATEMENT.get();

      int i = 1;
      for (String fieldName : valueFields) {
        stmt.setString(i++, values.get(fieldName).toString());
      }
      stmt.setString(i, key);

      stmt.executeUpdate();
    } else if (values.size() == 1) {
      String field = values.keySet().iterator().next();

      PreparedStatement stmt = UPDATE_ONE_FIELD_PREPARED_STATEMENTS.get().get(field);
      stmt.setString(1, values.get(field).toString());
      stmt.setString(2, key);

      stmt.executeUpdate();
    } else {
      try (Statement stmt = CONN.get().createStatement()) {
        String sql = getUpdateSql(key, values);

        stmt.executeUpdate(sql);
      }
    }
  }

  /**
   * Perform single DELETE operation with Ignite JDBC client.
   *
   * @param key Key.
   */
  protected static void remove(String key) throws SQLException {
    PreparedStatement stmt = DELETE_PREPARED_STATEMENT.get();

    stmt.setString(1, key);

    stmt.executeUpdate();
  }
}
