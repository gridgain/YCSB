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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.sql.Statement;
import org.apache.ignite.tx.Transaction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import site.ycsb.ByteIterator;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

/**
 * Ignite3 SQL API client.
 */
public class IgniteSqlClient extends AbstractSqlClient {
  /** */
  private static final Logger LOG = LogManager.getLogger(IgniteSqlClient.class);

  /** Statement for reading values. */
  private static final ThreadLocal<Statement> READ_STATEMENT = ThreadLocal
      .withInitial(IgniteSqlClient::buildReadStatement);

  /** Statement for inserting values. */
  private static final ThreadLocal<Statement> INSERT_STATEMENT = ThreadLocal
      .withInitial(IgniteSqlClient::buildInsertStatement);

  /** Statement for updating 1 of field values. */
  private static final ThreadLocal<Map<String, Statement>> UPDATE_ONE_FIELD_STATEMENTS = ThreadLocal
      .withInitial(IgniteSqlClient::buildUpdateOneStatements);

  /** Statement for updating all fields. */
  private static final ThreadLocal<Statement> UPDATE_ALL_FIELDS_STATEMENT = ThreadLocal
      .withInitial(IgniteSqlClient::buildUpdateAllStatement);

  /** Build statement for reading values. */
  private static Statement buildReadStatement() {
    return ignite.sql().createStatement(readPreparedStatementString);
  }

  /** Build statement for inserting values. */
  private static Statement buildInsertStatement() {
    return ignite.sql().createStatement(insertPreparedStatementString);
  }

  /** Build statements for updating 1 of field values. */
  private static Map<String, Statement> buildUpdateOneStatements() {
    Map<String, Statement> statements = new java.util.HashMap<>();

    updateOneFieldPreparedStatementStrings.forEach((field, updateSql) -> {
        statements.put(field, ignite.sql().createStatement(updateSql));
      });

    return statements;
  }

  /** Build statement for inserting values. */
  private static Statement buildUpdateAllStatement() {
    return ignite.sql().createStatement(updateAllFieldsPreparedStatementString);
  }

  /** {@inheritDoc} */
  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    try {
      return get(null, table, key, fields, result);
    } catch (Exception e) {
      LOG.error(String.format("Error reading key: %s", key), e);

      return Status.ERROR;
    }
  }

  /** {@inheritDoc} */
  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    try {
      modify(null, key, values);

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
      put(null, key, values);

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
        "DELETE FROM %s WHERE %s = '%s'", table, PRIMARY_COLUMN_NAME, key
    );

    try {
      if (debug) {
        LOG.info(deleteStatement);
      }

      ignite.sql().execute(null, deleteStatement).close();

      return Status.OK;
    } catch (Exception e) {
      LOG.error(String.format("Error deleting key: %s ", key), e);
    }

    return Status.ERROR;
  }

  /**
   * Perform single INSERT operation with Ignite SQL.
   *
   * @param tx Transaction.
   * @param key Key.
   * @param values Values.
   */
  protected void put(Transaction tx, String key, Map<String, ByteIterator> values) {
    List<String> valuesList = new ArrayList<>();
    valuesList.add(key);
    valueFields.forEach(fieldName -> valuesList.add(String.valueOf(values.get(fieldName))));
    ignite.sql().execute(tx, INSERT_STATEMENT.get(), (Object[]) valuesList.toArray(new String[0])).close();
  }

  /**
   * Perform single UPDATE operation with Ignite SQL.
   *
   * @param tx Transaction.
   * @param key Key.
   * @param values Values.
   */
  protected void modify(Transaction tx, String key, Map<String, ByteIterator> values) {
    if (updateAllFields) {
      List<String> valuesList = new ArrayList<>();
      valueFields.forEach(fieldName -> valuesList.add(String.valueOf(values.get(fieldName))));
      valuesList.add(key);

      Statement updateStmt = UPDATE_ALL_FIELDS_STATEMENT.get();

      ignite.sql().execute(tx, updateStmt, (Object[]) valuesList.toArray(new String[0])).close();
    } else if (values.size() == 1) {
      String field = values.keySet().iterator().next();

      Statement updateStmt = UPDATE_ONE_FIELD_STATEMENTS.get().get(field);

      ignite.sql().execute(tx, updateStmt, String.valueOf(values.get(field)), key).close();
    } else {
      String updateSql = getUpdateSql(key, values);

      ignite.sql().execute(tx, updateSql).close();
    }
  }

  /**
   * Perform single SELECT operation with Ignite SQL.
   *
   * @param tx Tx.
   * @param table Table.
   * @param key Key.
   * @param fields Fields.
   * @param result Result.
   */
  @NotNull
  protected Status get(Transaction tx, String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    try (ResultSet<SqlRow> rs = ignite.sql().execute(tx, READ_STATEMENT.get(), key)) {
      if (!rs.hasNext()) {
        return Status.NOT_FOUND;
      }

      SqlRow row = rs.next();

      if (fields == null || fields.isEmpty()) {
        fields = new HashSet<>();
        fields.addAll(this.valueFields);
      }

      for (String column : fields) {
        // Shift to exclude the first column from the result
        String val = row.stringValue(this.valueFields.indexOf(column) + 1);

        if (val != null) {
          result.put(column, new StringByteIterator(val));
        }
      }
    }

    if (debug) {
      LOG.info("table: {}, key: {}, fields: {}", table, key, fields);
      LOG.info("result: {}", result);
    }

    return Status.OK;
  }
}
