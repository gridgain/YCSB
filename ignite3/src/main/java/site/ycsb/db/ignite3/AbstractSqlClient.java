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

import static site.ycsb.Client.parseIntWithModifiers;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import site.ycsb.ByteIterator;
import site.ycsb.Client;
import site.ycsb.DBException;
import site.ycsb.workloads.CoreWorkload;

/**
 * Abstract SQL client for Ignite.
 */
public abstract class AbstractSqlClient extends IgniteAbstractClient {
  /** SQL string of prepared statement for reading values. */
  protected static String readPreparedStatementString;

  /** SQL string of prepared statement for reading batch of values. */
  protected static String batchReadPreparedStatementString;

  /** SQL string of prepared statement for inserting values. */
  protected static String insertPreparedStatementString;

  /** Whether to write all fields in update operation. */
  protected static boolean updateAllFields;

  /** SQL strings map of prepared statements for updating 1 of field values. */
  protected static Map<String, String> updateOneFieldPreparedStatementStrings;

  /** SQL string of prepared statement for updating all fields. */
  protected static String updateAllFieldsPreparedStatementString;

  /** SQL string of prepared statement for deleting values. */
  protected static String deletePreparedStatementString;

  /** Batch size. */
  protected static int batchSize;

  /** {@inheritDoc} */
  @Override
  public void init() throws DBException {
    super.init();

    synchronized (AbstractSqlClient.class) {
      if (readPreparedStatementString != null
          || batchReadPreparedStatementString != null
          || insertPreparedStatementString != null
          || deletePreparedStatementString != null
          || updateOneFieldPreparedStatementStrings != null
          || updateAllFieldsPreparedStatementString != null) {
        return;
      }

      updateAllFields = Boolean.parseBoolean(getProperties().getProperty(
          CoreWorkload.WRITE_ALL_FIELDS_PROPERTY, CoreWorkload.WRITE_ALL_FIELDS_PROPERTY_DEFAULT));

      batchSize = parseIntWithModifiers(getProperties().getProperty(
          Client.BATCH_SIZE_PROPERTY, Client.DEFAULT_BATCH_SIZE));

      List<String> columns = new ArrayList<>(Collections.singletonList(PRIMARY_COLUMN_NAME));
      columns.addAll(valueFields);

      String columnNamesString = String.join(", ", columns);

      String columnDynamicParamsString = String.join(", ", Collections.nCopies(columns.size(), "?"));

      String batchDynamicParamsString = String.join(", ", Collections.nCopies(batchSize, "?"));

      readPreparedStatementString = useColumnar ?
          String.format("SELECT * FROM %s /*+ use_secondary_storage */ WHERE %s = ?",
              tableNamePrefix, PRIMARY_COLUMN_NAME) :
          String.format("SELECT * FROM %s WHERE %s = ?",
              tableNamePrefix, PRIMARY_COLUMN_NAME);

      batchReadPreparedStatementString = useColumnar ?
          String.format("SELECT * FROM %s /*+ use_secondary_storage */ WHERE %s in (%s)",
              tableNamePrefix, PRIMARY_COLUMN_NAME, batchDynamicParamsString) :
          String.format("SELECT * FROM %s WHERE %s in (%s)",
              tableNamePrefix, PRIMARY_COLUMN_NAME, batchDynamicParamsString);

      insertPreparedStatementString = String.format("INSERT INTO %s (%s) VALUES (%s)",
          tableNamePrefix, columnNamesString, columnDynamicParamsString);

      updateOneFieldPreparedStatementStrings = new HashMap<>();
      for (String field : valueFields) {
        updateOneFieldPreparedStatementStrings.put(
            field,
            String.format("UPDATE %s SET %s = ? WHERE %s = ?", tableNamePrefix, field, PRIMARY_COLUMN_NAME)
        );
      }

      String updateFields = valueFields.stream()
          .map(v -> String.format("%s = ?", v))
          .collect(Collectors.joining(", "));
      updateAllFieldsPreparedStatementString = String.format("UPDATE %s SET %s WHERE %s = ?",
          tableNamePrefix, updateFields, PRIMARY_COLUMN_NAME);

      deletePreparedStatementString = String.format("DELETE * FROM %s WHERE %s = ?",
          tableNamePrefix, PRIMARY_COLUMN_NAME);
    }
  }

  /**
   * Set values for the prepared statement object.
   *
   * @param statement Prepared statement object.
   * @param key Key field value.
   * @param values Values.
   */
  protected void setStatementValues(PreparedStatement statement, String key, Map<String, ByteIterator> values)
      throws SQLException {
    int i = 1;

    statement.setString(i++, key);

    for (String fieldName: valueFields) {
      statement.setString(i++, values.get(fieldName).toString());
    }
  }

  /**
   * Get UPDATE SQL string.
   *
   * @param key Key.
   * @param values Values.
   */
  protected String getUpdateSql(String key, Map<String, ByteIterator> values) {
    List<String> updateValuesList = new ArrayList<>();

    for (Entry<String, ByteIterator> entry : values.entrySet()) {
      updateValuesList.add(String.format("%s='%s'", entry.getKey(), entry.getValue().toString()));
    }

    return String.format("UPDATE %s SET %s WHERE %s = '%s'",
        tableNamePrefix, String.join(", ", updateValuesList), PRIMARY_COLUMN_NAME, key);
  }
}
