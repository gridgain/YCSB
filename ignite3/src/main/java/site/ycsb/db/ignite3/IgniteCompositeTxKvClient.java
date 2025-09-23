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

import java.util.Map;
import java.util.Set;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.Transaction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import site.ycsb.ByteIterator;
import site.ycsb.DBException;
import site.ycsb.Status;

/**
 * Ignite3 key-value client with transactions that include extra table with composite key.
 */
public class IgniteCompositeTxKvClient extends IgniteTxKvClient {
  /** */
  private static final Logger LOG = LogManager.getLogger(IgniteCompositeTxKvClient.class);

  private static final String TAG_PRIMARY_KEY = "tag_pk";

  private static final String TAG_NAME_COLUMN = "tag_name";

  private static final String TAG_VALUE_COLUMN = "tag_value";

  private static volatile boolean tagsTableCreated = false;

  public static final String TAGS_TABLE_NAME = "tagstable";

  private KeyValueView<Tuple, Tuple> tagsKvView;

  /** {@inheritDoc} */
  @Override
  public void init() throws DBException {
    super.init();

    synchronized (IgniteCompositeTxKvClient.class) {
      if (!tagsTableCreated) {
        if (!isRunPhase) {
          createTagsTable();
        }

        tagsTableCreated = true;
      }
    }

    tagsKvView = ignite.tables().table(TAGS_TABLE_NAME).keyValueView();
  }

  /** {@inheritDoc} */
  @Override
  public Status read(String table, String key, Set<String> fields,
                     Map<String, ByteIterator> result) {
    try {
      return get(null, key, fields, result);
    } catch (Exception e) {
      LOG.error(String.format("Error reading key: %s", key), e);

      return Status.ERROR;
    }
  }

  /** {@inheritDoc} */
  @Override
  protected void put(Transaction tx, String key, Map<String, ByteIterator> values) {
    Tuple tValue = Tuple.create(fieldCount);
    values.forEach((field, value) -> tValue.set(field, value.toString()));

    Tuple tKey = Tuple.create(1).set(PRIMARY_COLUMN_NAME, key);
    getKvView(key).put(tx, tKey, tValue);

    Tuple tagsKey = Tuple.create();
    tagsKey.set(TAG_NAME_COLUMN, keyToTagName(key));
    tagsKey.set(TAG_VALUE_COLUMN, keyToTagValue(key));
    tagsKey.set(PRIMARY_COLUMN_NAME, key);
    tagsKvView.put(tx, tagsKey, tValue);
  }

  /** {@inheritDoc} */
  @Override
  protected void getAndPut(Transaction tx, String key, Map<String, ByteIterator> values) {
    Tuple tValue = Tuple.create(fieldCount);
    values.forEach((field, value) -> tValue.set(field, value.toString()));

    Tuple tKey = Tuple.create(1).set(PRIMARY_COLUMN_NAME, key);
    getKvView(key).getAndPut(tx, tKey, tValue);

    Tuple tagsKey = Tuple.create();
    tagsKey.set(TAG_NAME_COLUMN, keyToTagName(key));
    tagsKey.set(TAG_VALUE_COLUMN, keyToTagValue(key));
    tagsKey.set(PRIMARY_COLUMN_NAME, key);
    tagsKvView.putAll(tx, Map.of(tagsKey, tValue));
  }

  /** {@inheritDoc} */
  @Override
  protected void remove(Transaction tx, String key) {
    Tuple tKey = Tuple.create(1).set(PRIMARY_COLUMN_NAME, key);
    getKvView(key).remove(tx, tKey);

    Tuple tagsKey = Tuple.create();
    tagsKey.set(TAG_NAME_COLUMN, keyToTagName(key));
    tagsKey.set(TAG_VALUE_COLUMN, keyToTagValue(key));
    tagsKey.set(PRIMARY_COLUMN_NAME, key);
    tagsKvView.remove(tx, tagsKey);
  }

  /**
   * Create tags table.
   */
  public void createTagsTable() throws DBException {
    String keysSpecs = getKeysSpecs();
    String fieldsSpecs = getFieldsSpecs();
    String zoneSpecs = getZoneSpecs();

    String tagsTableSQL = String.format(
        "CREATE TABLE IF NOT EXISTS %s(%s, %s)%s",
        TAGS_TABLE_NAME, keysSpecs, fieldsSpecs, zoneSpecs);

    LOG.info("Creating table '{}'. SQL line: {}", TAGS_TABLE_NAME, tagsTableSQL);
    ignite.sql().execute(null, tagsTableSQL).close();

    boolean cachePresent = waitForCondition(() -> ignite.tables().table(TAGS_TABLE_NAME) != null,
        TABLE_CREATION_TIMEOUT_SECONDS * 1_000L);

    if (!cachePresent) {
      throw new DBException(String.format(
          "Table '%s' wasn't created in %s seconds.", TAGS_TABLE_NAME, TABLE_CREATION_TIMEOUT_SECONDS));
    }
  }

  /**
   * Get keys specifications SQL.
   */
  protected String getKeysSpecs() {
    return String.format(
        "%s VARCHAR NOT NULL, %s VARCHAR NOT NULL, %s VARCHAR NOT NULL, CONSTRAINT %s PRIMARY KEY (%s, %s, %s)",
        TAG_NAME_COLUMN, TAG_VALUE_COLUMN, PRIMARY_COLUMN_NAME, TAG_PRIMARY_KEY,
        TAG_NAME_COLUMN, TAG_VALUE_COLUMN, PRIMARY_COLUMN_NAME);
  }

  /**
   * Creates tag name from 'key' by replacing 'user' with 'tagn'.
   *
   * @param key Key.
   */
  private static String keyToTagName(String key) {
    return "tagn" + key.substring(4);
  }

  /**
   * Creates tag value from 'key' by replacing 'user' with 'tagv'.
   *
   * @param key Key.
   */
  private static String keyToTagValue(String key) {
    return "tagv" + key.substring(4);
  }
}
