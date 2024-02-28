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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.ignite.table.Tuple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import site.ycsb.ByteIterator;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;


/**
 * Ignite3 key-value client.
 */
public class IgniteBatchClient extends IgniteAbstractClient {
  /** Logger. */
  private static final Logger LOG = LogManager.getLogger(IgniteBatchClient.class);

  /** Batch holder. */
  private Map<Tuple, Tuple> batchHolder = new LinkedHashMap<>();

  /** Amount of records in batch. */
  private long recordsInBatch = 0;

  /** Total processed records. */
  private long processedRecords = 0;

  /** {@inheritDoc} */
  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    try {
      if (!table.equals(cacheName)) {
        throw new UnsupportedOperationException("Unexpected table name: " + table);
      }

      Tuple tKey = Tuple.create(1).set(PRIMARY_COLUMN_NAME, key);

      Tuple tValues = Tuple.create(fieldCount);
      values.forEach((field, value) -> tValues.set(field, value.toString()));

      batchHolder.put(tKey, tValues);

      recordsInBatch++;
      processedRecords++;

      if (recordsInBatch < batchSize && processedRecords < recordsCount) {
        return Status.BATCHED_OK;
      }

      kvView.putAll(null, batchHolder);
      batchHolder = new LinkedHashMap<>();
      recordsInBatch = 0;

      return Status.OK;
    } catch (Exception e) {
      LOG.error(String.format("Error inserting key: %s", key), e);

      return Status.ERROR;
    }
  }

  /** {@inheritDoc} */
  @Override
  public Status read(String table, List<String> keys, List<Set<String>> fields,
                     HashMap<String, Map<String, ByteIterator>> result) {
    try {
      List<Tuple> tKeys = new ArrayList<>();
      keys.forEach(k -> tKeys.add(Tuple.create(1).set(PRIMARY_COLUMN_NAME, k)));

      Map<Tuple, Tuple> tResult = kvView.getAll(null, tKeys);

      final Tuple tKey = Tuple.create(1);
      final Set<String> allFieldsSet = new HashSet<>(FIELDS);

      for (int i = 0; i < keys.size(); i++) {
        String key = keys.get(i);

        final Set<String> fieldsForKey;
        if (fields == null || fields.isEmpty() ||
            fields.get(i) == null || fields.get(i).isEmpty()) {
          fieldsForKey = allFieldsSet;
        } else {
          fieldsForKey = fields.get(i);
        }

        Tuple tValue = tResult.get(tKey.set(PRIMARY_COLUMN_NAME, key));

        if (tValue == null) {
          return Status.NOT_FOUND;
        }

        Map<String, ByteIterator> value = new LinkedHashMap<>();
        for (String field : fieldsForKey) {
          if (!Objects.equals(tValue.stringValue(field), null)) {
            value.put(field, new StringByteIterator(tValue.stringValue(field)));
          }
        }

        result.put(key, value);
      }

      return Status.OK;
    } catch (Exception e) {
      LOG.error(String.format("Error reading keys: %s", keys), e);

      return Status.ERROR;
    }
  }

  /** {@inheritDoc} */
  @Override
  public Status read(String table, String key, Set<String> fields,
                     Map<String, ByteIterator> result) {
    return Status.BATCHED_OK;
  }

  /** {@inheritDoc} */
  @Override
  public Status update(String table, String key,
                       Map<String, ByteIterator> values) {
    return Status.NOT_IMPLEMENTED;
  }

  /** {@inheritDoc} */
  @Override
  public Status delete(String table, String key) {
    try {
      kvView.remove(null, Tuple.create(1).set(PRIMARY_COLUMN_NAME, key));

      return Status.OK;
    } catch (Exception e) {
      LOG.error(String.format("Error deleting key: %s ", key), e);
    }

    return Status.ERROR;
  }
}
