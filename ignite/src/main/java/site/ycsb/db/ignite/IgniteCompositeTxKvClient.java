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
package site.ycsb.db.ignite;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import site.ycsb.ByteIterator;
import site.ycsb.DBException;
import site.ycsb.Status;

/**
 * Ignite key-value client with transactions that include extra table with composite key.
 */
public class IgniteCompositeTxKvClient extends IgniteTxKvClient {
  public static final String TAGS_TABLE_NAME = "tagstable";

  protected static IgniteCache<CompositeKey, BinaryObject> tagsCache;

  /** {@inheritDoc} */
  @Override
  public void init() throws DBException {
    super.init();

    tagsCache = ignite.cache(TAGS_TABLE_NAME).withKeepBinary();
  }

  /** {@inheritDoc} */
  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    try {
      return kvRead(key, fields, result);
    } catch (Exception e) {
      LOG.error(String.format("Error reading key: %s", key), e);

      return Status.ERROR;
    }
  }

  /** {@inheritDoc} */
  @Override
  protected void kvInsert(String key, Map<String, ByteIterator> values) {
    BinaryObject binObj = convert(values);
    getCache(key).getAndPut(key, binObj);

    CompositeKey tagKey = new CompositeKey(key);
    Map<CompositeKey, BinaryObject> tagsMap = new HashMap<>();
    tagsMap.put(tagKey, binObj);
    tagsCache.putAll(tagsMap);
  }

  /** {@inheritDoc} */
  @Override
  protected Status kvRead(String key, Set<String> fields, Map<String, ByteIterator> result) {
    BinaryObject binObj = getCache(key).get(key);

    if (binObj == null) {
      LOG.warn("Key '{}' not found for get operation.", key);
    }

    return convert(binObj, fields, result);
  }

  /** {@inheritDoc} */
  @Override
  protected void kvUpdate(String key, Map<String, ByteIterator> values) {
    BinaryObject binObj = convert(values);
    BinaryObject oldValue = getCache(key).get(key);
    getCache(key).put(key, binObj);

    if (oldValue == null) {
      LOG.warn("Key '{}' not found for getAndPut operation.", key);
    }

    CompositeKey tagKey = new CompositeKey(key);
    Map<CompositeKey, BinaryObject> tagsMap = new HashMap<>();
    tagsMap.put(tagKey, binObj);
    tagsCache.putAll(tagsMap);
  }

  /** {@inheritDoc} */
  @Override
  protected void kvDelete(String key) {
    BinaryObject oldValue = getCache(key).getAndRemove(key);

    if (oldValue == null) {
      LOG.warn("Key '{}' not found for remove operation.", key);
    }

    CompositeKey tagKey = new CompositeKey(key);
    Set<CompositeKey> tagSet = new HashSet<>();
    tagSet.add(tagKey);
    tagsCache.removeAll(tagSet);
  }
}
