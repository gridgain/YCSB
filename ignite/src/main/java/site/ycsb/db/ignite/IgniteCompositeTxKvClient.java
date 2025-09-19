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
  protected void put(String key, Map<String, ByteIterator> values) {
    BinaryObject binObj = convert(values);
    getCache(key).put(key, binObj);

    CompositeKey tagKey = new CompositeKey(key);
    tagsCache.put(tagKey, binObj);
  }

  /** {@inheritDoc} */
  @Override
  protected void getAndPut(String key, Map<String, ByteIterator> values) {
    getCache(key).invoke(key, new Updater(values));

    CompositeKey tagKey = new CompositeKey(key);
    BinaryObject binObj = convert(values);
    tagsCache.getAndPut(tagKey, binObj);
  }

  /** {@inheritDoc} */
  @Override
  protected Status get(String key, Set<String> fields, Map<String, ByteIterator> result) {
    getCache(key).get(key); //ignored

    CompositeKey tagKey = new CompositeKey(key);
    BinaryObject binObj = tagsCache.get(tagKey);
    return convert(binObj, fields, result);
  }

  /** {@inheritDoc} */
  @Override
  protected void remove(String key) {
    getCache(key).remove(key);

    CompositeKey tagKey = new CompositeKey(key);
    tagsCache.remove(tagKey);
  }
}
