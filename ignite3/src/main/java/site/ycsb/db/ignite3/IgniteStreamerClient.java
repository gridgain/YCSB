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

import static site.ycsb.Client.parseLongWithModifiers;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.SubmissionPublisher;
import org.apache.ignite.table.DataStreamerItem;
import org.apache.ignite.table.DataStreamerOptions;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import site.ycsb.ByteIterator;
import site.ycsb.Client;
import site.ycsb.DBException;
import site.ycsb.Status;

/**
 * Ignite3 Data Streamer client.
 */
public class IgniteStreamerClient extends IgniteAbstractClient {
  /** Logger. */
  private static final Logger LOG = LogManager.getLogger(IgniteStreamerClient.class);

  /** Data streamer auto-flush interval in ms. */
  protected static final int DATA_STREAMER_AUTOFLUSH_INTERVAL = 5000;

  /** List of record view publishers. */
  protected List<SubmissionPublisher<DataStreamerItem<Tuple>>> rvPublishers;

  /** List of record view data streamer completable futures. */
  protected List<CompletableFuture<Void>> rvStreamerFutures;

  /** Batch size. */
  protected long batchSize;

  /** {@inheritDoc} */
  @Override
  public void init() throws DBException {
    super.init();

    batchSize = parseLongWithModifiers(getProperties().getProperty(
        Client.BATCH_SIZE_PROPERTY, Client.DEFAULT_BATCH_SIZE));

    DataStreamerOptions dsOptions = DataStreamerOptions.builder()
        .pageSize((int) batchSize)
        .autoFlushInterval(DATA_STREAMER_AUTOFLUSH_INTERVAL)
        .build();

    rvPublishers = new ArrayList<>(tableCount);
    rvStreamerFutures = new ArrayList<>(tableCount);

    for (RecordView<Tuple> rView : rViews) {
      SubmissionPublisher<DataStreamerItem<Tuple>> publisher = new SubmissionPublisher<>();
      rvPublishers.add(publisher);
      rvStreamerFutures.add(rView.streamData(publisher, dsOptions));
    }
  }

  /** {@inheritDoc} */
  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    return Status.NOT_IMPLEMENTED;
  }

  /** {@inheritDoc} */
  @Override
  public Status batchInsert(String table, List<String> keys, List<Map<String, ByteIterator>> values) {
    try {
      for (int i = 0; i < keys.size(); i++) {
        Tuple value = Tuple.create(fieldCount + 1);
        value.set(PRIMARY_COLUMN_NAME, keys.get(i));
        values.get(i).forEach((k, v) -> value.set(k, v.toString()));

        getPublisher(keys.get(0)).submit(DataStreamerItem.of(value));
      }

      return Status.OK;
    } catch (Exception e) {
      LOG.error("Error inserting batch of keys.", e);

      return Status.ERROR;
    }
  }

  /** {@inheritDoc} */
  @Override
  public Status read(String table, String key, Set<String> fields,
                     Map<String, ByteIterator> result) {
    return Status.NOT_IMPLEMENTED;
  }

  /** {@inheritDoc} */
  @Override
  public Status batchRead(String table, List<String> keys, List<Set<String>> fields,
                          List<Map<String, ByteIterator>> result) {
    return Status.NOT_IMPLEMENTED;
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
    return Status.NOT_IMPLEMENTED;
  }

  /** {@inheritDoc} */
  @Override
  public void cleanup() throws DBException {
    rvPublishers.stream().parallel().forEach(SubmissionPublisher::close);

    rvStreamerFutures.forEach(CompletableFuture::join);

    super.cleanup();
  }

  /**
   * Get table streamer publisher for key.
   *
   * @param key Key.
   */
  protected SubmissionPublisher<DataStreamerItem<Tuple>> getPublisher(String key) {
    int index = tableCount <= 1 ?
        0 : //skip the key processing for choosing publisher in case of 1 test table
        (int) (Long.parseLong(key.substring(4)) % tableCount); //CoreWorkload uses key hash with prefix "user"

    return rvPublishers.get(index);
  }
}
