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


import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Tuple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Ignite abstract client.
 * <p>
 * See {@code ignite/README.md} for details.
 */
public abstract class IgniteAbstractClient extends DB {
  /**
   *
   */
  protected static Logger log = LogManager.getLogger(IgniteAbstractClient.class);
  protected static final int FIELDS_COUNT = 10;
  protected static final String DEFAULT_CACHE_NAME = "usertable";
  protected static final String HOSTS_PROPERTY = "hosts";
  protected static final String PORTS_PROPERTY = "ports";

  /**
   * Ignite cluster.
   */
  protected static KeyValueView<Tuple, Tuple> kvView = null;
  protected static IgniteClient client = null;

  /**
   * Count the number of times initialized to teardown on the last
   * {@link #cleanup()}.
   */
  private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);

  /**
   * Debug flag.
   */
  protected static boolean debug = false;


  /**
   * Initialize any state for this DB. Called once per DB instance; there is one
   * DB instance per client thread.
   */
  @Override
  public void init() throws DBException {
    INIT_COUNT.incrementAndGet();

    synchronized (IgniteAbstractClient.class) {
      if (client != null) {
        return;
      }

      try {
        String host = getProperties().getProperty(HOSTS_PROPERTY);
        if (host == null) {
          throw new DBException(String.format(
              "Required property \"%s\" missing for Ignite Cluster",
              HOSTS_PROPERTY));
        }

        String ports = getProperties().getProperty(PORTS_PROPERTY, "10800");

        // <-- this block exists because there is no way to create a cache from the configuration.
        Class.forName("org.apache.ignite.internal.jdbc.IgniteJdbcDriver");
        List<String> fieldnames = new ArrayList<>();
        for (int i = 0; i < FIELDS_COUNT; i++) {
          fieldnames.add("field" + i + " VARCHAR");       //VARBINARY(6)
        }
        String request = "CREATE TABLE IF NOT EXISTS " + DEFAULT_CACHE_NAME + " ("
            + "yscb_key VARCHAR PRIMARY KEY, "
            + String.join(", ", fieldnames)
            + ");";
        System.out.println(request);
        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://" + host + ":" + ports);
             Statement stmt = conn.createStatement()) {
          stmt.executeUpdate(request);
        }
        // -->

        client = IgniteClient.builder().addresses(host + ":" + ports).build();
        kvView = client.tables().table(DEFAULT_CACHE_NAME).keyValueView();
        if (kvView == null) {
          throw new Exception("Failed to find cache: " + DEFAULT_CACHE_NAME);
        }
      } catch (Exception e) {
        throw new DBException(e);
      }
    }
  }

  /**
   * Cleanup any state for this DB. Called once per DB instance; there is one DB
   * instance per client thread.
   */
  @Override
  public void cleanup() throws DBException {
    synchronized (IgniteAbstractClient.class) {
      int curInitCount = INIT_COUNT.decrementAndGet();

      if (curInitCount <= 0) {
        try {
          client.close();
          client = null;
        } catch (Exception e) {
          throw new DBException(e);
        }
      }
    }
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
                     Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    return Status.NOT_IMPLEMENTED;
  }
}
