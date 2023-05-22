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

import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.InitParameters;
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
import site.ycsb.workloads.CoreWorkload;

/**
 * Ignite abstract client.
 * <p>
 * See {@code ignite/README.md} for details.
 */
public abstract class IgniteAbstractClient extends DB {

  private static final Logger LOG = LogManager.getLogger(IgniteAbstractClient.class);

  protected static String cacheName;

  protected static int fieldCount;

  protected static String fieldPrefix;

  protected static final String HOSTS_PROPERTY = "hosts";

  protected static final String PORTS_PROPERTY = "ports";

  protected static final String PRIMARY_COLUMN_NAME = "yscb_key";

  /**
   * Single Ignite thin client per process.
   */
  protected static Ignite node;

  protected static KeyValueView<Tuple, Tuple> kvView;

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
   * Start embedded Ignite node instead of using external one.
   */
  protected static boolean useEmbeddedIgnite = false;

  /**
   * Initialize any state for this DB. Called once per DB instance; there is one
   * DB instance per client thread.
   */
  @Override
  public void init() throws DBException {
    INIT_COUNT.incrementAndGet();

    synchronized (IgniteAbstractClient.class) {
      if (node != null) {
        return;
      }

      try {
        debug = Boolean.parseBoolean(getProperties().getProperty("debug", "false"));
        useEmbeddedIgnite = Boolean.parseBoolean(getProperties().getProperty("useEmbedded", "false"));

        cacheName = getProperties().getProperty(CoreWorkload.TABLENAME_PROPERTY,
            CoreWorkload.TABLENAME_PROPERTY_DEFAULT);
        fieldCount = Integer.parseInt(getProperties().getProperty(
            CoreWorkload.FIELD_COUNT_PROPERTY, CoreWorkload.FIELD_COUNT_PROPERTY_DEFAULT));
        fieldPrefix = getProperties().getProperty(CoreWorkload.FIELD_NAME_PREFIX,
            CoreWorkload.FIELD_NAME_PREFIX_DEFAULT);

        String host = getProperties().getProperty(HOSTS_PROPERTY);
        if (host == null) {
          throw new DBException(String.format(
              "Required property \"%s\" missing for Ignite Cluster",
              HOSTS_PROPERTY));
        }
        String ports = getProperties().getProperty(PORTS_PROPERTY, "10800");

        if (useEmbeddedIgnite) {
          initEmbeddedServerNode();
        } else {
          initIgniteClientNode(host, ports);
        }

        if (kvView == null) {
          throw new Exception("Failed to find cache: " + cacheName);
        }

        createTestTable(host, ports);
      } catch (Exception e) {
        throw new DBException(e);
      }
    }
  }

  private void initIgniteClientNode(String host, String ports) {
    node = IgniteClient.builder().addresses(host + ":" + ports).build();
    kvView = node.tables().table(cacheName).keyValueView();
  }

  private void initEmbeddedServerNode() throws DBException {
    node = startIgniteNode();
    kvView = node.tables().table(cacheName).keyValueView();
  }

  private static Ignite startIgniteNode() throws DBException {
    Ignite ignite;
    String clusterName = "myCluster";
    String nodeName = "defaultNode";

    try {
      URL cfgUrl = IgniteAbstractClient.class.getClassLoader().getResource("ignite-config.json");
      assert cfgUrl != null;
      Path cfgPath = Paths.get(cfgUrl.getPath());

      // TODO: fixme
      cfgPath = Paths.get("/home/ivan/Projects/YCSB-gg/ignite3/src/main/resources/ignite-config.json");
      Path workDir = Paths.get("/tmp/ignite3-ycsb");

      LOG.info("Starting Ignite node {} in {} with config {}", nodeName, workDir, cfgPath);
      CompletableFuture<Ignite> fut = IgnitionManager.start(nodeName, cfgPath, workDir);

      InitParameters initParameters = InitParameters.builder()
          .destinationNodeName(nodeName)
          .metaStorageNodeNames(Collections.singletonList(nodeName))
          .clusterName(clusterName)
          .build();
      IgnitionManager.init(initParameters);

      ignite = fut.join();
    } catch (Exception e) {
      throw new DBException("Failed to start an embedded Ignite node", e);
    }

    return ignite;
  }

  private void createTestTable(String host, String ports) throws DBException {
    try {
      Class.forName("org.apache.ignite.internal.jdbc.IgniteJdbcDriver");

      List<String> fieldnames = new ArrayList<>();

      for (int i = 0; i < fieldCount; i++) {
        fieldnames.add(fieldPrefix + i + " VARCHAR");       //VARBINARY(6)
      }
      String request = "CREATE TABLE IF NOT EXISTS " + cacheName + " ("
          + PRIMARY_COLUMN_NAME + " VARCHAR PRIMARY KEY, "
          + String.join(", ", fieldnames)
          + ");";
      LOG.info("Create table request: {}", request);

      try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://" + host + ":" + ports);
          Statement stmt = conn.createStatement()) {
        stmt.executeUpdate(request);
      }
    } catch (Exception e) {
      throw new DBException(e);
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
          node.close();
          node = null;
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
