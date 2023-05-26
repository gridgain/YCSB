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
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.Session;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Tuple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;

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

  /**
   * TODO: fix hardcoded path.
   */
  protected static final String NODE_CFG_PATH =
      "/home/ivan/Projects/YCSB-gg/ignite3/src/main/resources/ignite-config.json";

  protected static final Path NODE_WORK_DIR =
      Paths.get("/tmp/ignite3-ycsb", String.valueOf(System.currentTimeMillis()));

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
      } catch (Exception e) {
        throw new DBException(e);
      }
    }
  }

  private void initIgniteClientNode(String host, String ports) throws DBException {
    node = IgniteClient.builder().addresses(host + ":" + ports).build();
    createTestTable(node);
    kvView = node.tables().table(cacheName).keyValueView();
    if (kvView == null) {
      throw new DBException("Failed to find cache: " + cacheName);
    }
  }

  private void initEmbeddedServerNode() throws DBException {
    node = startIgniteNode();
    createTestTable(node);
    kvView = node.tables().table(cacheName).keyValueView();
    if (kvView == null) {
      throw new DBException("Failed to find cache: " + cacheName);
    }
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
      cfgPath = Paths.get(NODE_CFG_PATH);
      Path workDir = NODE_WORK_DIR;

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

  private void createTestTable(Ignite node0) throws DBException {
    try {
      List<String> fieldnames = new ArrayList<>();

      for (int i = 0; i < fieldCount; i++) {
        fieldnames.add(fieldPrefix + i + " VARCHAR");       //VARBINARY(6)
      }
      String request = "CREATE TABLE IF NOT EXISTS " + cacheName + " ("
          + PRIMARY_COLUMN_NAME + " VARCHAR PRIMARY KEY, "
          + String.join(", ", fieldnames)
          + ");";
      LOG.info("Create table request: {}", request);

      try (Session ses = node0.sql().createSession()) {
        ses.execute(null, request);
      }
    } catch (Exception e) {
      throw new DBException(e);
    }
  }

  private long entriesInTable(Ignite ignite0, String tableName) throws DBException {
    long entries = 0L;

    try (Session session = ignite0.sql().createSession()) {
      ResultSet<SqlRow> res = session.execute(null, "SELECT COUNT(*) FROM " + tableName + ";");

      while (res.hasNext()) {
        SqlRow row = res.next();

        entries = row.longValue(0);
      }
    } catch (Exception e) {
      throw new DBException("Failed to get number of entries in table " + tableName, e);
    }

    return entries;
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
          long recordsCnt = entriesInTable(node, cacheName);
          LOG.info("Records in table {}: {}", cacheName, recordsCnt);

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
