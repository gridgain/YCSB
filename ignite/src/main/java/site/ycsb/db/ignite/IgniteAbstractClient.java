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

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.logger.log4j2.Log4J2Logger;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.workloads.CoreWorkload;

/**
 * Ignite abstract client.
 * <p>
 * See {@code ignite/README.md} for details.
 */
public abstract class IgniteAbstractClient extends DB {
  /** */
  protected static final Logger LOG = LogManager.getLogger(IgniteAbstractClient.class);

  protected static final String PRIMARY_COLUMN_NAME = "ycsb_key";
  protected static final String HOSTS_PROPERTY = "hosts";
  protected static final String CLIENT_NODE_NAME = "YCSB client node";
  protected static final List<String> FIELDS = new ArrayList<>();
  protected static final Set<String> ACCESS_METHODS =
      new HashSet<>(Arrays.asList("kv", "sql", "jdbc", "txkv", "txsql", "txjdbc"));

  /**
   * Count the number of times initialized to teardown on the last
   * {@link #cleanup()}.
   */
  protected static final AtomicInteger INIT_COUNT = new AtomicInteger(0);

  private static volatile boolean initCompleted = false;

  private static volatile boolean externalIgnite = false;

  /** Ignite cluster. */
  protected static Ignite ignite = null;

  /** Ignite cache to store key-values. */
  protected static List<IgniteCache<String, BinaryObject>> caches;

  /** Debug flag. */
  protected static boolean debug = false;

  protected static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

  /** Whether to shut down externally provided Ignite instance. */
  protected static boolean shutdownExternalIgnite = false;

  /** Start an embedded Ignite node instead of connecting to an external one. */
  protected static boolean useEmbeddedIgnite = false;

  protected static String tableNamePrefix;

  protected static List<String> tableNames = new ArrayList<>();

  /**
   * Used to specify the number of test tables.
   * Table names will be formed from TABLENAME_PROPERTY_DEFAULT value with adding index at the end.
   */
  protected static int tableCount;

  protected static int fieldCount;

  protected static String fieldPrefix;

  protected static int indexCount;

  protected static String indexOptions;

  protected static String hosts;

  protected static Path embeddedIgniteWorkDir;

  /** Node access method ("kv" - Key-Value [default], "sql" - Thick Java client SQL, "jdbc" - JDBC). */
  protected static String accessMethod = "kv";

  /** Transaction concurrency. */
  protected static TransactionConcurrency txConcurrency;

  /** Transaction isolation. */
  protected static TransactionIsolation txIsolation;

  /** Transaction. */
  protected Transaction tx;

  /**
   * Set Ignite instance to work with.
   *
   * @param igniteSrv Ignite instance.
   */
  public static void setIgniteServer(Ignite igniteSrv) {
    ignite = igniteSrv;
    externalIgnite = true;
  }

  /**
   * Initialize any state for this DB. Called once per DB instance; there is one
   * DB instance per client thread.
   */
  @Override
  public void init() throws DBException {

    // Keep track of number of calls to init (for later cleanup)
    INIT_COUNT.incrementAndGet();

    // Synchronized so that we only have a single
    // cluster/session instance for all the threads.
    synchronized (INIT_COUNT) {

      if (initCompleted) {
        return;
      }

      initProperties(getProperties());

      initIgnite();

      initTestCaches();

      createIndexes();

      initCompleted = true;
    }
  }

  /**
   * Init property values.
   *
   * @param properties Properties.
   */
  private void initProperties(Properties properties) throws DBException {
    try {
      debug = Boolean.parseBoolean(properties.getProperty("debug", "false"));
      shutdownExternalIgnite = Boolean.parseBoolean(properties.getProperty("shutdownIgnite", "false"));
      useEmbeddedIgnite = Boolean.parseBoolean(properties.getProperty("useEmbedded", "false"));

      tableNamePrefix = properties.getProperty(
          CoreWorkload.TABLENAME_PROPERTY, CoreWorkload.TABLENAME_PROPERTY_DEFAULT);
      tableCount = IgniteParam.TABLE_COUNT.getValue(properties);

      if (tableCount <= 1) {
        tableNames.add(tableNamePrefix);
      } else {
        for (int i = 0; i < tableCount; i++) {
          tableNames.add(tableNamePrefix + i);
        }
      }

      fieldCount = Integer.parseInt(properties.getProperty(
          CoreWorkload.FIELD_COUNT_PROPERTY, CoreWorkload.FIELD_COUNT_PROPERTY_DEFAULT));
      fieldPrefix = properties.getProperty(
          CoreWorkload.FIELD_NAME_PREFIX, CoreWorkload.FIELD_NAME_PREFIX_DEFAULT);

      for (int i = 0; i < fieldCount; i++) {
        FIELDS.add(fieldPrefix + i);
      }

      indexCount = Integer.parseInt(properties.getProperty(
          CoreWorkload.INDEX_COUNT_PROPERTY, CoreWorkload.INDEX_COUNT_PROPERTY_DEFAULT));
      indexOptions = properties.getProperty(CoreWorkload.INDEX_OPTIONS_PROPERTY, "");
      String txConcurrencyStr = IgniteParam.TX_CONCURRENCY.getValue(properties);
      txConcurrency = TransactionConcurrency.valueOf(txConcurrencyStr.trim().toUpperCase());
      String txIsolationStr = IgniteParam.TX_ISOLATION.getValue(properties);
      txIsolation = TransactionIsolation.valueOf(txIsolationStr.trim().toUpperCase());

      if (indexCount > fieldCount) {
        throw new DBException(String.format(
            "Indexed fields count (%s=%s) should be less or equal to fields count (%s=%s)",
            CoreWorkload.INDEX_COUNT_PROPERTY, indexCount, CoreWorkload.FIELD_COUNT_PROPERTY, fieldCount));
      }
    } catch (Exception e) {
      throw new DBException(e);
    }
  }

  /**
   * - Start embedded Ignite node (if needed).
   * - Get Ignite client (if needed).
   * - Activate cluster.
   */
  private void initIgnite() throws DBException {
    if (ignite == null) {
      try {
        if (useEmbeddedIgnite) {
          ignite = getEmbeddedServerNode();
        } else {
          ignite = getIgniteClientNode();
        }
      } catch (Exception e) {
        throw new DBException(e);
      }
    }

    LOG.info("Activate Ignite cluster.");
    ignite.cluster().state(ClusterState.ACTIVE);
  }

  /**
   * Init test caches.
   */
  private void initTestCaches() throws DBException {
    caches = new ArrayList<>(tableCount);

    for (String tableName : tableNames) {
      IgniteCache<String, BinaryObject> cache = ignite.cache(tableName).withKeepBinary();

      if (cache == null) {
        throw new DBException(new IgniteCheckedException("Failed to find cache " + tableName));
      }

      caches.add(cache);
    }
  }

  /**
   * Start Ignite thick client.
   */
  private Ignite getIgniteClientNode() throws DBException, IgniteCheckedException {
    IgniteConfiguration igcfg = new IgniteConfiguration();
    igcfg.setIgniteInstanceName(CLIENT_NODE_NAME);

    hosts = getProperties().getProperty(HOSTS_PROPERTY);
    if (hosts == null) {
      throw new DBException(String.format(
          "Required property \"%s\" missing for Ignite Cluster",
          HOSTS_PROPERTY));
    }

    System.setProperty("IGNITE_QUIET", "false");

    TcpDiscoverySpi disco = new TcpDiscoverySpi();

    Collection<String> addrs = new LinkedHashSet<>(Arrays.asList(hosts.split(",")));

    ((TcpDiscoveryVmIpFinder) ipFinder).setAddresses(addrs);
    disco.setIpFinder(ipFinder);

    igcfg.setDiscoverySpi(disco);
    igcfg.setNetworkTimeout(2000);
    igcfg.setClientMode(true);

    Log4J2Logger logger = new Log4J2Logger(this.getClass().getClassLoader().getResource("log4j2.xml"));
    igcfg.setGridLogger(logger);

    LOG.info("Start Ignite client node.");
    return Ignition.start(igcfg);
  }

  /**
   * Start embedded Ignite node.
   */
  private Ignite getEmbeddedServerNode() throws IOException {
    if (!ACCESS_METHODS.contains(accessMethod.toLowerCase())) {
      throw new RuntimeException("Wrong value for parameter 'accessMethod'. "
          + "Expected one of " + ACCESS_METHODS + " . Actual: " + accessMethod);
    }

    String workDirProperty = getProperties().getProperty("workDir", "./ignite-ycsb-work");
    embeddedIgniteWorkDir = Paths.get(workDirProperty);

    String cfgFileName = String.format("emb-%s.xml", accessMethod.toLowerCase());
    Path cfgPath = embeddedIgniteWorkDir.resolve(cfgFileName);

    Files.createDirectories(embeddedIgniteWorkDir);
    try (InputStream cfgIs = getClass().getClassLoader().getResourceAsStream(cfgFileName)) {
      Files.copy(Objects.requireNonNull(cfgIs), cfgPath, StandardCopyOption.REPLACE_EXISTING);
    }

    LOG.info("Start embedded Ignite node.");
    return Ignition.start(cfgPath.toString());
  }

  /**
   * Create indexes if needed.
   */
  private void createIndexes() {
    List<String> createIndexesReqs = createIndexesSQL();

    if (!createIndexesReqs.isEmpty()) {
      LOG.info(String.format("Creating %s indexes.", indexCount));

      createIndexesReqs.forEach(idxReq -> {
          LOG.info("SQL line: {}", idxReq);

          caches.get(0).query(new SqlFieldsQuery(idxReq)).getAll();
        });
    }
  }

  /**
   * Prepare the creation indexes SQL lines.
   */
  private List<String> createIndexesSQL() {
    if (indexCount <= 0) {
      return Collections.emptyList();
    }

    List<String> createIndexesReqs = new ArrayList<>();

    FIELDS.subList(0, indexCount).forEach(field ->
        createIndexesReqs.add(
            String.format("CREATE INDEX IF NOT EXISTS idx_%s ON %s (%s)%s;",
                field, tableNamePrefix, field, indexOptions)));

    return createIndexesReqs;
  }

  /**
   * Get cache for key.
   *
   * @param key Key.
   */
  protected IgniteCache<String, BinaryObject> getCache(String key) {
    int index = tableCount <= 1 ?
        0 : //skip the key processing for choosing view in case of 1 test table
        (int) (Long.parseLong(key.substring(4)) % tableCount); //CoreWorkload uses key hash with prefix "user"

    return caches.get(index);
  }

  /**
   * Cleanup any state for this DB. Called once per DB instance; there is one DB
   * instance per client thread.
   */
  @Override
  public void cleanup() throws DBException {
    synchronized (INIT_COUNT) {
      final int curInitCount = INIT_COUNT.decrementAndGet();

      if (curInitCount <= 0 && (!externalIgnite || shutdownExternalIgnite)) {
        ignite.close();
        ignite = null;
      }

      if (curInitCount < 0) {
        // This should never happen.
        throw new DBException(
            String.format("initCount is negative: %d", curInitCount));
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  public Status scan(String table, String startkey, int recordcount,
                     Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    return Status.NOT_IMPLEMENTED;
  }

  /**
   * Start transaction.
   */
  protected void txStart() {
    tx = ignite.transactions().txStart(txConcurrency, txIsolation);
  }
}
