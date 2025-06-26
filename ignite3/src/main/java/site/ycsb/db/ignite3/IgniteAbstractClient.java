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

import static site.ycsb.Client.DO_TRANSACTIONS_PROPERTY;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteServer;
import org.apache.ignite.InitParameters;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.TransactionOptions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.workloads.CoreWorkload;

/**
 * Ignite abstract client.
 */
public abstract class IgniteAbstractClient extends DB {
  protected static final String HOSTS_PROPERTY = "hosts";

  protected static final String PRIMARY_COLUMN_NAME = "ycsb_key";

  protected static final String DEFAULT_ZONE_NAME = "Z1";

  protected static final String DEFAULT_STORAGE_PROFILE_NAME = "default";

  protected static final String DEFAULT_COLUMNAR_PROFILE_NAME = "myColumnarStore";

  protected static final long TABLE_CREATION_TIMEOUT_SECONDS = 30L;

  protected static final String NODES_FILTER_ATTRIBUTE = "ycsbFilter";

  private static final Logger LOG = LogManager.getLogger(IgniteAbstractClient.class);

  protected String zoneName;

  protected String tableNamePrefix;

  protected List<String> tableNames = new ArrayList<>();

  protected int fieldCount;

  protected int fieldLength;

  protected int indexCount;

  protected String indexType;

  protected String fieldPrefix;

  protected boolean useLimitedVarchar;

  protected final List<String> valueFields = new ArrayList<>();

  /**
   * Single Ignite client per process.
   */
  protected static Ignite ignite;

  protected static IgniteClient igniteClient;

  protected static IgniteServer igniteServer;

  protected String hosts;

  protected List<KeyValueView<Tuple, Tuple>> kvViews;

  protected List<RecordView<Tuple>> rViews;

  /**
   * Count the number of times initialized to teardown on the last
   * {@link #cleanup()}.
   */
  private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);

  private static volatile boolean initCompleted = false;

  private static volatile boolean externalIgnite = false;

  /**
   * Used to print more information into logs for debugging purposes.
   */
  protected static boolean debug = false;

  /**
   * Whether to perform "run" phase ({@code true}) or "load" phase ({@code false}).
   */
  protected static boolean isRunPhase;

  /**
   * Whether to shut down externally provided Ignite instance.
   */
  protected static boolean shutdownExternalIgnite = false;

  /**
   * Start an embedded Ignite node instead of connecting to an external one.
   */
  protected static boolean useEmbeddedIgnite = false;

  protected static Path embeddedIgniteWorkDir;

  /**
   * Used to disable FSYNC by passing different config to an embedded Ignite node.
   */
  protected static boolean disableFsync = false;

  /**
   * Create table with columnar secondary storage profile and use it to fetch data.
   */
  protected static boolean useColumnar = false;

  /**
   * Used to choose storage engine (e.g., 'aipersist' or 'rocksdb').
   * @deprecated Removed in <a href="https://ggsystems.atlassian.net/browse/IGN-23905">IGN-23905</a>
   */
  @Deprecated
  protected static String dbEngine;

  /**
   * Used to choose storage profile (e.g., 'default', 'aimem', 'aipersist', 'rocksdb').
   */
  protected static String storageProfile;

  /**
   * Used to choose secondary storage profile (e.g., 'columnar').
   */
  protected static String secondaryStorageProfile;

  /**
   * Used to choose replication factor value.
   */
  protected static String replicas;

  /**
   * Used to choose partitions value.
   */
  protected static String partitions;

  /**
   * Used to filter nodes by NODES_FILTER_ATTRIBUTE.
   */
  protected static String nodesFilter;

  /**
   * Used to set explicit RO transactions (used in pair with Tx clients and 'batchsize').
   */
  protected static boolean txReadOnly;

  /**
   * Explicit transaction options (for example, use RO transactions for Tx clients).
   */
  protected static TransactionOptions txOptions;

  /**
   * Used to specify the number of test tables.
   * Table names will be formed from TABLENAME_PROPERTY_DEFAULT value with adding index at the end.
   */
  protected static int tableCount;

  /**
   * Set IgniteServer instance to work with.
   *
   * @param igniteSrv Ignite.
   */
  public static void setIgniteServer(IgniteServer igniteSrv) {
    igniteServer = igniteSrv;
    ignite = igniteServer.api();
    externalIgnite = true;
  }

  /** {@inheritDoc} */
  @Override
  public void init() throws DBException {
    INIT_COUNT.incrementAndGet();

    initProperties(getProperties());

    synchronized (IgniteAbstractClient.class) {
      if (!initCompleted) {
        initIgnite(useEmbeddedIgnite);

        if (!isRunPhase) {
          createZone();

          createTables();

          createIndexes();
        }

        initCompleted = true;
      }
    }

    initViews();
  }

  /**
   * Init property values.
   *
   * @param properties Properties.
   */
  public void initProperties(Properties properties) throws DBException {
    try {
      debug = Ignite3Param.DEBUG.getValue(properties);
      isRunPhase = Boolean.parseBoolean(properties.getProperty(DO_TRANSACTIONS_PROPERTY, "true"));
      shutdownExternalIgnite = Ignite3Param.SHUTDOWN_IGNITE.getValue(properties);
      useEmbeddedIgnite = Ignite3Param.USE_EMBEDDED.getValue(properties);
      disableFsync = Ignite3Param.DISABLE_FSYNC.getValue(properties);
      txOptions = new TransactionOptions().readOnly(Ignite3Param.TX_READ_ONLY.getValue(properties));
      dbEngine = Ignite3Param.DB_ENGINE.getValue(properties);
      storageProfile = Ignite3Param.STORAGE_PROFILES.getValue(properties);
      secondaryStorageProfile = Ignite3Param.SECONDARY_STORAGE_PROFILE.getValue(properties);

      // backward compatibility of setting 'dbEngine' as storage engine name only.
      if (storageProfile.isEmpty() && !dbEngine.isEmpty()) {
        storageProfile = dbEngine;
      }

      replicas = Ignite3Param.REPLICAS.getValue(properties);
      partitions = Ignite3Param.PARTITIONS.getValue(properties);
      nodesFilter = Ignite3Param.NODES_FILTER.getValue(properties);
      useColumnar = Ignite3Param.USE_COLUMNAR.getValue(properties);

      boolean doCreateZone = !storageProfile.isEmpty() || !replicas.isEmpty() || !partitions.isEmpty()
          || !nodesFilter.isEmpty() || useColumnar;
      zoneName = doCreateZone ? DEFAULT_ZONE_NAME : "";

      tableNamePrefix = properties.getProperty(
          CoreWorkload.TABLENAME_PROPERTY, CoreWorkload.TABLENAME_PROPERTY_DEFAULT);
      tableCount = Ignite3Param.TABLE_COUNT.getValue(properties);

      if (tableCount <= 1) {
        tableNames.add(tableNamePrefix);
      } else {
        for (int i = 0; i < tableCount; i++) {
          tableNames.add(tableNamePrefix + i);
        }
      }

      String workDirProperty = Ignite3Param.WORK_DIR.getValue(properties);
      embeddedIgniteWorkDir = Paths.get(workDirProperty);

      fieldCount = Integer.parseInt(properties.getProperty(
          CoreWorkload.FIELD_COUNT_PROPERTY, CoreWorkload.FIELD_COUNT_PROPERTY_DEFAULT));
      fieldLength = Integer.parseInt(properties.getProperty(
          CoreWorkload.FIELD_LENGTH_PROPERTY, CoreWorkload.FIELD_LENGTH_PROPERTY_DEFAULT));
      fieldPrefix = properties.getProperty(
          CoreWorkload.FIELD_NAME_PREFIX, CoreWorkload.FIELD_NAME_PREFIX_DEFAULT);
      useLimitedVarchar = Boolean.parseBoolean(properties.getProperty(
          CoreWorkload.USE_LIMITED_VARCHAR_PROPERTY, CoreWorkload.USE_LIMITED_VARCHAR_PROPERTY_DEFAULT));

      for (int i = 0; i < fieldCount; i++) {
        valueFields.add(fieldPrefix + i);
      }

      indexCount = Integer.parseInt(properties.getProperty(
          CoreWorkload.INDEX_COUNT_PROPERTY, CoreWorkload.INDEX_COUNT_PROPERTY_DEFAULT));
      indexType = properties.getProperty(CoreWorkload.INDEX_TYPE_PROPERTY, "");

      if (indexCount > fieldCount) {
        throw new DBException(String.format(
            "Indexed fields count (%s=%s) should be less or equal to fields count (%s=%s)",
            CoreWorkload.INDEX_COUNT_PROPERTY, indexCount, CoreWorkload.FIELD_COUNT_PROPERTY, fieldCount));
      }

      hosts = properties.getProperty(HOSTS_PROPERTY);

      if (ignite == null && !useEmbeddedIgnite && hosts == null) {
        throw new DBException(String.format(
            "Required property \"%s\" is missing for Ignite Cluster",
            HOSTS_PROPERTY));
      }
    } catch (Exception e) {
      throw new DBException(e);
    }
  }

  /**
   * - Start embedded Ignite node (if needed).
   * - Get Ignite client (if needed).
   *
   * @param isEmbedded Whether to start embedded node.
   */
  private void initIgnite(boolean isEmbedded) throws DBException {
    //skip if 'ignite' was set with 'setIgniteServer'
    if (ignite == null) {
      if (isEmbedded) {
        igniteServer = startEmbeddedNode();
        ignite = igniteServer.api();
      } else {
        igniteClient = IgniteClient.builder().addresses(hosts.split(",")).build();
        ignite = igniteClient;
      }
    }
  }

  /**
   * Start embedded Ignite node.
   */
  private static IgniteServer startEmbeddedNode() throws DBException {
    IgniteServer embeddedIgnite;
    String clusterName = "myCluster";
    String nodeName = "defaultNode";

    try {
      String cfgResourceName = String.format("ignite-config%s%s.json",
          disableFsync ? "-nofsync" : "",
          storageProfile.isEmpty() ? "" : "-" + storageProfile);
      Path cfgPath = embeddedIgniteWorkDir.resolve(cfgResourceName);

      Files.createDirectories(embeddedIgniteWorkDir);
      try (InputStream cfgIs =
               IgniteAbstractClient.class.getClassLoader().getResourceAsStream(cfgResourceName)) {
        Files.copy(Objects.requireNonNull(cfgIs), cfgPath, StandardCopyOption.REPLACE_EXISTING);
      }

      LOG.info("Starting Ignite node {} in {} with config {}", nodeName, embeddedIgniteWorkDir, cfgPath);
      embeddedIgnite = IgniteServer.start(nodeName, cfgPath, embeddedIgniteWorkDir);

      InitParameters initParameters = InitParameters.builder()
          .metaStorageNodeNames(nodeName)
          .clusterName(clusterName)
          .build();

      embeddedIgnite.initCluster(initParameters);
    } catch (Exception e) {
      throw new DBException("Failed to start an embedded Ignite node", e);
    }

    return embeddedIgnite;
  }

  /**
   * Create test table(s).
   */
  public void createTables() throws DBException {
    List<String> sqlList = createTablesSQL();

    for (int i = 0; i < tableCount; i++) {
      String tableName = tableNames.get(i);
      String createTableReq = sqlList.get(i);

      LOG.info("Creating table '{}'. SQL line: {}", tableName, createTableReq);
      ignite.sql().execute(null, createTableReq).close();

      boolean cachePresent = waitForCondition(() -> ignite.tables().table(tableName) != null,
          TABLE_CREATION_TIMEOUT_SECONDS * 1_000L);

      if (!cachePresent) {
        throw new DBException(String.format(
            "Table '%s' wasn't created in %s seconds.", tableName, TABLE_CREATION_TIMEOUT_SECONDS));
      }
    }
  }

  /**
   * Prepare the creation table SQL line(s).
   */
  public List<String> createTablesSQL() {
    String fieldType = useLimitedVarchar
        ? String.format(" VARCHAR(%s)", fieldLength)
        : " VARCHAR";

    String fieldsSpecs = valueFields.stream()
        .map(e -> e + fieldType)
        .collect(Collectors.joining(", "));

    String withZoneName = "";
    if (!zoneName.isEmpty()) {
      if (useColumnar) {
        withZoneName = String.format(
            " ZONE \"%s\" STORAGE PROFILE '%s' SECONDARY ZONE \"%s\" SECONDARY STORAGE PROFILE '%s'",
            zoneName,
            storageProfile,
            zoneName,
            secondaryStorageProfile);
      } else {
        withZoneName = String.format(" ZONE \"%s\"", zoneName);
      }
    }

    List<String> sqlList = new ArrayList<>();

    for (String tableName : tableNames) {
      sqlList.add(String.format(
          "CREATE TABLE IF NOT EXISTS %s(%s VARCHAR PRIMARY KEY, %s)%s",
          tableName, PRIMARY_COLUMN_NAME, fieldsSpecs, withZoneName));
    }

    return sqlList;
  }

  /**
   * Create zone if needed.
   */
  public void createZone() {
    String createZoneReq = createZoneSQL();

    if (!createZoneReq.isEmpty()) {
      LOG.info("Creating zone. SQL line: {}", createZoneReq);
      ignite.sql().execute(null, createZoneReq).close();
    }
  }

  /**
   * Prepare the creation zone SQL line.
   */
  public String createZoneSQL() {
    if (zoneName.isEmpty()) {
      return "";
    }

    storageProfile = storageProfile.isEmpty() ?
        DEFAULT_STORAGE_PROFILE_NAME :
        storageProfile;
    secondaryStorageProfile = secondaryStorageProfile.isEmpty() ?
        DEFAULT_COLUMNAR_PROFILE_NAME :
        secondaryStorageProfile;

    String storageProfiles = useColumnar ?
        String.join(",", storageProfile, secondaryStorageProfile) :
        storageProfile;

    String paramStorageProfiles = String.format("STORAGE_PROFILES='%s'", storageProfiles);
    String paramReplicas = replicas.isEmpty() ? "" : "replicas=" + replicas;
    String paramPartitions = partitions.isEmpty() ? "" : "partitions=" + partitions;
    String paramNodesFilter = nodesFilter.isEmpty() ? "" :
        String.format("DATA_NODES_FILTER='$[?(@.%s == \"%s\")]'", NODES_FILTER_ATTRIBUTE, nodesFilter);
    String params = Stream.of(paramStorageProfiles, paramReplicas, paramPartitions, paramNodesFilter)
        .filter(s -> !s.isEmpty())
        .collect(Collectors.joining(", "));
    String reqWithParams = params.isEmpty() ? "" : " WITH " + params;

    return String.format("CREATE ZONE IF NOT EXISTS %s%s;", zoneName, reqWithParams);
  }

  /**
   * Create indexes if needed.
   */
  public void createIndexes() {
    List<String> sqlList = createIndexesSQL();

    if (!sqlList.isEmpty()) {
      LOG.info(String.format("Creating %s indexes.", indexCount));

      sqlList.forEach(idxReq -> {
          LOG.info("SQL line: {}", idxReq);
          ignite.sql().execute(null, idxReq).close();
        });
    }
  }

  /**
   * Prepare the creation indexes SQL lines.
   */
  public List<String> createIndexesSQL() {
    if (indexCount <= 0) {
      return Collections.emptyList();
    }

    List<String> createIndexesReqs = new ArrayList<>();

    String usingIndexType = indexType != null && !indexType.isEmpty() ? " USING " + indexType.toUpperCase() : "";

    valueFields.subList(0, indexCount).forEach(field ->
        createIndexesReqs.add(String.format("CREATE INDEX IF NOT EXISTS idx_%s ON %s%s (%s);",
            field, tableNamePrefix, usingIndexType, field)));

    return createIndexesReqs;
  }

  /**
   * Init Key-Value view and Record view lists.
   */
  private void initViews() {
    kvViews = new ArrayList<>(tableCount);
    rViews = new ArrayList<>(tableCount);

    for (String tableName : tableNames) {
      kvViews.add(ignite.tables().table(tableName).keyValueView());
      rViews.add(ignite.tables().table(tableName).recordView());
    }
  }

  /**
   * Get table entries amount.
   *
   * @param ignite0 Ignite node.
   * @param tableName Table name.
   */
  private static long entriesInTable(Ignite ignite0, String tableName) throws DBException {
    long entries = 0L;

    try (ResultSet<SqlRow> res = ignite0.sql().execute(null, "SELECT COUNT(*) FROM " + tableName)) {
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
   * Try to get positive result for the given amount of time (but at least once, to mitigate some GC pauses).
   *
   * @param cond Condition to check.
   * @param timeout Timeout in milliseconds.
   * @return {@code True} if condition has happened within the timeout.
   */
  public static boolean waitForCondition(BooleanSupplier cond, long timeout) {
    return waitForCondition(cond, timeout, 50);
  }

  /**
   * Try to get positive result for the given amount of time (but at least once, to mitigate some GC pauses).
   *
   * @param cond Condition to check.
   * @param timeout Timeout in milliseconds.
   * @param interval Interval to test condition in milliseconds.
   * @return {@code True} if condition has happened within the timeout.
   */
  @SuppressWarnings("BusyWait")
  public static boolean waitForCondition(BooleanSupplier cond, long timeout, long interval) {
    long stop = System.currentTimeMillis() + timeout;

    do {
      if (cond.getAsBoolean()) {
        return true;
      }

      try {
        Thread.sleep(interval);
      } catch (InterruptedException e) {
        return false;
      }
    } while (System.currentTimeMillis() < stop);

    return false;
  }

  /** {@inheritDoc} */
  @Override
  public void cleanup() throws DBException {
    synchronized (IgniteAbstractClient.class) {
      int curInitCount = INIT_COUNT.decrementAndGet();

      if (curInitCount <= 0) {
        try {
          if (debug) {
            LOG.info("Records in table {}: {}", tableNamePrefix, entriesInTable(ignite, tableNamePrefix));
          }

          if (igniteClient != null) {
            igniteClient.close();
          }

          if (igniteServer != null && (!externalIgnite || shutdownExternalIgnite)) {
            igniteServer.shutdown();
          }

          ignite = null;
        } catch (Exception e) {
          throw new DBException(e);
        }
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
   * Get table Key-Value view for key.
   *
   * @param key Key.
   */
  protected KeyValueView<Tuple, Tuple> getKvView(String key) {
    int index = tableCount <= 1 ?
        0 : //skip the key processing for choosing view in case of 1 test table
        (int) (Long.parseLong(key.substring(4)) % tableCount); //CoreWorkload uses key hash with prefix "user"

    return kvViews.get(index);
  }
}
