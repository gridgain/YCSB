package site.ycsb.db.ignite3;

import java.util.Properties;
import java.util.function.Function;
import org.jetbrains.annotations.NotNull;

/**
 * Ignite 3 client parameter.
 *
 * @param <T> Parameter value type.
 */
public final class Ignite3Param<T> {
  public static final Ignite3Param<Boolean> DEBUG =
      new Ignite3Param<>("debug", false, Boolean::parseBoolean);

  public static final Ignite3Param<Boolean> SHUTDOWN_IGNITE =
      new Ignite3Param<>("shutdownIgnite", false, Boolean::parseBoolean);

  public static final Ignite3Param<Boolean> USE_EMBEDDED =
      new Ignite3Param<>("useEmbedded", false, Boolean::parseBoolean);

  public static final Ignite3Param<Boolean> DISABLE_FSYNC =
      new Ignite3Param<>("disableFsync", false, Boolean::parseBoolean);

  public static final Ignite3Param<Boolean> USE_COLUMNAR =
      new Ignite3Param<>("useColumnar", false, Boolean::parseBoolean);

  public static final Ignite3Param<String> DB_ENGINE =
      new Ignite3Param<>("dbEngine", "", s -> s);

  public static final Ignite3Param<String> STORAGE_PROFILES =
      new Ignite3Param<>("storage_profiles", "", s -> s);

  public static final Ignite3Param<String> SECONDARY_STORAGE_PROFILE =
      new Ignite3Param<>("secondary_storage_profile", "", s -> s);

  public static final Ignite3Param<String> REPLICAS =
      new Ignite3Param<>("replicas", "", s -> s);

  public static final Ignite3Param<String> PARTITIONS =
      new Ignite3Param<>("partitions", "", s -> s);

  public static final Ignite3Param<String> NODES_FILTER =
      new Ignite3Param<>("nodesFilter", "", s -> s);

  public static final Ignite3Param<Boolean> TX_READ_ONLY =
      new Ignite3Param<>("txreadonly", false, Boolean::parseBoolean);

  public static final Ignite3Param<Integer> TABLE_COUNT =
      new Ignite3Param<>("tablecount", 2, Integer::parseInt);

  public static final Ignite3Param<String> WORK_DIR =
      new Ignite3Param<>("workDir", "../ignite3-ycsb-work/" + System.currentTimeMillis(), s -> s);

  /**
   * Parameter name.
   */
  @NotNull
  private String parameter;

  /**
   * Default parameter value.
   */
  @NotNull
  private T defaultValue;

  /**
   * Parsing function to get parameter value from property.
   */
  @NotNull
  private Function<String, T> parser;

  /**
   * Constructor.
   *
   * @param parameter Parameter.
   * @param defaultValue Default value for parameter.
   * @param parser Parsing function to get parameter value from property.
   */
  private Ignite3Param(@NotNull String parameter, @NotNull T defaultValue, @NotNull Function<String, T> parser) {
    this.parameter = parameter;
    this.defaultValue = defaultValue;
    this.parser = parser;
  }

  /**
   * Get parameter value from properties.
   *
   * @param properties Properties.
   */
  @NotNull
  public T getValue(@NotNull Properties properties) {
    if (properties.containsKey(parameter)) {
      return parser.apply(properties.getProperty(parameter));
    }

    return defaultValue;
  }
}
