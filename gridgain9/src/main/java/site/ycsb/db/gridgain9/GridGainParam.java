package site.ycsb.db.gridgain9;

import java.util.Properties;
import java.util.function.Function;
import org.jetbrains.annotations.NotNull;

/**
 * GridGain 9 client parameter.
 *
 * @param <T> Parameter value type.
 */
public final class GridGainParam<T> {
  // Maximum number of entries in the near cache.
  // -1 means use default value.
  public static final GridGainParam<Integer> NEAR_CACHE_MAX_ENTRIES =
      new GridGainParam<>("nearCacheMaxEntries", -1, Integer::parseInt);

  // Near cache entry expiration interval after reading in milliseconds.
  // -1 means use default value.
  public static final GridGainParam<Integer> NEAR_CACHE_EXPIRE_AFTER_ACCESS =
      new GridGainParam<>("nearCacheExpireAfterAcess", -1, Integer::parseInt);

  // Near cache entry expiration interval after update in milliseconds.
  // -1 means use default value.
  public static final GridGainParam<Integer> NEAR_CACHE_EXPIRE_AFTER_UPDATE =
      new GridGainParam<>("nearCacheExpireAfterUpdate", -1, Integer::parseInt);

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
  private GridGainParam(@NotNull String parameter, @NotNull T defaultValue, @NotNull Function<String, T> parser) {
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
