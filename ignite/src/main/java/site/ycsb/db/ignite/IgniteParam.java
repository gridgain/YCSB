package site.ycsb.db.ignite;

import java.util.Properties;
import java.util.function.Function;
import org.jetbrains.annotations.NotNull;

/**
 * Ignite 2 client parameter.
 *
 * @param <T> Parameter value type.
 */
public final class IgniteParam<T> {
  public static final IgniteParam<String> TX_CONCURRENCY =
      new IgniteParam<>("txconcurrency", "pessimistic", s -> s);

  public static final IgniteParam<String> TX_ISOLATION =
      new IgniteParam<>("txisolation", "serializable", s -> s);

  public static final IgniteParam<Integer> TABLE_COUNT =
      new IgniteParam<>("tablecount", 1, Integer::parseInt);

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
  private IgniteParam(@NotNull String parameter, @NotNull T defaultValue, @NotNull Function<String, T> parser) {
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
