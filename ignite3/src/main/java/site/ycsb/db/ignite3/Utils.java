package site.ycsb.db.ignite3;

import java.util.Properties;
import org.apache.ignite.table.TableViewOptions; // GG9 only!
import org.apache.ignite.table.NearCacheOptions; // GG9 only!

/**
 * Utility functions for Ignite3 YCSB client.
 */
public final class Utils {
  private Utils() {
    // not used
  }

  public static TableViewOptions parseTableViewOptions(Properties properties) {
    NearCacheOptions.Builder nearCacheOptionsBuilder = NearCacheOptions.builder();

    if (IgniteParam.NEAR_CACHE_MAX_ENTRIES.getValue(properties) >= 0) {
      nearCacheOptionsBuilder.maxEntries(IgniteParam.NEAR_CACHE_MAX_ENTRIES.getValue(properties));
    }

    if (IgniteParam.NEAR_CACHE_EXPIRE_AFTER_ACCESS.getValue(properties) >= 0) {
      nearCacheOptionsBuilder.expireAfterAccess(IgniteParam.NEAR_CACHE_EXPIRE_AFTER_ACCESS.getValue(properties));
    }

    if (IgniteParam.NEAR_CACHE_EXPIRE_AFTER_UPDATE.getValue(properties) >= 0) {
      nearCacheOptionsBuilder.expireAfterUpdate(IgniteParam.NEAR_CACHE_EXPIRE_AFTER_UPDATE.getValue(properties));
    }

    return TableViewOptions.builder().nearCacheOptions(nearCacheOptionsBuilder.build()).build();
  }
}
