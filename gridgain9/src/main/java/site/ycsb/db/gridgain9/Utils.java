package site.ycsb.db.gridgain9;

import java.util.Properties;
import org.apache.ignite.table.TableViewOptions;
import org.apache.ignite.table.NearCacheOptions;

/**
 * Utility functions for GridGain 9 YCSB client.
 */
public final class Utils {
  private Utils() {
    // not used
  }

  public static TableViewOptions parseTableViewOptions(Properties properties) {
    NearCacheOptions.Builder nearCacheOptionsBuilder = NearCacheOptions.builder();

    if (GridGainParam.NEAR_CACHE_MAX_ENTRIES.getValue(properties) >= 0) {
      nearCacheOptionsBuilder.maxEntries(GridGainParam.NEAR_CACHE_MAX_ENTRIES.getValue(properties));
    }

    if (GridGainParam.NEAR_CACHE_EXPIRE_AFTER_ACCESS.getValue(properties) >= 0) {
      nearCacheOptionsBuilder.expireAfterAccess(GridGainParam.NEAR_CACHE_EXPIRE_AFTER_ACCESS.getValue(properties));
    }

    if (GridGainParam.NEAR_CACHE_EXPIRE_AFTER_UPDATE.getValue(properties) >= 0) {
      nearCacheOptionsBuilder.expireAfterUpdate(GridGainParam.NEAR_CACHE_EXPIRE_AFTER_UPDATE.getValue(properties));
    }

    return TableViewOptions.builder().nearCacheOptions(nearCacheOptionsBuilder.build()).build();
  }
}
