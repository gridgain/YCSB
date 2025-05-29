package site.ycsb.db.ignite3;

import java.util.Properties;
import org.apache.ignite.table.TableViewOptions; // GG9 only!
import org.apache.ignite.table.NearCacheOptions; // GG9 only!

public class Utils {
  public static TableViewOptions parseTableViewOptions(Properties properties) {

    // TODO: parametrize with NearCacheOptions
    return TableViewOptions.DEFAULT;
  }
}
