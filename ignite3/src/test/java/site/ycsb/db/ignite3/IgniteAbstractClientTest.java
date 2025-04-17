package site.ycsb.db.ignite3;

import java.util.List;
import java.util.Properties;
import org.junit.Test;
import site.ycsb.DBException;

import static org.junit.Assert.assertEquals;

/**
 * Tests for the {@link IgniteAbstractClient} class.
 */
public class IgniteAbstractClientTest {
  private static final String CREATE_TABLE_DDL =
      "CREATE TABLE IF NOT EXISTS usertable(ycsb_key VARCHAR PRIMARY KEY, field0 VARCHAR, " +
          "field1 VARCHAR, field2 VARCHAR, field3 VARCHAR, field4 VARCHAR, field5 VARCHAR, field6 VARCHAR, " +
          "field7 VARCHAR, field8 VARCHAR, field9 VARCHAR)";

  public static Properties properties() {
    Properties result = new Properties();

    result.put("hosts", "192.168.209.148");

    return result;
  }

  @Test
  public void testCreateDdl() throws DBException {
    Properties properties = properties();

    doTestCreateDdl(
        "",
        List.of(CREATE_TABLE_DDL),
        properties);
  }

  @Test
  public void testCreateDdlWithColumnar() throws DBException {
    Properties properties = properties();

    properties.put("useColumnar", String.valueOf(true));

    doTestCreateDdl(
        "CREATE ZONE IF NOT EXISTS Z1 STORAGE PROFILES ['default,myColumnarStore'];",
        List.of(CREATE_TABLE_DDL +
            " ZONE \"Z1\" STORAGE PROFILE 'default' SECONDARY ZONE \"Z1\" SECONDARY STORAGE PROFILE 'myColumnarStore'"),
        properties);
  }

  @Test
  public void testCreateDdlWithReplicasAndPartitionsAndNodesFilter() throws DBException {
    Properties properties = properties();

    properties.put("useColumnar", String.valueOf(false));
    properties.put("replicas", "1");
    properties.put("partitions", "2");
    properties.put("nodesFilter", "test_filter");

    doTestCreateDdl(
        "CREATE ZONE IF NOT EXISTS Z1 "
            + "(replicas 1, partitions 2, NODES FILTER '$[?(@.ycsbFilter == \"test_filter\")]') STORAGE PROFILES ['default'];",
        List.of(CREATE_TABLE_DDL + " ZONE \"Z1\""),
        properties);
  }

  public void doTestCreateDdl(
      String createZoneDdlExpected,
      List<String> createTableDdlExpected,
      Properties properties
  ) throws DBException {
    IgniteAbstractClient client = new IgniteClient();
    client.initProperties(properties);

    String createZoneDdlActual = client.createZoneSQL();
    List<String> createTableDdlActual = client.createTablesSQL();

    assertEquals(createZoneDdlExpected, createZoneDdlActual);
    assertEquals(createTableDdlExpected, createTableDdlActual);
  }
}
