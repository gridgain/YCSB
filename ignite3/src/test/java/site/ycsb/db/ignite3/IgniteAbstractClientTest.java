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

    result.put("hosts","192.168.209.148");

    return result;
  }

  @Test
  public void testCreateDdl() throws DBException {
    doTestCreateDdl(
        "CREATE ZONE IF NOT EXISTS Z1 WITH STORAGE_PROFILES='default,myColumnarStore';",
        List.of(CREATE_TABLE_DDL +
                " ZONE \"Z1\" STORAGE PROFILE 'default' SECONDARY ZONE \"Z1\" SECONDARY STORAGE PROFILE 'myColumnarStore'"),
        true);
    doTestCreateDdl(
        "",
        List.of(CREATE_TABLE_DDL),
        false);
  }

  public void doTestCreateDdl(
      String createZoneDdlExpected,
      List<String> createTableDdlExpected,
      boolean useColumnar
  ) throws DBException {
    IgniteAbstractClient client = new IgniteClient();
    Properties properties = properties();
    properties.put("useColumnar", String.valueOf(useColumnar));
    client.initProperties(properties);

    String createZoneDdlActual = client.createZoneSQL();
    List<String> createTableDdlActual = client.createTablesSQL();

    assertEquals(createZoneDdlExpected, createZoneDdlActual);
    assertEquals(createTableDdlExpected, createTableDdlActual);
  }
}
