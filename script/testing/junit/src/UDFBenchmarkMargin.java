/**
 * Insert statement tests.
 */

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


public class UDFBenchmarkMargin {
    private Connection conn;
    private ResultSet rs;
//    private static final String SQL_DROP_TABLE =
//            "DROP TABLE IF EXISTS sample;";
//
//    private static final String SQL_CREATE_TABLE =
//            "CREATE TABLE sample (x integer);";

//



    //private static final String SQL_QUERY_1 = "SELECT x FROM sample LIMIT %d;";
    //private static final String SQL_QUERY_2 = "SELECT x+1 FROM sample LIMIT %d;";
    //private static final String SQL_QUERY_3 = "SELECT compTest02(x) FROM sample LIMIT %d;";

    private static final String SQL_CREATE_FUNCTION = "CREATE FUNCTION margin(partkey integer) RETURNS integer AS\n" +
"$$\n" +
"  DECLARE\n" +
"    this_order_k integer := NULL;\n" +
"    this_order_d date := NULL;\n" +
"    buy            integer           := NULL;\n" +
"    sell           integer           := NULL;\n" +
"    margin         numeric(15,2) := NULL;\n" +
"    cheapest       numeric(15,2) := NULL;\n" +
"    cheapest_order integer;\n" +
"    price          numeric(15,2);\n" +
"    profit         numeric(15,2);\n" +
"  BEGIN\n" +
"    -- ➊ first order for the given part\n" +
"    SELECT o.o_orderdate into this_order_d\n" +
"                   FROM   lineitem AS l, orders AS o\n" +
"                   WHERE  l.l_orderkey = o.o_orderkey\n" +
"                   AND    l.l_partkey  = partkey\n" +
"                   ORDER BY o.o_orderdate\n" +
"                   LIMIT 1;\n" +
"\n" +
"    SELECT o.o_orderkey into this_order_k\n" +
"                   FROM   lineitem AS l, orders AS o\n" +
"                   WHERE  l.l_orderkey = o.o_orderkey\n" +
"                   AND    l.l_partkey  = partkey\n" +
"                   ORDER BY o.o_orderdate\n" +
"                   LIMIT 1;\n" +
"\n" +
"    -- hunt for the best margin while there are more orders to consider\n" +
"    WHILE this_order_k IS NOT NULL LOOP\n" +
"      -- ➋ price of part in this order\n" +
"      SELECT MIN(l.l_extendedprice * (1 - l.l_discount) * (1 + l.l_tax)) into price\n" +
"                FROM   lineitem AS l\n" +
"                WHERE  l.l_orderkey = this_order_k\n" +
"                AND    l.l_partkey  = partkey;\n" +
"\n" +
"      -- if this the new cheapest price, remember it\n" +
"      IF cheapest IS NULL THEN\n" +
"        cheapest := price;\n" +
"      END IF;\n" +
"\n" +
"      IF price <= cheapest THEN\n" +
"        cheapest       := price;\n" +
"        cheapest_order := this_order_k;\n" +
"      END IF;\n" +
"      -- compute current obtainable margin\n" +
"      profit := price - cheapest;\n" +
"      IF margin IS NULL THEN\n" +
"        margin := profit;\n" +
"      END IF;\n" +
"      IF profit >= margin THEN\n" +
"        buy    := cheapest_order;\n" +
"        sell   := this_order_k;\n" +
"        margin := profit;\n" +
"      END IF;\n" +
"\n" +
"      -- ➌ find next order (if any) that traded the part\n" +
"      SELECT o.o_orderkey into this_order_k\n" +
"                     FROM   lineitem AS l, orders AS o\n" +
"                     WHERE  l.l_orderkey = o.o_orderkey\n" +
"                     AND    l.l_partkey  = partkey\n" +
"                     AND    o.o_orderdate > this_order_d\n" +
"                     ORDER BY o.o_orderdate\n" +
"                     LIMIT 1;\n" +
"      SELECT o.o_orderdate into this_order_d\n" +
"                     FROM   lineitem AS l, orders AS o\n" +
"                     WHERE  l.l_orderkey = o.o_orderkey\n" +
"                     AND    l.l_partkey  = partkey\n" +
"                     AND    o.o_orderdate > this_order_d\n" +
"                     ORDER BY o.o_orderdate\n" +
"                     LIMIT 1;\n" +
"    END LOOP;\n" +
"\n" +
"    RETURN buy;\n" +
"  END;\n" +
"$$\n" +
"LANGUAGE PLPGSQL;";

private static final String SQL_CREATE_FUNCTION = "CREATE FUNCTION cursormargin2(partkey int) RETURNS integer AS\n" +
"$$\n" +
"  DECLARE\n" +
"    this_order_k integer := NULL;\n" +
"    buy            integer           := 24;\n" +
"    sell           integer           := NULL;\n" +
"    margin         numeric(15,2) := NULL;\n" +
"    cheapest       numeric(15,2) := NULL;\n" +
"    cheapest_order integer;\n" +
"    price          numeric(15,2);\n" +
"    profit         numeric(15,2);\n" +
"  BEGIN\n" +
"    -- ➊ first order for the given part\n" +
"\n" +
"    FOR this_order_k in (SELECT o.o_orderkey\n" +
"                   FROM   lineitem AS l, orders AS o\n" +
"                   WHERE  l.l_orderkey = o.o_orderkey\n" +
"                   AND    l.l_partkey  = partkey\n" +
"                   ORDER BY o.o_orderdate) LOOP\n" +
"        SELECT MIN(l.l_extendedprice * (1 - l.l_discount) * (1 + l.l_tax)) into price\n" +
"                FROM   lineitem AS l\n" +
"                WHERE  l.l_orderkey = this_order_k\n" +
"                AND    l.l_partkey  = partkey;\n" +
"        IF cheapest IS NULL THEN\n" +
"          cheapest := price;\n" +
"        END IF;\n" +
"        IF price <= cheapest THEN\n" +
"          cheapest       := price;\n" +
"          cheapest_order := this_order_k;\n" +
"        END IF;\n" +
"      -- compute current obtainable margin\n" +
"      profit := price - cheapest;\n" +
"      IF margin IS NULL THEN\n" +
"        margin := profit;\n" +
"      END IF;\n" +
"      IF profit >= margin THEN\n" +
"        buy    := cheapest_order;\n" +
"        -- buy := buy + 1;\n" +
"        sell   := this_order_k;\n" +
"        margin := profit;\n" +
"      END IF;\n" +
"    END LOOP;\n" +
"\n" +
"    RETURN buy;\n" +
"  END;\n" +
"$$\n" +
"LANGUAGE PLPGSQL;";


    private static final String SQL_QUERY_4 = "SELECT margin(part) FROM partkeys;";
    private static final String SQL_QUERY_5 = "SELECT cursormargin(part) FROM partkeys;";
//    private static final String SQL_QUERY_5 = "SELECT L2Norm(%d);";
//    private static final String SQL_QUERY_6 = "SELECT L2Norm2(%d);";

    private static final String DEMARCATOR = "SELECT 1;";

//    private static final int[] LIMITS = {0,1,10,100,1000,10000,100000, 1000000, 10000000, 50000000, 100000000};

//    private static final int[] N_VALUES = {1,2,3,4,5,10,100};

    /**
     * Initialize the database and table for testing
     */
    private void initDatabase() throws SQLException {
        Statement stmt = conn.createStatement();
//        stmt.execute(SQL_DROP_TABLE);
//        stmt.execute(SQL_CREATE_TABLE);
        stmt.execute(SQL_CREATE_FUNCTION);
        stmt.execute(SQL_CREATE_FUNCTION_2);

          for(int i = 0;i < 3;i++){
            stmt.execute(SQL_QUERY_5);
          }

          stmt.execute(DEMARCATOR);

          for(int i = 0;i < 3;i++){
            stmt.execute(SQL_QUERY_4);
          }

    }

    /**
     * Setup for each test, execute before each test
     * reconnect and setup default table
     */
    public void setup() throws SQLException, ClassNotFoundException {
        Properties props = new Properties();
        props.setProperty("prepareThreshold", "0");
        props.setProperty("preferQueryMode", "extended");
//        props.setProperty("prepareThreshold", 0);
        String url = String.format("jdbc:postgresql://localhost:15721/noisepage");
        Class.forName("org.postgresql.Driver");
        conn = DriverManager.getConnection(url, props);
            conn.setAutoCommit(true);
            initDatabase();

    }

    /**
     * Cleanup for each test, execute after each test
     * drop the default table and close connection
     */
    public void teardown() throws SQLException {

    }

    public void bottomtext() throws SQLException, ClassNotFoundException{
        setup();
        teardown();
        return;

}

public static void main(String[] args) throws SQLException, ClassNotFoundException{
        UDFBenchmarkMargin b = new UDFBenchmarkMargin();
        b.bottomtext();
}

    /* --------------------------------------------
     * Cte tests
     * ---------------------------------------------
     */

    /*
    * Project Column Tests -
    * To check table schema and output schema coloids properly in tpl
    * */




}