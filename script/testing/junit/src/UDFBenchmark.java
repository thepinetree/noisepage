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


public class UDFBenchmark {
    private Connection conn;
    private ResultSet rs;
    private static final String SQL_DROP_TABLE =
            "DROP TABLE IF EXISTS sample;";

    private static final String SQL_CREATE_TABLE =
            "CREATE TABLE sample (x integer);";

    private static final String SQL_CREATE_FUNCTION =
            "CREATE FUNCTION inlinecompTest02(x integer) RETURNS INT AS\n" +
"$$\n" +
"DECLARE\n" +
"  y integer := 0;\n" +
"BEGIN\n" +
"  y = x;\n" +
"  y = y + 1;\n" +
"  RETURN y;\n" +
"END;\n" +
"$$ LANGUAGE plpgsql;";

    private static final String SQL_QUERY_1 = "SELECT x FROM sample LIMIT %d;";
    private static final String SQL_QUERY_2 = "SELECT x+1 FROM sample LIMIT %d;";
    private static final String SQL_QUERY_3 = "SELECT inlinecompTest02(x) FROM sample LIMIT %d;";

    private static final String SQL_QUERY_4 = "SELECT margin(part) FROM partkeys;";
    private static final String SQL_QUERY_5 = "SELECT loop(%d);";

    private static final int[] LIMITS = {0,1,10,100,1000,10000,100000, 1000000, 10000000, 50000000, 100000000};

    /**
     * Initialize the database and table for testing
     */
    private void initDatabase() throws SQLException {
        Statement stmt = conn.createStatement();
//        stmt.execute(SQL_DROP_TABLE);
//        stmt.execute(SQL_CREATE_TABLE);
        //stmt.execute(SQL_CREATE_FUNCTION);
        //String insert_SQL_1 = "INSERT INTO sample VALUES (1);";
       // assert(false);

//        for(int i = 0;i < 100000;i++){
//            stmt.execute(insert_SQL_1);
//        }
        for(int lim : LIMITS){
            System.out.printf("limit %d\n", lim);

        System.out.println("QUERY 3");
        stmt.execute(String.format(SQL_QUERY_3, lim));

        for(int i = 0;i < 5;i++){
            stmt.execute(String.format(SQL_QUERY_3, lim));
        }
        }

        for(int lim : LIMITS){
            System.out.printf("limit %d\n", lim);

        System.out.println("QUERY 2");
        stmt.execute(String.format(SQL_QUERY_2, lim));

        for(int i = 0;i < 5;i++){
            stmt.execute(String.format(SQL_QUERY_2, lim));
        }
        }

        for(int lim : LIMITS){
            System.out.printf("limit %d\n", lim);

        System.out.println("QUERY 1");
        stmt.execute(String.format(SQL_QUERY_1, lim));

        for(int i = 0;i < 5;i++){
            stmt.execute(String.format(SQL_QUERY_1, lim));
        }
        }
//          for(int j = 0;j < LIMITS.length;j++){
//          for(int i = 0;i < 3;i++){
//            stmt.execute(String.format(SQL_QUERY_5, LIMITS[j]));
//          }
//          }

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
        UDFBenchmark b = new UDFBenchmark();
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