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


public class UDFBenchmarkL2 {
    private Connection conn;
    private ResultSet rs;
//    private static final String SQL_DROP_TABLE =
//            "DROP TABLE IF EXISTS sample;";
//
//    private static final String SQL_CREATE_TABLE =
//            "CREATE TABLE sample (x integer);";

    private static final String SQL_CREATE_FUNCTION =
"CREATE OR REPLACE FUNCTION L2Norm2(n integer) RETURNS integer AS \n" +
"$$\n" +
"\tDECLARE \n" +
"\t\tsum integer := NULL;\n" +
"\t\ti integer := NULL;\n" +
"\t\tj integer := NULL;\n" +
"\t\ta integer := NULL;\n" +
"\t\tb integer := NULL;\n" +
"\t\trow integer := NULL;\n" +
"\tBEGIN\n" +
"\t\tsum := 0;\n" +
"\t\ti := 1;\n" +
"\t\tWHILE i <= n LOOP\n" +
"\t\t\trow := 0;\n" +
"\t\t\tj := 1;\n" +
"\t\t\tWHILE j <= n LOOP\n" +
"\t\t\t\tFOR a,b in (select p.v,q.v  FROM matrix_1 p JOIN matrix_2 q ON p.c = q.r WHERE p.r = i and q.c = j) LOOP\n" +
"\t\t\t\t\trow := row + a*b;\n" +
"\t\t\t\tEND LOOP;\n" +
"\t\t\t\tj := j + 1;\n" +
"\t\t\tEND LOOP;\n" +
"\t\t\tsum := sum + row * row;\n" +
"\t\t\ti := i + 1;\n" +
"\t\tEND LOOP;\n" +
"\t\tRETURN sum;\n" +
"\tEND;\n" +
"$$\n" +
"LANGUAGE PLPGSQL;";

private static final String SQL_CREATE_FUNCTION_2 =
"CREATE OR REPLACE FUNCTION L2Norm(n integer) RETURNS integer AS \n" +
"$$\n" +
"\tDECLARE \n" +
"\t\tsum integer := NULL;\n" +
"\t\ti integer := NULL;\n" +
"\t\tj integer := NULL;\n" +
"\t\ta integer := NULL;\n" +
"\t\tb integer := NULL;\n" +
"\t\telem integer := NULL;\n" +
"\t\trow integer := NULL;\n" +
"\tBEGIN\n" +
"\t\tsum := 0;\n" +
"\t\ti := 1;\n" +
"\t\tWHILE i <= n LOOP\n" +
"\t\t\trow := 0;\n" +
"\t\t\tj := 1;\n" +
"\t\t\tWHILE j <= n LOOP\n" +
"\t\t\t\tSELECT SUM(p.v*q.v) into elem FROM matrix_1 p JOIN matrix_2 q ON p.c = q.r WHERE p.r = i and q.c = j;\n" +
"\t\t\t\t-- RAISE NOTICE '% , % , %', i,j,elem;\n" +
"\t\t\t\tIF elem IS NULL THEN\n" +
"\t\t\t\t\telem := 0;\n" +
"\t\t\t\tEND IF;\n" +
"\t\t\t\trow := row + elem;\n" +
"\t\t\t\tj := j + 1;\n" +
"\t\t\tEND LOOP;\n" +
"\t\t\tsum := sum + row * row;\n" +
"\t\t\ti := i + 1;\n" +
"\t\tEND LOOP;\n" +
"\t\tRETURN sum;\n" +
"\tEND;\n" +
"$$\n" +
"LANGUAGE PLPGSQL;";

    //private static final String SQL_QUERY_1 = "SELECT x FROM sample LIMIT %d;";
    //private static final String SQL_QUERY_2 = "SELECT x+1 FROM sample LIMIT %d;";
    //private static final String SQL_QUERY_3 = "SELECT compTest02(x) FROM sample LIMIT %d;";

//    private static final String SQL_QUERY_4 = "SELECT margin(part) FROM partkeys;";
    private static final String SQL_QUERY_5 = "SELECT L2Norm(%d);";
    private static final String SQL_QUERY_6 = "SELECT L2Norm2(%d);";

    private static final String DEMARCATOR = "SELECT 1;";

    private static final int[] LIMITS = {0,1,10,100,1000,10000,100000, 1000000, 10000000, 50000000, 100000000};

    private static final int[] N_VALUES = {1,2,3,4,5,10,20,30,50,100,200};

    /**
     * Initialize the database and table for testing
     */
    private void initDatabase() throws SQLException {
        Statement stmt = conn.createStatement();
//        stmt.execute(SQL_DROP_TABLE);
//        stmt.execute(SQL_CREATE_TABLE);
        stmt.execute(SQL_CREATE_FUNCTION);
        stmt.execute(SQL_CREATE_FUNCTION_2);
//        StringBuffer sb = "INSERT INTO matrix_1 VALUES ";
       // assert(false);

//        for(int i = 0;i < 100000;i++){
//            stmt.execute(insert_SQL_1);
//        }
//        for(int lim : LIMITS){
//            System.out.printf("limit %d\n", lim);
//
//        System.out.println("QUERY 3");
//        stmt.execute(String.format(SQL_QUERY_3, lim));
//
//        for(int i = 0;i < 5;i++){
//            stmt.execute(String.format(SQL_QUERY_3, lim));
//        }
//        }
//
//        for(int lim : LIMITS){
//            System.out.printf("limit %d\n", lim);
//
//        System.out.println("QUERY 2");
//        stmt.execute(String.format(SQL_QUERY_2, lim));
//
//        for(int i = 0;i < 5;i++){
//            stmt.execute(String.format(SQL_QUERY_2, lim));
//        }
//        }
//
//        for(int lim : LIMITS){
//            System.out.printf("limit %d\n", lim);
//
//        System.out.println("QUERY 1");
//        stmt.execute(String.format(SQL_QUERY_1, lim));
//
//        for(int i = 0;i < 5;i++){
//            stmt.execute(String.format(SQL_QUERY_1, lim));
//        }
//        }
          for(int j = 0;j < N_VALUES.length;j++){
          for(int i = 0;i < 3;i++){
            stmt.execute(String.format(SQL_QUERY_5, N_VALUES[j]));
          }
          }

          stmt.execute(DEMARCATOR);

          for(int j = 0;j < N_VALUES.length;j++){
          for(int i = 0;i < 3;i++){
            stmt.execute(String.format(SQL_QUERY_6, N_VALUES[j]));
          }
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
        UDFBenchmarkL2 b = new UDFBenchmarkL2();
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