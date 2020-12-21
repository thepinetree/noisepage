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
import java.lang.StringBuilder;
import java.util.Random;


public class TupleInserter {
    private Connection conn;
    private ResultSet rs;
    private static final String SQL_DROP_TABLE =
            "DROP TABLE IF EXISTS matrix_1;DROP TABLE IF EXISTS matrix_2;";

    private static final String SQL_CREATE_TABLE =
            "CREATE TABLE matrix_1 (r integer, c integer, v integer);CREATE TABLE matrix_2 (r integer, c integer, v integer);" +
             "CREATE INDEX keys_1 ON matrix_1 USING btree(r, c);CREATE INDEX keys_2 ON matrix_2 USING btree(r, c);";

    private static final String SQL_CREATE_FUNCTION =
            "CREATE FUNCTION compTest02(x integer) RETURNS INT AS\n" +
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
    private static final String SQL_QUERY_3 = "SELECT compTest02(x) FROM sample LIMIT %d;";

    private static final int[] LIMITS = {0,1,10,100,1000,10000,100000};

    /**
     * Initialize the database and table for testing
     */
    private void initDatabase() throws SQLException {
        Random rand = new Random();
        Statement stmt = conn.createStatement();
        stmt.execute(SQL_DROP_TABLE);
        stmt.execute(SQL_CREATE_TABLE);
//        stmt.execute(SQL_CREATE_FUNCTION);
//
        StringBuilder sb = new StringBuilder("INSERT INTO matrix_1 VALUES (?,?,?);");
        PreparedStatement insertstmt = conn.prepareStatement(sb.toString());
//        assert(false);
        int rows_inserted = 0;
        int n = 1000;

        for(int i = 0;i < n*n;i++){
            int r = i / n;
            int c = i % n;
            int v = rand.nextInt();
//            if(i != 0){
//                sb.append(",");
//            }
//            sb.append(String.format("(%d,%d,%d)", r,c,v));
            insertstmt.setInt(1, r);
            insertstmt.setInt(2, c);
            insertstmt.setInt(3, v);
            insertstmt.executeUpdate();
        }

        sb = new StringBuilder("INSERT INTO matrix_2 VALUES (?,?,?);");
        insertstmt = conn.prepareStatement(sb.toString());

        for(int i = 0;i < n*n;i++){
            int r = i / n;
            int c = i % n;
            int v = rand.nextInt();
//            if(i != 0){
//                sb.append(",");
//            }
//            sb.append(String.format("(%d,%d,%d)", r,c,v));
            insertstmt.setInt(1, r);
            insertstmt.setInt(2, c);
            insertstmt.setInt(3, v);
            insertstmt.executeUpdate();
        }

//        conn.commit();
//        sb.append(";");


//        String insert_SQL_1 = sb.toString();
//        System.out.println("INSERTING NOW");
//
//        for(int i = 0;i < 100000;i++){
//            stmt.execute(insert_SQL_1);
//            rows_inserted += n*n;
//            System.out.printf("%d\n", rows_inserted);
//        }


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
//            Statement stmt = conn.createStatement();
//            stmt.execute(SQL_DROP_TABLE);

    }

    public void bottomtext() throws SQLException, ClassNotFoundException{
        setup();
        teardown();
        return;

}

public static void main(String[] args) throws SQLException, ClassNotFoundException{
        TupleInserter b = new TupleInserter();
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