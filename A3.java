import java.sql.*;
import java.util.*;
import java.io.*;

public class A3 {
	
/* Do not hard code this connection string. Your program must accept the connection string provided as input parameter (arg 0) to your program!
    private static final String CONNECTION_STRING ="jdbc:mysql://127.0.0.1/cs348?user=root&password=cs348&Database=cs348;";
  */  
    private static Connection con;

    public static void main(String[] args) throws
                             ClassNotFoundException,SQLException,IOException
    {
	   String CONNECTION_STRING =args[0];
	   String INPUT_FILE = args[1];

        con = DriverManager.getConnection(args[0]);
        BufferedReader br = new BufferedReader(new FileReader(INPUT_FILE));
        ArrayList<String> commands = new ArrayList<String>();
        String line;
        while ((line = br.readLine()) != null) {
            commands.add(line);
        }
        br.close();
        StringTokenizer stk; 
        for(String l : commands)
        {
            stk = new StringTokenizer(l);
            switch(stk.nextToken())
            {
                case("DELETE"):
                    String param;
                    param = stk.nextToken();
                    
                    if(param.equals("ALL"))
                    {
                        deleteAll();
                    }
                    else
                    {
                        deleteMatrix(param);    
                    }
                    break;
                case("GETV"):
                    getV(stk.nextToken(), stk.nextToken(), stk.nextToken());
                    break;

                default:
                    System.out.println("ERROR");

            }
        }

        con.close();
    }

    public static void getV(String matrix_id, String row, String col) throws SQLException
    {
        String query = "SELECT * FROM MATRIX WHERE MATRIX_ID = " + matrix_id;
        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery(query);
        if (!rs.next()) {

            System.out.println("ERROR");
            return;
        }
        else
        {
            if(Integer.parseInt(row) > rs.getInt("ROW_DIM"))
            {
                System.out.println("ERROR");
                return;
            }
            if(Integer.parseInt(col) > rs.getInt("COL_DIM"))
            {
                System.out.println("ERROR");
                return;
            }

        }

        query = "SELECT * FROM MATRIX_DATA WHERE MATRIX_ID = " + matrix_id + " AND ROW_NUM = " + row + " AND COL_NUM = " + col;
        rs = stmt.executeQuery(query);
        if (!rs.next()) {

            System.out.println("0");
            return;
        }
        else
        {
             // TODO set the precision of Value to precision on 1 decimal place.
            System.out.println(rs.getString("VALUE"));
        }

    }

    
    public static void deleteAll() throws SQLException

    {
        String query = "DELETE FROM MATRIX_DATA";
        Statement stmt = con.createStatement();
        stmt.executeUpdate(query);
        query = "DELETE FROM MATRIX";
        stmt.executeUpdate(query);
        System.out.println("DONE");
    }

    public static void deleteMatrix(String param) throws SQLException
    {
        String query = "DELETE FROM MATRIX_DATA WHERE MATRIX_ID = " + param;
        Statement stmt = con.createStatement();
        stmt.executeUpdate(query);
        query = "DELETE FROM MATRIX WHERE MATRIX_ID = " + param;
        stmt.executeUpdate(query);
        System.out.println("DONE");
    }
}
    	

	


