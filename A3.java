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
            }
        }

        con.close();
    }

    public static void getV() throws SQLException
    {
        String query = "SELECT COUNT(*) FROM MATRIX";
        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery(query);
        while (rs.next()) {

            System.out.println(rs.getString(1));
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
        
    }
}
    	

	


