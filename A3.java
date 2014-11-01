import java.sql.*;
import java.util.*;
import java.io.*;

public class A3 {

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
            if(!stk.hasMoreTokens())
            {
                System.out.println("ERROR");
            }
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
                case("SETV"):
                    setV(stk.nextToken(), stk.nextToken(), stk.nextToken(), stk.nextToken());
                    break;
                case("SETM"):
                    setM(stk.nextToken(), stk.nextToken(), stk.nextToken());
                    break;
                case("ADD"):
                    break;
                case("SUB"):
                    break;
                case("MULT"):
                    break;
                case("TRANSOPOSE"):
                    break;
                case("SQL"):
                    String query = "";
                    while(stk.hasMoreTokens())
                    {
                        query += (stk.nextToken() + " ");
                    }
                    runSQL(query);
                    break;
                default:
                    System.out.println("ERROR");

            }
        }

        con.close();
    }

    public static void runSQL(String query) throws SQLException
    {
        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery(query);
        if(rs.next())
            System.out.println(rs.getString(1));
    }

    public static void setM(String matrix_id, String row, String col) throws SQLException
    {
        try
        {
            con.setAutoCommit(false);
            String query = "DELETE FROM MATRIX WHERE MATRIX_ID = " + matrix_id;
            Statement stmt = con.createStatement();
            stmt.executeUpdate(query);
            query = "INSERT INTO MATRIX(MATRIX_ID, ROW_DIM, COL_DIM) VALUES( " + matrix_id + ", " + row + ", " + col + ");";
            stmt.executeUpdate(query);
            query = "DELETE FROM MATRIX_DATA WHERE MATRIX_ID = " + matrix_id + " AND ROW_NUM > " + row + " OR COL_NUM > " + col;
            stmt.executeUpdate(query);
            System.out.println("DONE");
            con.commit();
            con.setAutoCommit(true);
        }
        catch(Exception e)
        {
            System.out.println("ERROR");
            con.rollback();
            con.setAutoCommit(true);
        }
    }

    public static void setV(String matrix_id, String row, String col, String val) throws SQLException
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
        query = "DELETE FROM MATRIX_DATA WHERE MATRIX_ID = " + matrix_id + " AND ROW_NUM = " + row + " AND COL_NUM = " + col;
        stmt.executeUpdate(query);
        if(Double.parseDouble(val) == 0)
        {
            return;
        }
        query = "INSERT INTO MATRIX_DATA(MATRIX_ID, ROW_NUM, COL_NUM, VALUE) VALUES( " + matrix_id + ", " + row + ", " + col + ", " + val + ");";
        stmt.executeUpdate(query);
        System.out.println("DONE");

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
            System.out.println(rs.getString("VALUE").substring(0,3));
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
    	

	


