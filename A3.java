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
                        deleteMatrix(param, true);    
                    }
                    break;
                case("GETV"):
                    getV(stk.nextToken(), stk.nextToken(), stk.nextToken());
                    break;
                case("SETV"):
                    setV(stk.nextToken(), stk.nextToken(), stk.nextToken(), stk.nextToken(), true);
                    break;
                case("SETM"):
                    setM(stk.nextToken(), stk.nextToken(), stk.nextToken() , true);
                    break;
                case("ADD"):
                    addM(stk.nextToken(), stk.nextToken(), stk.nextToken(), true);
                    break;
                case("SUB"):
                    addM(stk.nextToken(), stk.nextToken(), stk.nextToken(), false);
                    break;
                case("MULT"):
                    multiply(stk.nextToken(), stk.nextToken(), stk.nextToken());
                    break;
                case("TRANSPOSE"):
                    transpose(stk.nextToken(), stk.nextToken());
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

    public static void setM(String matrix_id, String row, String col, boolean msg) throws SQLException
    {
        try
        {
            con.setAutoCommit(false);
            String query = "DELETE FROM MATRIX WHERE MATRIX_ID = " + matrix_id;
            Statement stmt = con.createStatement();
            stmt.executeUpdate(query);
            query = "INSERT INTO MATRIX(MATRIX_ID, ROW_DIM, COL_DIM) VALUES( " + matrix_id + ", " + row + ", " + col + ");";
            stmt.executeUpdate(query);
            query = "DELETE FROM MATRIX_DATA WHERE MATRIX_ID = " + matrix_id + " AND ( ROW_NUM > " + row + " OR COL_NUM > " + col + ")";
            stmt.executeUpdate(query);
            if(msg)
                System.out.println("DONE");
            con.commit();
        }
        catch(Exception e)
        {
            if(msg)    
                System.out.println("ERROR");
            else 
                System.out.println(e.toString());
            con.rollback();
        }
        finally
        {
            con.setAutoCommit(true);
        }
    }

    public static void setV(String matrix_id, String row, String col, String val, boolean msg) throws SQLException
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
        if(msg)
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

    public static void deleteMatrix(String param, boolean msg) throws SQLException
    {
        String query = "DELETE FROM MATRIX_DATA WHERE MATRIX_ID = " + param;
        Statement stmt = con.createStatement();
        stmt.executeUpdate(query);
        query = "DELETE FROM MATRIX WHERE MATRIX_ID = " + param;
        stmt.executeUpdate(query);
        if(msg)
            System.out.println("DONE");
    }

    public static void addM(String matrixTo, String mUno, String mDos, boolean sign) throws SQLException
    {
        
        String query = "SELECT * FROM MATRIX WHERE MATRIX_ID = " + mUno + " OR MATRIX_ID = " + mDos;
        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery(query);
        int row_dim_uno = 0;
        int row_dim_dos = 0;
        int col_dim_uno = 0;
        int col_dim_dos = 0;
        if(!rs.next())
        {
            System.out.println("ERROR");
            return;
        }
        else
        {
            row_dim_uno = rs.getInt("ROW_DIM");
            col_dim_uno = rs.getInt("COL_DIM");
        }
        if(!rs.next())
        {
            System.out.println("ERROR");
            return;
        }
        else
        {
            row_dim_dos = rs.getInt("ROW_DIM");
            col_dim_dos = rs.getInt("COL_DIM");
        }
        if(row_dim_uno != row_dim_dos || col_dim_uno != col_dim_dos)
        {
            System.out.println("ERROR");
            return;
        }
        deleteMatrix(matrixTo, false);
        setM(matrixTo, Integer.toString(row_dim_uno), Integer.toString(col_dim_uno), false);
        Double matrix1[][] = new Double[row_dim_uno][col_dim_uno];
        Double matrix2[][] = new Double[row_dim_uno][col_dim_uno];
        Double msum[][] = new Double[row_dim_uno][col_dim_uno];
        for(int i = 0; i < row_dim_uno; i++)
        {
            for(int j= 0; j < col_dim_uno; j++)
            {
                matrix1[i][j] = 0.0;
                matrix2[i][j] = 0.0;
            }
        }
        query = "SELECT * FROM MATRIX_DATA WHERE MATRIX_ID = " + mUno;
        ResultSet rs1 = stmt.executeQuery(query);
        while(rs1.next())
        {
            matrix1[rs1.getInt("ROW_NUM") - 1][rs1.getInt("COL_NUM") - 1] =  Double.parseDouble(rs1.getString("VALUE")) ; 
        }
        query = "SELECT * FROM MATRIX_DATA WHERE MATRIX_ID = " + mDos;
        ResultSet rs2 = stmt.executeQuery(query);
        while(rs2.next())
        {
            matrix2[rs2.getInt("ROW_NUM") - 1][rs2.getInt("COL_NUM") - 1] =  Double.parseDouble(rs2.getString("VALUE")) ; 
        }
        for(int i = 0; i < row_dim_uno; i++)
        {
            for(int j= 0; j < col_dim_uno; j++)
            {
                if(sign)
                    msum[i][j] = matrix1[i][j] + matrix2[i][j];
                else
                    msum[i][j] = matrix1[i][j] - matrix2[i][j];
                //System.out.print(msum[i][j] + " ");
                setV(matrixTo, Integer.toString(i + 1), Integer.toString(j + 1), Double.toString(msum[i][j]), false);
            }
            //System.out.println();
        }
        System.out.println("DONE");
    } 

    public static void transpose(String matrix1, String matrix2) throws SQLException
    {
        String query = "SELECT * FROM MATRIX WHERE MATRIX_ID = " + matrix2;
        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery(query);
        int row_dim_uno = 0;
        int col_dim_uno = 0;
        if(!rs.next())
        {
            System.out.println("ERROR");
            return;
        }
        else
        {
            row_dim_uno = rs.getInt("ROW_DIM");
            col_dim_uno = rs.getInt("COL_DIM");
        }
        rs.close();
        deleteMatrix(matrix1, false);
        setM(matrix1, Integer.toString(col_dim_uno), Integer.toString(row_dim_uno), false);
        Double m1[][] = new Double[row_dim_uno][col_dim_uno];
        Double m2[][] = new Double[col_dim_uno][row_dim_uno];
        for(int i = 0; i < row_dim_uno; i++)
        {
            for(int j= 0; j < col_dim_uno; j++)
            {
                m1[i][j] = 0.0;
                m2[j][i] = 0.0;
            }
        }
        query = "SELECT * FROM MATRIX_DATA WHERE MATRIX_ID = " + matrix2 + ";";
        stmt = con.createStatement();
        ResultSet rs1 = stmt.executeQuery(query);
        while(rs1.next())
        {
            m1[rs1.getInt("ROW_NUM") - 1][rs1.getInt("COL_NUM") - 1] =  Double.parseDouble(rs1.getString("VALUE")) ; 
        }
        rs1.close();
        for(int i = 0; i < row_dim_uno; i++)
        {
            for(int j= 0; j < col_dim_uno; j++)
            {   
                m2[j][i] = m1[i][j];
                setV(matrix1, Integer.toString(j + 1), Integer.toString(i + 1), Double.toString(m2[j][i]), false);
            }
        }
        System.out.println("DONE");
    }

    public static void multiply(String matrixTo, String mUno, String mDos)
    {
        
    }
}
