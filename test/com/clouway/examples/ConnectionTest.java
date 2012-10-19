package com.clouway.examples;

import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;

/**
 * @author georgi.hristov@clouway.com
 */
public class ConnectionTest {

  @Test
  public void connectionClearWarnings(){
    try{
    Connection sqlConnection;

    sqlConnection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/workingdb", "postgres", "");

    Statement stmt = sqlConnection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,ResultSet.CONCUR_UPDATABLE);

    SQLWarning sw;

   ResultSet rs =  stmt.executeQuery("Select * from system_user");

    sw = stmt.getWarnings();

    System.out.println(sw.getMessage());

      while (rs.next()) {
        System.out.println("Employee name: " + rs.getString(2));
      }
      rs.previous();
      rs.updateString("name", "Jon");

    }catch(SQLException e){

      System.out.println("shit");

    } catch (Exception ex) {
      ex.printStackTrace();

    }
  }

}
