package com.clouway.task2;

import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * @author georgi.hristov@clouway.com
 */
public class TripTest {

  class CustomerInformation {

    private Connection customerDBConnection;
    private PreparedStatement registerCustomer;

    public CustomerInformation(Connection customerDBConnection) {

      this.customerDBConnection = customerDBConnection;

    }

    public void prepareStatements() throws SQLException {

      registerCustomer = customerDBConnection.prepareStatement("INSERT INTO customer(name,eng,age,email) VALUES(?,?,?,?);");

    }


    public void registerCustomer(String name, String egn, Integer age, String email) throws SQLException {

      registerCustomer.setString(1, name);
      registerCustomer.setString(2, egn);
      registerCustomer.setInt(3, age);
      registerCustomer.setString(4, email);

    }

  }


  @Test
  public void registerNewCustomerIntoDatabase() throws SQLException {
    String dbAddress = "jdbc:postgresql://localhost:5432/holiday";
    String dbUsername = "postgres";
    String dbPassword = "123456";

    int recordsCount=0;

    Connection customerDBConnection = DriverManager.getConnection(dbAddress, dbUsername,dbPassword);
    Statement localStatement = customerDBConnection.createStatement();

    CustomerInformation customerInformation = new CustomerInformation(customerDBConnection);
    customerInformation.prepareStatements();

    String testCustomerName = "Georgi";
    String testCustomerEGN = "8903191401";
    Integer testCustomerAge = 23;
    String testCustomerEmail = "georgi.hristov@clouway.com";

    customerInformation.registerCustomer(testCustomerName,testCustomerEGN,testCustomerAge,testCustomerEmail);

    ResultSet resultSet = localStatement.executeQuery("SELECT * FROM people");

    while(resultSet.next()){
      recordsCount = resultSet.getInt("count");
    }


    assertThat(recordsCount,is(1));

  }


}
