package com.clouway.task2;

import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertThat;

/**
 * @author georgi.hristov@clouway.com
 */
public class TripTest {
  private String testCustomerName;
  private String testCustomerEGN;
  private Integer testCustomerAge;
  private String testCustomerEmail;
  private Statement localStatement;
  private Customer customer;
  private ResultSet set;

  @Before
  public void setUp() throws SQLException {
    String dbAddress = "jdbc:postgresql://localhost:5432/holiday";
    String dbUsername = "postgres";
    String dbPassword = "123456";

    testCustomerName = "Georgi";
    testCustomerEGN = "8903191401";
    testCustomerAge = 23;
    testCustomerEmail = "georgi.hristov@clouway.com";

    Connection customerDBConnection = DriverManager.getConnection(dbAddress, dbUsername, dbPassword);
    localStatement = customerDBConnection.createStatement();
    CustomerUpdateOptions options = new CustomerUpdateOptions(customerDBConnection);
    customer = new Customer(customerDBConnection, options);
    customer.prepareStatements();


  }


  @Test
  public void registerNewCustomerIntoDatabase() throws SQLException {
    int recordsCount1=0;
    set = localStatement.executeQuery("SELECT count(*) FROM people;");
    while (set.next()) {
      recordsCount1 = set.getInt("count");
    }
    int recordsCount2 = 0;
    customer.registerCustomer("Ivan", "123456789", 32, "test@evo.bg");
    set = localStatement.executeQuery("SELECT count(*) FROM people;");
    while (set.next()) {
      recordsCount2 = set.getInt("count");
    }
    localStatement.executeUpdate("DELETE FROM people WHERE egn='123456789';");
    assertThat(recordsCount1, not(recordsCount2));

  }

  @Test
  public void changesCustomerNameWithNewValue() throws SQLException {
    customer.updateCustomerName(testCustomerEGN, "Ivan");
    set = localStatement.executeQuery("SELECT * FROM people WHERE egn='8903191401';");
    while (set.next()) {
      testCustomerName = set.getString("name");
    }

    customer.updateCustomerName(testCustomerEGN,"Georgi");
    assertThat(testCustomerName, is("Ivan"));

  }


  @Test
  public void changesCustomerAgeWithNewValue() throws SQLException {
    customer.updateCustomerAge(testCustomerEGN, 32);
    set = localStatement.executeQuery("SELECT * FROM people WHERE egn='8903191401';");
    while (set.next()) {
      testCustomerAge = set.getInt("age");
    }
    customer.updateCustomerAge(testCustomerEGN, 24);
    assertThat(testCustomerAge, is(32));

  }

  @Test
  public void changesCustomerEmailWithNewValue() throws SQLException {
    customer.updateCustomerEmail(testCustomerEGN, "ggeorgiev@evo.bg");
    set = localStatement.executeQuery("SELECT * FROM people WHERE egn='8903191401';");
    while (set.next()) {
      testCustomerEmail = set.getString("email");
    }
    customer.updateCustomerEmail(testCustomerEGN,"georgi.hristov@clouway.com");
    assertThat(testCustomerEmail, is("ggeorgiev@evo.bg"));

  }

  @Test
  public void returnsCustomersInformation() throws SQLException {

    int customersCount=0;

    List<CustomerInformation> customerInformationList = customer.getCustomers();

    set = localStatement.executeQuery("SELECT count(*) FROM people;");
    while(set.next()){
      customersCount=set.getInt("count");
    }

    assertThat(customerInformationList.size(),is(customersCount));


  }


  @Test
  public void returnsCustomersInformationWhereNameStartsWithCharSequence() throws SQLException {

    int customersCount=0;
    String charSequence = "Geo";

    List<CustomerInformation> customerInformationList = customer.getCustomersWithStartingLetter(charSequence);

    set = localStatement.executeQuery("SELECT count(*) FROM people WHERE name LIKE '"+charSequence+"%';");
    while(set.next()){
      customersCount=set.getInt("count");
    }

    assertThat(customerInformationList.size(),is(customersCount));


  }

}
