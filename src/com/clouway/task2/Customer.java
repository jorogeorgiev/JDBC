package com.clouway.task2;

import com.google.common.collect.Lists;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

/**
 * @author georgi.hristov@clouway.com
 */
public  class Customer {

  private Connection customerDBConnection;
  private Map<String, CustomerUpdate> customerUpdateOptions;
  private PreparedStatement registerCustomer;


  public Customer(Connection customerDBConnection, CustomerUpdateOptions options) {

    this.customerDBConnection = customerDBConnection;

    customerUpdateOptions = options.createOptions();

  }

  public void prepareStatements() throws SQLException {

    registerCustomer = customerDBConnection.prepareStatement("INSERT INTO people VALUES(?,?,?,?);");

    for (Map.Entry<String, CustomerUpdate> customerUpdateOption : customerUpdateOptions.entrySet()) {

      customerUpdateOption.getValue().prepareStatement();

    }

  }


  public void registerCustomer(String name, String egn, Integer age, String email) throws SQLException {

    registerCustomer.setString(1, name);

    registerCustomer.setString(2, egn);

    registerCustomer.setInt(3, age);

    registerCustomer.setString(4, email);

    registerCustomer.executeUpdate();

  }

  public void updateCustomerName(String egn, String newName) throws SQLException {

    customerUpdateOptions.get("name").updateProperty(egn, newName);

  }

  public void updateCustomerAge(String egn, Integer age) throws SQLException {

    customerUpdateOptions.get("age").updateProperty(egn, String.valueOf(age));

  }

  public void updateCustomerEmail(String egn, String email) throws SQLException {

    customerUpdateOptions.get("email").updateProperty(egn, String.valueOf(email));

  }

  public List<CustomerInformation> getCustomers() throws SQLException {

    Statement getCustomers = customerDBConnection.createStatement();
    List<CustomerInformation> customers = Lists.newArrayList();
    ResultSet customersInformation  = getCustomers.executeQuery("SELECT * FROM people ;");
    while(customersInformation.next()){
      String customerName= customersInformation.getString("name");
      String customerEgn = customersInformation.getString("egn");
      String customerAge =String.valueOf(customersInformation.getInt("age"));
      String customerEmail =customersInformation.getString("email");
      customers.add(new CustomerInformation(customerName,customerEgn,customerAge,customerEmail));

    }
    return customers;

  }

  public List<CustomerInformation> getCustomersWithStartingLetter(String charSequence) throws SQLException {

    Statement getCustomers = customerDBConnection.createStatement();
    List<CustomerInformation> customers = Lists.newArrayList();
    ResultSet customersInformation  = getCustomers.executeQuery("SELECT * FROM people WHERE name LIKE '"+charSequence+"%';");
    while(customersInformation.next()){
      String customerName= customersInformation.getString("name");
      String customerEgn = customersInformation.getString("egn");
      String customerAge =String.valueOf(customersInformation.getInt("age"));
      String customerEmail =customersInformation.getString("email");
      customers.add(new CustomerInformation(customerName,customerEgn,customerAge,customerEmail));

    }
    return customers;

  }
}
