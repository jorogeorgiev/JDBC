package com.clouway.task3;

import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertThat;

/**
 * @author georgi.hristov@clouway.com
 */
public class HolidayTest {

  interface Person {

    String getName();
    String getEgn();
    Integer getAge();
    String getEmail();

  }

  abstract class EmptyPerson implements Person{


    @Override
    public String getName() {
      return "";
    }

    @Override
    public String getEgn() {
      return "";
    }

    @Override
    public Integer getAge() {
      return 0;
    }

    @Override
    public String getEmail() {
      return "";
    }
  }


  class NoSuchPerson extends  EmptyPerson{


  }

  class RegisteredPerson implements Person {

    private final String name;
    private final String egn;
    private final Integer age;
    private final String email;

    public RegisteredPerson(String name, String egn, Integer age, String email) {

      this.name = name;
      this.egn = egn;
      this.age = age;
      this.email = email;

    }


    @Override
    public String getName() {
      return name;
    }

    @Override
    public String getEgn() {
     return egn;
    }

    @Override
    public Integer getAge() {
      return age;
    }

    @Override
    public String getEmail() {
      return email;
    }
  }


  class Register {

    private Connection dbConnection;
    private PreparedStatement registerPerson;
    private Statement loginPerson;

    public Register(Connection dbConnection) throws SQLException {

      this.dbConnection = dbConnection;
      prepareStatements();

    }

    private void prepareStatements() throws SQLException {

      registerPerson = dbConnection.prepareStatement("INSERT INTO people VALUES (?,?,?,?);");
      loginPerson = dbConnection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,ResultSet.CONCUR_UPDATABLE);

    }


    public void registerPerson(String name, String egn, Integer age, String email) throws SQLException {

      registerPerson.setString(1, name);
      registerPerson.setString(2, egn);
      registerPerson.setInt(3, age);
      registerPerson.setString(4, email);
      registerPerson.executeUpdate();


    }

    public Person logPerson(String egn) throws SQLException {

      Person person;

      ResultSet set = loginPerson.executeQuery("SELECT * from people WHERE egn='" + egn + "';");

      set.first();

      if (set.first()){

        person = new RegisteredPerson(set.getString("name"),set.getString("egn"),set.getInt("age"),set.getString("email"));

      }   else{

        person = new NoSuchPerson();
      }

      return person;
    }


  }


  @Test
  public void registerNewUser() throws SQLException {



    String dbAddress = "jdbc:postgresql://localhost:5432/holiday";

    String dbUsername = "postgres";

    String dbPassword = "123456";

    Connection dbconnection = DriverManager.getConnection(dbAddress, dbUsername, dbPassword);

    Statement tempStatement = dbconnection.createStatement();

    Register personRegistration = new Register(dbconnection);

    String testName = "Georgi";
    String testEgn = "1213141516";
    Integer testAge = 33;
    String testEmail = "test@test.com";

    int recordCountBeforeRegistration = 0;

    ResultSet records;

    records = tempStatement.executeQuery("SELECT count(*) FROM people");
    while (records.next()) {
      recordCountBeforeRegistration = records.getInt("count");
    }

    personRegistration.registerPerson(testName, testEgn, testAge, testEmail);

    int recordCountAfterRegistration = 0;

    records = tempStatement.executeQuery("SELECT count(*) FROM people");
    while (records.next()) {
      recordCountAfterRegistration = records.getInt("count");
    }

    assertThat(recordCountAfterRegistration, not(recordCountBeforeRegistration));


  }

  @Test
  public void logsPersonThatExistsIntoDatabase() throws SQLException {

    String dbAddress = "jdbc:postgresql://localhost:5432/holiday";

    String dbUsername = "postgres";

    String dbPassword = "123456";

    Connection dbconnection = DriverManager.getConnection(dbAddress, dbUsername, dbPassword);

    Register personLogin = new Register(dbconnection);

    Person person =  personLogin.logPerson("8903191401");

    assertThat(person.getEgn(),is("8903191401"));





  }




}
