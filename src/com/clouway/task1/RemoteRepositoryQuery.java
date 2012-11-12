package com.clouway.task1;


import com.google.common.collect.Lists;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;
import java.util.Scanner;

/**
 * @author georgi.hristov@clouway.com
 */
public class RemoteRepositoryQuery {

  private final String repositoryAddress;
  private final String repositoryUsername;
  private final String repositoryPassword;
  private Connection repositoryConnection;
  private List<Statement> preparedStatementList;
  private Statement alterStatement;
  private Statement createTableStatement;
  private Statement dropStatement;
  private Statement selectStatement;
  private PreparedStatement selectEgnStatement;
  private PreparedStatement selectWithLikeStatement;
  private PreparedStatement updateEmailStatement;
  private PreparedStatement insertRecordStatement;
  private PreparedStatement deleteRecordStatement;


  public RemoteRepositoryQuery(String repositoryAddress, String repositoryUsername, String repositoryPassword) {

    this.repositoryAddress = repositoryAddress;
    this.repositoryUsername = repositoryUsername;
    this.repositoryPassword = repositoryPassword;

  }

  public void connect() throws SQLException {

    repositoryConnection = DriverManager.getConnection(repositoryAddress, repositoryUsername, repositoryPassword);

  }


  public void createStatements() throws SQLException {

    preparedStatementList = Lists.newArrayList();

    createTableStatement=createStatement();

    alterStatement = createStatement();

    dropStatement = createStatement();

    selectStatement = createStatement();

    selectEgnStatement = createStatement("SELECT * FROM customer WHERE egn=?");

    selectWithLikeStatement = createStatement("SELECT  * FROM customer WHERE firstname LIKE ?");

    updateEmailStatement = createStatement("UPDATE customer SET email=? WHERE egn=?");

    insertRecordStatement = createStatement("INSERT INTO customer VALUES(?,?,?,?,?)");

    deleteRecordStatement = createStatement("DELETE FROM customer WHERE firstname=? AND lastname=?");

  }

  private PreparedStatement createStatement(String statementSyntax) throws SQLException {
    PreparedStatement statement = repositoryConnection.prepareStatement(statementSyntax);
    preparedStatementList.add(statement);
    return statement;
  }

  private Statement createStatement() throws SQLException {

    Statement statement = repositoryConnection.createStatement();
    preparedStatementList.add(statement);
    return statement;
  }


  public void createTable(String sqlStatemenet) throws SQLException {

    createTableStatement.executeUpdate(sqlStatemenet);

  }


  public ResultSet getRecords(String columnNames, String tableNames) throws SQLException {

    return selectStatement.executeQuery("SELECT " + columnNames + " From " + tableNames + ";");

  }


  public ResultSet getSpecificEgnRecord(String egn) throws SQLException {

    selectEgnStatement.setString(1, egn);

    return selectEgnStatement.executeQuery();

  }

  public ResultSet getSpecificNameStartingWith(String startCharacters) throws SQLException {

    selectWithLikeStatement.setString(1, startCharacters + "%");

    return selectWithLikeStatement.executeQuery();

  }

  public ResultSet getSpecificNameContaining(String charSequence) throws SQLException {

    selectWithLikeStatement.setString(1, "%" + charSequence + "%");

    return selectWithLikeStatement.executeQuery();

  }

  public ResultSet getSpecificNameEnding(String endCharacter) throws SQLException {

    selectWithLikeStatement.setString(1, "%" + endCharacter);

    return selectWithLikeStatement.executeQuery();

  }

  public void updateEmail(String newEmail, String egn) throws SQLException {

    updateEmailStatement.setString(1, newEmail);

    updateEmailStatement.setString(2, egn);

    updateEmailStatement.executeUpdate();

  }

  public void insertValues(String firstname, String lastname, Integer age, String egn, String email) throws SQLException {

    insertRecordStatement.setString(1, firstname);

    insertRecordStatement.setString(2, lastname);

    insertRecordStatement.setInt(3, age);

    insertRecordStatement.setString(4, egn);

    insertRecordStatement.setString(5, email);

    insertRecordStatement.executeUpdate();

  }

  public void deleteRecord(String firstname, String secondname) throws SQLException {

    deleteRecordStatement.setString(1, firstname);

    deleteRecordStatement.setString(2, secondname);

    deleteRecordStatement.executeUpdate();

  }

  public void dropTable(String tablename) throws SQLException {

    dropStatement.executeUpdate("DROP TABLE " + tablename + ";");

  }


  public void addColumn(String columnName) throws SQLException {

    alterStatement.executeUpdate("ALTER TABLE customer ADD " + columnName + " varchar(32);");

  }

  public void deleteColumn(String columnName) throws  SQLException {

    alterStatement.executeUpdate("ALTER TABLE customer DROP COLUMN "+columnName+";");

  }


  public void close() throws SQLException {

    if (repositoryConnection != null) {
      repositoryConnection.close();
    }
    for (Statement statement : preparedStatementList) {
      if (statement != null) {
        statement.close();
      }
    }

  }


}

class DropOrAdd{

  private RemoteRepositoryQuery query;

  public DropOrAdd(RemoteRepositoryQuery query) {

    this.query = query;

  }

  public void executeCommand(String command, String columnName) throws SQLException {
    if(command.equals("add")){

      query.addColumn(columnName);

    } else{

      query.deleteColumn(columnName);

    }



  }

}