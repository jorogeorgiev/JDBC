package com.clouway.task1;


import com.google.common.collect.Lists;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/**
 * @author georgi.hristov@clouway.com
 */
public class DatabaseQuery {

  private Connection databaseConnection;
  private List<Statement> statements;
  private Statement alterTable;
  private Statement createTable;
  private Statement dropTable;
  private Statement select;
  private PreparedStatement recordsAccordingEgn;
  private PreparedStatement recordsAccordingName;
  private PreparedStatement updateEmail;
  private PreparedStatement insertRecord;
  private PreparedStatement deleteRecord;


  public DatabaseQuery(Connection databaseConnection) {

    this.databaseConnection = databaseConnection;

  }


  public void prepareStatements() throws SQLException {

    statements = Lists.newArrayList();

    createTable =createStatement();

    alterTable = createStatement();

    dropTable = createStatement();

    select = createStatement();

    recordsAccordingEgn = createStatement("SELECT * FROM customer WHERE egn=?");

    recordsAccordingName = createStatement("SELECT  * FROM customer WHERE firstname LIKE ?");

    updateEmail = createStatement("UPDATE customer SET email=? WHERE egn=?");

    insertRecord = createStatement("INSERT INTO customer VALUES(?,?,?,?,?)");

    deleteRecord = createStatement("DELETE FROM customer WHERE firstname=? AND lastname=?");

  }

  private PreparedStatement createStatement(String statementSyntax) throws SQLException {
    PreparedStatement statement = databaseConnection.prepareStatement(statementSyntax);
    statements.add(statement);
    return statement;
  }

  private Statement createStatement() throws SQLException {
    Statement statement = databaseConnection.createStatement();
    statements.add(statement);
    return statement;
  }

  public void createTable(String statement) throws SQLException {

    createTable.executeUpdate(statement);

  }


  public void dropTable(String tablename) throws SQLException {

    dropTable.executeUpdate("DROP TABLE " + tablename + ";");

  }


  public void addColumn(String columnName) throws SQLException {

    alterTable.executeUpdate("ALTER TABLE customer ADD " + columnName + " varchar(32);");

  }

  public void deleteColumn(String columnName) throws  SQLException {

    alterTable.executeUpdate("ALTER TABLE customer DROP COLUMN " + columnName + ";");

  }

  public void insertRecord(String firstName, String lastName, Integer age, String egn, String email) throws SQLException {

    insertRecord.setString(1, firstName);

    insertRecord.setString(2, lastName);

    insertRecord.setInt(3, age);

    insertRecord.setString(4, egn);

    insertRecord.setString(5, email);

    insertRecord.executeUpdate();

  }

  public ResultSet getRecords(String columnNames, String tableNames) throws SQLException {

    return select.executeQuery("SELECT " + columnNames + " From " + tableNames + ";");

  }

  public void deleteRecord(String firstname, String secondname) throws SQLException {

    deleteRecord.setString(1, firstname);

    deleteRecord.setString(2, secondname);

    deleteRecord.executeUpdate();

  }

  public ResultSet getRecordsAccordingEgn(String egn) throws SQLException {

    recordsAccordingEgn.setString(1, egn);

    return recordsAccordingEgn.executeQuery();

  }

  public ResultSet getRecordsAccordingNameStartingWith(String startCharacters) throws SQLException {

    recordsAccordingName.setString(1, startCharacters + "%");

    return recordsAccordingName.executeQuery();

  }

  public ResultSet getRecordsAccordingNameContaining(String charSequence) throws SQLException {

    recordsAccordingName.setString(1, "%" + charSequence + "%");

    return recordsAccordingName.executeQuery();

  }

  public ResultSet getRecordsAccordingNameEndingOn(String endCharacter) throws SQLException {

    recordsAccordingName.setString(1, "%" + endCharacter);

    return recordsAccordingName.executeQuery();

  }

  public void updateEmail(String newEmail, String egn) throws SQLException {

    updateEmail.setString(1, newEmail);

    updateEmail.setString(2, egn);

    updateEmail.executeUpdate();

  }

  public void close() throws SQLException {

    if (databaseConnection != null) {

      databaseConnection.close();

    }

    for (Statement statement : statements) {

      if (statement != null) {

        statement.close();

      }
    }
  }
}
