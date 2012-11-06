package com.clouway.task1;

import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * @author georgi.hristov@clouway.com
 */
public class RemoteRepositoryDataDescribingQueryTest {

  private Boolean tableExistence = false;
  private String catalog = null;
  private String schemaPattern = null;
  private String tableNamePattern = null;
  private String[] types = null;
  private QueryMessages queryMessage;
  private Statement statement;
  private DatabaseMetaData databaseMetaData;
  private RemoteRepositoryDataDescribingQuery remoteRepositoryDDQ;
  private String table = "task1";
  private String createTableStatement = "CREATE TABLE " + table +
          "(" +
          "name varchar(32)," +
          "age int," +
          "egn varchar(10) NOT NULL," +
          "email varchar(32)," +
          "CONSTRAINT task1_pk PRIMARY KEY(egn)" +
          ");";


  @Before
  public void setUp() throws SQLException {
    String repositoryAddress = "jdbc:postgresql://localhost:5432/workingdb";
    String username = "postgres";
    String password = "";
    Connection connection = DriverManager.getConnection(repositoryAddress, username, password);
    queryMessage = new QueryMessages();
    databaseMetaData = connection.getMetaData();
    statement = connection.createStatement();
    remoteRepositoryDDQ = new RemoteRepositoryDataDescribingQuery(statement, queryMessage);
    remoteRepositoryDDQ.createTable(createTableStatement);
  }

  class RemoteRepositoryDataDescribingQuery {

    private String queryStatus = "";
    private final Statement statement;
    private QueryMessages message;

    public RemoteRepositoryDataDescribingQuery(Statement statement, QueryMessages message) {

      this.statement = statement;

      this.message = message;
    }

    public String createTable(String createTableStatement) {
      try {
        statement.executeUpdate(createTableStatement);
        queryStatus = message.onSuccess();
      } catch (SQLException e) {
        queryStatus = message.onFailure();
      }
      return queryStatus;
    }


    public void dropTable(String tableName) throws SQLException {

      statement.executeUpdate("DROP TABLE " + tableName + ";");

    }


  }


  class QueryMessages {

    public String onSuccess() {

      return "Query is executed successfully";

    }

    public String onFailure() {

      return "Query is executed with failure";

    }

  }


  @Test

  public void queryCreatesTable() throws SQLException {

    assertThatTableExists();

  }


  @Test
  public void queryIndicatedCreatingTableStatementExecutionSuccess() throws SQLException {

    String createTableStatement = "Create table failed_table(test varchar(32));";

    String queryStatus = remoteRepositoryDDQ.createTable(createTableStatement);

    remoteRepositoryDDQ.dropTable("failed_table");

    assertThat(queryStatus, is(queryMessage.onSuccess()));

  }

  @Test
  public void queryIndicatedCreatingTableStatementExecutionFailure() {

    String createTableStatement = "Create table failed_table(test varchar(32);";

    String queryStatus = remoteRepositoryDDQ.createTable(createTableStatement);

    assertThat(queryStatus, is(queryMessage.onFailure()));

  }


  @Test
  public void queryDropsTable() throws SQLException {

    remoteRepositoryDDQ.dropTable(table);

    assertThatTableNotExists();

  }


  private void assertThatTableExists() throws SQLException {

    assertTrue(hasTable(table));

  }

  private void assertThatTableNotExists() throws SQLException {

    assertFalse(hasTable(table));

  }


  private boolean hasTable(String tableName) throws SQLException {

    ResultSet result = databaseMetaData.getTables(catalog, schemaPattern, tableNamePattern, types);

    while (result.next()) {
      String droppedTable = result.getString(3);
      if (droppedTable.equals(tableName)) {
        tableExistence = true;
      }
    }

    deleteTable(table);

    statement.close();

    return tableExistence;
  }


  private void deleteTable(String tableName) throws SQLException {
    if (tableExistence) {
      statement.executeUpdate("DROP TABLE " + tableName + ";");
    }
  }

}
