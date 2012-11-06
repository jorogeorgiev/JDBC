package com.clouway.task1;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertFalse;
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
    databaseMetaData = connection.getMetaData();
    statement = connection.createStatement();
    remoteRepositoryDDQ = new RemoteRepositoryDataDescribingQuery(statement);
    remoteRepositoryDDQ.createTable(createTableStatement);
  }


  @After
  public void tearDown() throws SQLException {


    deleteTable(table);

    statement.close();

  }


  class RemoteRepositoryDataDescribingQuery {

    private final Statement statement;

    public RemoteRepositoryDataDescribingQuery(Statement statement) {

      this.statement = statement;

    }

    public void createTable(String createTableStatement) throws SQLException {

      statement.executeUpdate(createTableStatement);

    }


    public void dropTable(String tableName) throws SQLException {

      statement.executeUpdate("DROP TABLE " + tableName + ";");

    }


  }


  @Test

  public void queryCreatesTable() throws SQLException {

    assertThatTableExists();

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
    return tableExistence;
  }


  private void deleteTable(String tableName) throws SQLException {
    if (tableExistence) {
      statement.executeUpdate("DROP TABLE " + tableName + ";");
    }
  }

}
