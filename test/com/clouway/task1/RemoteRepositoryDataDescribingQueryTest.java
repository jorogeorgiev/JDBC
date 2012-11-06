package com.clouway.task1;

import org.junit.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * @author georgi.hristov@clouway.com
 */
public class RemoteRepositoryDataDescribingQueryTest {

  class RemoteRepositoryDataDescribingQuery {

    private final Connection connection;
    private final Statement statement;

    public RemoteRepositoryDataDescribingQuery(Connection connection, Statement statement) {

      this.connection = connection;

      this.statement = statement;

    }

    public void createTable(String createTableStatement) throws SQLException {

      statement.executeUpdate(createTableStatement);

    }


    public void dropTable(String tableName) throws SQLException {

      statement.executeUpdate("DROP TABLE " + tableName+ ";");

    }


  }


  @Test
  public void queryCreatesTable() throws SQLException {

    String repositoryAddress = "jdbc:postgresql://localhost:5432/workingdb";
    String username = "postgres";
    String password = "";

    Connection connection = DriverManager.getConnection(repositoryAddress, username, password);
    Statement statement = connection.createStatement();

    RemoteRepositoryDataDescribingQuery remoteRepositoryDDQ = new RemoteRepositoryDataDescribingQuery(connection, statement);

    String createTableStatement = "CREATE TABLE task1" +
            "(" +
            "name varchar(32)," +
            "age int," +
            "egn varchar(10) NOT NULL," +
            "email varchar(32)," +
            "CONSTRAINT task1_pk PRIMARY KEY(egn)" +
            ");";


    remoteRepositoryDDQ.createTable(createTableStatement);

    ResultSet resultSet = statement.executeQuery("SELECT * FROM task1");

    ResultSetMetaData metaData = resultSet.getMetaData();

    statement.executeUpdate("DROP TABLE task1");

    assertThat(metaData.getColumnCount(), is(4));

    statement.close();

    connection.close();

  }


  @Test

  public void queryDropsTable() throws SQLException {
    String repositoryAddress = "jdbc:postgresql://localhost:5432/workingdb";
    String username = "postgres";
    String password = "";

    Connection connection = DriverManager.getConnection(repositoryAddress, username, password);

    Statement statement = connection.createStatement();

    RemoteRepositoryDataDescribingQuery remoteRepositoryDDQ = new RemoteRepositoryDataDescribingQuery(connection, statement);

    String tablename = "task1";

    String createTableStatement = "CREATE TABLE " + tablename +
            "(" +
            "name varchar(32)," +
            "age int," +
            "egn varchar(10) NOT NULL," +
            "email varchar(32)," +
            "CONSTRAINT task1_pk PRIMARY KEY(egn)" +
            ");";

    remoteRepositoryDDQ.createTable(createTableStatement);
    remoteRepositoryDDQ.dropTable(tablename);


    Boolean exist=false;

    DatabaseMetaData databaseMetaData = connection.getMetaData();

    String   catalog          = null;
    String   schemaPattern    = null;
    String   tableNamePattern = null;
    String[] types            = null;

    ResultSet result = databaseMetaData.getTables(catalog, schemaPattern, tableNamePattern, types );



    while(result.next()) {
      String tableName = result.getString(3);
      if(tableName.equals(tablename)){
         exist=true;
      }
    }

    assertThat(exist,is(false));

  }

}
