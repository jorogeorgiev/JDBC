package com.clouway.task1;

import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

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
  private Map<String, QueryExecutionReport> reportMap;
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
    reportMap = new Report().createReportStatements();
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




  interface QueryExecutionReport {

    String showReport();

  }



  class QueryExecutionReportOnSuccess implements QueryExecutionReport {

    @Override
    public String showReport() {
     return queryMessage.onSuccess();
    }

  }


  class QueryExecutionReportOnFailure implements QueryExecutionReport {

    @Override
    public String showReport() {
      return queryMessage.onFailure();
    }
  }


  class Report{

    private Map<String , QueryExecutionReport> report = new HashMap<String,QueryExecutionReport>();

    public Map<String , QueryExecutionReport> createReportStatements(){

      report.put("success",new QueryExecutionReportOnSuccess());
      report.put("failure", new QueryExecutionReportOnFailure());
      return report;
    }

  }





  @Test

  public void queryCreatesTable() throws SQLException {

    assertThatTableExists();

    deleteTable(table);

  }


  @Test
  public void queryIndicatedCreatingTableStatementExecutionSuccess() throws SQLException {

    assertQueryExecutionResult("success","Create table fable(test varchar(32));");

    deleteTable("fable");

  }

  @Test
  public void queryIndicatedCreatingTableStatementExecutionFailure() throws SQLException {

    assertQueryExecutionResult("failure","Create table fable(test varchar(32);");

  }


  @Test
  public void queryDropsTable() throws SQLException {

    remoteRepositoryDDQ.dropTable(table);

    assertThatTableNotExists();

  }


  private void assertQueryExecutionResult(String  result, String statement){

    String queryStatus = remoteRepositoryDDQ.createTable(statement);

    assertThat(queryStatus, is(reportMap.get(result).showReport()));

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

      statement.executeUpdate("DROP TABLE " + tableName + ";");

  }

}
