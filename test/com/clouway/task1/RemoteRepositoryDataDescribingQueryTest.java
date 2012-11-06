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
  private String incorrectCreateTableStatement = "Create table " + table + "(test varchar(32);";
  private String correctCreateTableStatement = "Create table " + table + "(test varchar(32));";


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

  }


  class RemoteRepositoryDataDescribingQuery {

    private final Statement statement;

    private QueryMessages message;

    public RemoteRepositoryDataDescribingQuery(Statement statement, QueryMessages message) {

      this.statement = statement;

      this.message = message;

    }

    public String createTable(String createTableStatement) {

      String queryStatus = "";

      try {

        statement.executeUpdate(createTableStatement);

        queryStatus = message.onSuccess();

      } catch (SQLException e) {

        queryStatus = message.onFailure();

      }

      return queryStatus;

    }


    public String dropTable(String tableName) throws SQLException {


      String queryStatus = "";

      try {

        statement.executeUpdate("DROP TABLE " + tableName + ";");

        queryStatus = message.onSuccess();

      } catch (SQLException e) {

        queryStatus = message.onFailure();

      }

      return queryStatus;

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

  class Report {

    private Map<String, QueryExecutionReport> report = new HashMap<String, QueryExecutionReport>();

    public Map<String, QueryExecutionReport> createReportStatements() {

      report.put("success", new QueryExecutionReportOnSuccess());

      report.put("failure", new QueryExecutionReportOnFailure());

      return report;

    }

  }


  @Test
  public void successfulCreatingTableExecutionNotifiesUser() throws SQLException {

    assertQueryExecutionResult("success", remoteRepositoryDDQ.createTable(correctCreateTableStatement));

    deleteTable(table);

  }

  @Test
  public void failedCreatingTableExecutionNotifiesUser() throws SQLException {

    assertQueryExecutionResult("failure", remoteRepositoryDDQ.createTable(incorrectCreateTableStatement));

  }

  @Test
  public void successfulTableDroppingNotifiesUser() throws SQLException {

    remoteRepositoryDDQ.createTable(correctCreateTableStatement);

    assertQueryExecutionResult("success", remoteRepositoryDDQ.dropTable(table));

  }

  @Test
  public void failedTableDroppingNotifiesUser() throws SQLException {

    assertQueryExecutionResult("failure", remoteRepositoryDDQ.dropTable(table));

  }


  private void assertQueryExecutionResult(String result, String queryStatus) {

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
    Boolean tableExistence = false;
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
