package com.clouway.task1;

import com.google.common.collect.Maps;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;

/**
 * @author georgi.hristov@clouway.com
 */
public class DatabaseQueryTest {
  private final String CREATE_TABLE_COMMAND = "create table";
  private final String DROP_TABLE_COMMAND = "drop table";
  private final String ADD_COLUMN_COMMAND = "add column";
  private final String DELETE_COLUMN_COMMAND = "delete column";

  private final String COLUMN = "firstname";
  private final String EXPECTED_RESULT = "GEORGI";
  private DatabaseQuery dbQuery;

  private Map<String, DataDefinitionQuery> dataDefinitionQueries = Maps.newHashMap();

  @Before
  public void setUp() throws SQLException {

    String dbName = "task1";
    String dbAddress = "jdbc:postgresql://localhost:5432/" + dbName;
    String dbUsername = "postgres";
    String dbPassword = "123456";

    dbQuery = new DatabaseQuery(DriverManager.getConnection(dbAddress, dbUsername, dbPassword));
    dbQuery.prepareStatements();

    String testTable = "testTable";
    String testColumn = "testColumn";

    dataDefinitionQueries.put(CREATE_TABLE_COMMAND, new CreateTableQuery(dbQuery, testTable));
    dataDefinitionQueries.put(DROP_TABLE_COMMAND, new DropTableQuery(dbQuery, testTable));
    dataDefinitionQueries.put(ADD_COLUMN_COMMAND, new AddColumnQuery(dbQuery, testColumn));
    dataDefinitionQueries.put(DELETE_COLUMN_COMMAND, new DeleteColumnQuery(dbQuery, testColumn));

  }

  @After
  public void tearDown() throws SQLException {

    dbQuery.close();

  }

  interface DataDefinitionQuery {

    void executeQuery() throws SQLException;

  }

  class DropTableQuery implements DataDefinitionQuery {

    private final DatabaseQuery query;
    private final String table;

    public DropTableQuery(DatabaseQuery query, String table) {
      this.query = query;
      this.table = table;
    }

    @Override
    public void executeQuery() throws SQLException {
      query.dropTable(table);
    }
  }

  class CreateTableQuery implements DataDefinitionQuery {

    private final DatabaseQuery query;
    private final String table;

    public CreateTableQuery(DatabaseQuery query, String table) {
      this.query = query;
      this.table = table;
    }

    @Override
    public void executeQuery() throws SQLException {
      query.createTable("CREATE TABLE " + table + "();");
    }
  }

  class AddColumnQuery implements DataDefinitionQuery {

    private final DatabaseQuery query;
    private final String column;

    public AddColumnQuery(DatabaseQuery query, String column) {
      this.query = query;
      this.column = column;
    }

    @Override
    public void executeQuery() throws SQLException {
      query.addColumn(column);
    }
  }

  class DeleteColumnQuery implements DataDefinitionQuery {

    private final DatabaseQuery query;
    private final String column;

    public DeleteColumnQuery(DatabaseQuery query, String column) {
      this.query = query;
      this.column = column;
    }

    @Override
    public void executeQuery() throws SQLException {
      query.deleteColumn(column);
    }
  }

  interface Counter {

    int count() throws SQLException;

  }

  class TablesCounter implements Counter {

    @Override
    public int count() throws SQLException {
      return countTables();
    }

  }

  class ColumnsCounter implements Counter {

    @Override
    public int count() throws SQLException {
      return countColumns();
    }
  }


  @Test
  public void returnsRecordAccordingSpecificEgn() throws SQLException {

    assertResult(COLUMN, EXPECTED_RESULT, dbQuery.getRecordsAccordingEgn("8903191401"));

  }

  @Test
  public void returnsRecordsStartingWithParticularCharacter() throws SQLException {

    assertResult(COLUMN, EXPECTED_RESULT, dbQuery.getRecordsAccordingNameStartingWith("G"));

  }

  @Test
  public void returnsRecordsCointainingParticularCharSequence() throws SQLException {

    assertResult(COLUMN, EXPECTED_RESULT, dbQuery.getRecordsAccordingNameContaining("ORG"));

  }

  @Test
  public void returnsRecordEndingWithParticularCharacter() throws SQLException {

    assertResult(COLUMN, EXPECTED_RESULT, dbQuery.getRecordsAccordingNameEndingOn("I"));

  }

  @Test
  public void updatesEmailWhenEgnIsSpecified() throws SQLException {

    dbQuery.updateEmail("ggeorgiev@evo.bg", "8903191401");

    assertResult("email", "ggeorgiev@evo.bg", dbQuery.getRecords("email", "customer"));

  }

  @Test
  public void insertsNewRecordIntoDabatase() throws SQLException {

    dbQuery.insertRecord("IVAN", "IVANOV", 23, "8904041404", "ivan.ivanov@abv.bg");

    assertResult("count", "2", dbQuery.getRecords("count(*)", "customer"));

  }

  @Test
  public void deleteRecordFromRepository() throws SQLException {

    dbQuery.deleteRecord("IVAN", "IVANOV");

    assertResult("count", "1", dbQuery.getRecords("count(*)", "customer"));

  }

  @Test
  public void createsNewTableIntoDatabase() throws SQLException {

    assertCount(dataDefinitionQueries, CREATE_TABLE_COMMAND, new TablesCounter());

  }

  @Test
  public void dropsTableFromDatabase() throws SQLException {

    assertCount(dataDefinitionQueries, DROP_TABLE_COMMAND, new TablesCounter());

  }

  @Test
  public void addAdditionalColumnToTheTable() throws SQLException {

    assertCount(dataDefinitionQueries, ADD_COLUMN_COMMAND, new ColumnsCounter());

  }

  @Test
  public void deleteColumnFromATable() throws SQLException {

    assertCount(dataDefinitionQueries, DELETE_COLUMN_COMMAND, new ColumnsCounter());

  }

  private void assertCount(Map<String, DataDefinitionQuery> map, String command, Counter counter) throws SQLException {

    int countBeforeExecution = counter.count();

    map.get(command).executeQuery();

    int countAfterExecution = counter.count();

    assertThat(countAfterExecution, not(countBeforeExecution));
  }

  private int countTables() throws SQLException {

    int temp = 0;

    ResultSet resultSet = dbQuery.getRecords("count(*)", "information_schema.tables");

    while (resultSet.next()) {

      temp = Integer.valueOf(resultSet.getString("count"));

    }

    return temp;

  }

  private int countColumns() throws SQLException {

    ResultSetMetaData setMetaData = dbQuery.getRecords("*", "customer").getMetaData();

    return setMetaData.getColumnCount();

  }

  private void assertResult(String column, String expected, ResultSet resultSet) throws SQLException {

    String result = "";

    while (resultSet.next()) {

      result = resultSet.getString(column);

    }

    assertThat(result, is(expected));

  }

}