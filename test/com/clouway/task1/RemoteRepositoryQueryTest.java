package com.clouway.task1;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;

/**
 * @author georgi.hristov@clouway.com
 */
public class RemoteRepositoryQueryTest {

  private final String COLUMN = "firstname";
  private final String EXPECTED_NAME = "GEORGI";
  private RemoteRepositoryQuery query;
  private ResultSet set;

  @Before
  public void setUp() throws SQLException {
    String dbName = "task1";
    String dbAddress = "jdbc:postgresql://localhost:5432/" + dbName;
    String dbUsername = "postgres";
    String dbPassword = "123456";

    query = new RemoteRepositoryQuery(dbAddress, dbUsername, dbPassword);
    query.connect();
    query.createStatements();


  }

  @After
  public void tearDown() throws SQLException {

    query.close();

  }






  class DropOrCreate{

    private RemoteRepositoryQuery query;

    public DropOrCreate(RemoteRepositoryQuery query) {

      this.query = query;

    }

    public void executeCommand(String command, String table) throws SQLException {
      if(command.equals("create")){

        query.createTable("CREATE TABLE " + table + "();");

      } else{

        query.dropTable(table);

      }



    }

  }

  @Test
  public void getsRecordFromMultiAttributes() throws SQLException {

    set = query.getRecords("egn,firstname,lastname", "customer");
    assertResult(COLUMN, EXPECTED_NAME);

  }


  @Test
  public void getsRecordDueSpecifiedEgn() throws SQLException {

    set = query.getSpecificEgnRecord("8903191401");
    assertResult(COLUMN, EXPECTED_NAME);

  }


  @Test
  public void getsRecordWhereNameStartsWithParticularLetter() throws SQLException {

    set = query.getSpecificNameStartingWith("G");
    assertResult(COLUMN, EXPECTED_NAME);


  }

  @Test
  public void getsRecordWhereNameContainesCharSequence() throws SQLException {

    set = query.getSpecificNameContaining("ORG");
    assertResult(COLUMN, EXPECTED_NAME);

  }

  @Test
  public void getsRecordWhereNameEndsWithParticularLetter() throws SQLException {

    set = query.getSpecificNameEnding("I");
    assertResult(COLUMN, EXPECTED_NAME);

  }

  @Test
  public void updatesEmailOfRecordUsingEgn() throws SQLException {

    query.updateEmail("ggeorgiev@evo.bg", "8903191401");

    set = query.getRecords("email", "customer");

    assertResult("email", "ggeorgiev@evo.bg");

  }

  @Test
  public void insertsNewRecordIntoREpository() throws SQLException {

    query.insertValues("IVAN", "IVANOV", 23, "8904041404", "ivan.ivanov@abv.bg");

    set = query.getRecords("count(*)", "customer");

    assertResult("count", "2");

  }

  @Test
  public void deleteRecordFromRepository() throws SQLException {

    query.deleteRecord("IVAN", "IVANOV");

    set = query.getRecords("count(*)", "customer");

    assertResult("count", "1");

  }

  @Test
  public void createsNewTableIntoRepository() throws SQLException {

    assertTablesCount(new DropOrCreate(query), "create", "test");

  }


  @Test
  public void dropsTableFromDatabase() throws SQLException {

    assertTablesCount(new DropOrCreate(query), "drop", "test");

  }


  @Test
  public void addAdditionalColumnToTheTable() throws SQLException {

    assertColumnsCount(new DropOrAdd(query),"add","country");

  }

  @Test
  public void deleteColumnFromATable() throws SQLException {

    assertColumnsCount(new DropOrAdd(query),"drop","country");

  }


  private void assertColumnsCount(DropOrAdd doa, String command, String columnName) throws SQLException {

    int countBeforeExecution = countColumns();

    doa.executeCommand(command,columnName);

    int countAfterExecution = countColumns();

    assertThat(countAfterExecution, not(countBeforeExecution));
  }

  private int countColumns() throws SQLException {

    ResultSet set  = query.getRecords("*","customer");

    ResultSetMetaData setMetaData = set.getMetaData();

    return setMetaData.getColumnCount();

  }

  private void assertTablesCount(DropOrCreate doc, String command, String table) throws SQLException {

    int countBeforeExecution = countTables();

    doc.executeCommand(command,table);

    int countAfterExecution = countTables();

    assertThat(countAfterExecution, not(countBeforeExecution));
  }

  private int countTables() throws SQLException {
    int temp=0;
    set = query.getRecords("count(*)", "information_schema.tables");
    while (set.next()) {
      temp = Integer.valueOf(set.getString("count"));
    }
    return temp;
  }

 private void assertResult(String column, String expected) throws SQLException {
    String result = "";
    while (set.next()) {
      result = set.getString(column);
    }
    assertThat(result, is(expected));
  }


}
