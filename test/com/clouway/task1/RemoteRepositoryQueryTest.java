package com.clouway.task1;

import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

/**
 * @author georgi.hristov@clouway.com
 */
public class RemoteRepositoryQueryTest {

  private final String COLUMN = "firstname";
  private final String EXPECTED_NAME = "GEORGI";
  private final String RESULT_NAME = "";
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


  class RemoteRepositoryQuery {

    private final String repositoryAddress;
    private final String repositoryUsername;
    private final String repositoryPassword;
    private Connection repositoryConnection;
    private List<PreparedStatement> preparedStatementList;
    private Statement selectStatement;
    private PreparedStatement selectEgnStatement;
    private PreparedStatement selectWithLikeStatement;


    public RemoteRepositoryQuery(String repositoryAddress, String repositoryUsername, String repositoryPassword) {

      this.repositoryAddress = repositoryAddress;
      this.repositoryUsername = repositoryUsername;
      this.repositoryPassword = repositoryPassword;

    }

    public void connect() throws SQLException {

      repositoryConnection = DriverManager.getConnection(repositoryAddress, repositoryUsername, repositoryPassword);

    }


    public void createStatements() throws SQLException {

      selectStatement = repositoryConnection.createStatement();

      preparedStatementList = Lists.newArrayList();

      selectEgnStatement = repositoryConnection.prepareStatement("SELECT * FROM customer WHERE egn=?;");
      preparedStatementList.add(selectEgnStatement);

      selectWithLikeStatement = repositoryConnection.prepareStatement("SELECT  * FROM customer WHERE firstname LIKE ?;");
      preparedStatementList.add(selectWithLikeStatement);


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


    public void close() throws SQLException {
      if (selectStatement != null) {
        selectStatement.close();
      }
      if (repositoryConnection != null) {
        repositoryConnection.close();
      }
      for (PreparedStatement statement : preparedStatementList) {
        if (statement != null) {
          statement.close();
        }
      }

    }


  }

  @Test
  public void getsRecordFromMultiAttributes() throws SQLException {

    set = query.getRecords("egn,firstname,lastname", "customer");
    assertResult(COLUMN, EXPECTED_NAME, RESULT_NAME);

  }


  @Test
  public void getsRecordDueSpecifiedEgn() throws SQLException {

    set = query.getSpecificEgnRecord("8903191401");
    assertResult(COLUMN, EXPECTED_NAME, RESULT_NAME);

  }


  @Test
  public void getsRecordWhereNameStartsWithParticularLetter() throws SQLException {

    set = query.getSpecificNameStartingWith("G");
    assertResult(COLUMN, EXPECTED_NAME, RESULT_NAME);


  }

  @Test
  public void getsRecordWhereNameContainesCharSequence() throws SQLException {

    set = query.getSpecificNameContaining("ORG");
    assertResult(COLUMN, EXPECTED_NAME, RESULT_NAME);

  }

  @Test
  public void getsRecordWhereNameEndsWithParticularLetter() throws SQLException {

    set = query.getSpecificNameEnding("I");
    assertResult(COLUMN, EXPECTED_NAME, RESULT_NAME);

  }


  private void assertResult(String column, String expected, String result) throws SQLException {
    while (set.next()) {
      result = set.getString(column);
    }
    assertThat(result, is(expected));
  }


}
