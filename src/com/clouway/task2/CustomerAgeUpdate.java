package com.clouway.task2;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
* @author georgi.hristov@clouway.com
*/
class CustomerAgeUpdate implements CustomerUpdate {

  private Connection dbConnection;

  private PreparedStatement update;

  public CustomerAgeUpdate(Connection dbConnection) {

    this.dbConnection = dbConnection;


  }

  public void prepareStatement() throws SQLException {

    update = dbConnection.prepareStatement("UPDATE people SET age = ? WHERE egn=? ;");

  }

  @Override
  public void updateProperty(String egn, String age) throws SQLException {

    update.setInt(1, Integer.valueOf(age));

    update.setString(2, egn);

    update.executeUpdate();
  }

}
