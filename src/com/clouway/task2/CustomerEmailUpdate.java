package com.clouway.task2;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
* @author georgi.hristov@clouway.com
*/
class CustomerEmailUpdate implements CustomerUpdate {

  private Connection dbConnection;

  private PreparedStatement update;

  public CustomerEmailUpdate(Connection dbConnection) {

    this.dbConnection = dbConnection;

  }

  public void prepareStatement() throws SQLException {

    update = dbConnection.prepareStatement("UPDATE people SET email = ? WHERE egn=? ;");

  }

  @Override
  public void updateProperty(String egn, String email) throws SQLException {

    update.setString(1, email);

    update.setString(2, egn);

    update.executeUpdate();
  }

}
