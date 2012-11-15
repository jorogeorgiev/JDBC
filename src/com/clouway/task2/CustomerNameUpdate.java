package com.clouway.task2;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
* @author georgi.hristov@clouway.com
*/
class CustomerNameUpdate implements CustomerUpdate {

  private Connection dbConnection;

  private PreparedStatement update;

  public CustomerNameUpdate(Connection dbConnection) {

    this.dbConnection = dbConnection;


  }

  public void prepareStatement() throws SQLException {

    update = dbConnection.prepareStatement("UPDATE people SET name = ? WHERE egn=? ;");

  }

  @Override
  public void updateProperty(String egn, String newName) throws SQLException {

    update.setString(1, newName);

    update.setString(2, egn);

    update.executeUpdate();
  }

}
