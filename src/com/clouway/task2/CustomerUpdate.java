package com.clouway.task2;

import java.sql.SQLException;

/**
 * @author georgi.hristov@clouway.com
 */
public interface CustomerUpdate {

  void updateProperty(String egn, String value) throws SQLException;

  void prepareStatement() throws SQLException;

}