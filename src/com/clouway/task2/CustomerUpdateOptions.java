package com.clouway.task2;

import com.google.common.collect.Maps;

import java.sql.Connection;
import java.util.Map;

/**
* @author georgi.hristov@clouway.com
*/
class CustomerUpdateOptions {

  private Map<String, CustomerUpdate> customerUpdateOptions = Maps.newHashMap();
  private Connection connection;

  public CustomerUpdateOptions(Connection connection) {

    this.connection = connection;
  }

  public Map<String, CustomerUpdate> createOptions() {
    customerUpdateOptions.put("name", new CustomerNameUpdate(connection));
    customerUpdateOptions.put("age", new CustomerAgeUpdate(connection));
    customerUpdateOptions.put("email", new CustomerEmailUpdate(connection));
    return customerUpdateOptions;
  }
}
