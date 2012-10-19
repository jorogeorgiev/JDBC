package com.clouway.examples;

import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;

import static org.junit.Assert.assertTrue;

/**
 * @author georgi.hristov@clouway.com
 */
public class DriverManagerTest {
  private Scanner scanner;

  @Before
   public void setUp() throws FileNotFoundException, SQLException {

    String LOG_PATH = "/home/tekbwainz/Desktop/dblog.txt";

    DriverManager.setLogWriter(new PrintWriter(new FileOutputStream(new File(LOG_PATH))));

     DriverManager.getConnection("jdbc:postgresql://localhost:5432/workingdb", "postgres", "");

     scanner = new Scanner(new FileInputStream(LOG_PATH));

   }

  //logger used in the driver manager is used to log the events from DriverManager


  @Test
  public void driverManagerLoggerLogsConnectionEvents(){

    assertTrue(scanner.nextLine()!=null);

  }


  //DriverManager.println() writes to the log;

  @Test
  public void driverManagerPrintLnWritesIntoLog() throws FileNotFoundException, SQLException {

    String driverManagerMSG = "this is a test msg";

    DriverManager.println(driverManagerMSG);

    StringBuilder fileContent = new StringBuilder();

    while(scanner.hasNext()){

      fileContent.append(scanner.nextLine());

    }

    assertTrue(fileContent.toString().substring(fileContent.length() - driverManagerMSG.length(), fileContent.length()).equals(driverManagerMSG));

  }

}
