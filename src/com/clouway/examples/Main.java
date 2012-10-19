package com.clouway.examples;


import com.google.common.collect.Lists;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;
import java.util.Scanner;

/**
 * @author georgi.hristov@clouway.com
 */
public class Main {
  public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {

    String[] currCommand;

    List<String> voidCommands = Lists.newArrayList();

    voidCommands.add("DROP");
    voidCommands.add("CREATE");
    voidCommands.add("INSERT");

    StringBuilder bufferedCmd = new StringBuilder();

    Connection connection;

    Statement statement;

    ResultSet results;

    Scanner query = new Scanner(System.in);

    InputStream stream = new FileInputStream("/home/tekbwainz/Desktop/SQLLOG.txt");

    Scanner readLog = new Scanner(stream);


    StringBuilder readCmd = new StringBuilder();

    while (readLog.hasNextLine()) {

      readCmd.append(readLog.nextLine());
      readCmd.append('\n');

    }

    OutputStream logging = new FileOutputStream("/home/tekbwainz/Desktop/SQLLOG.txt");

    PrintWriter logger = new PrintWriter(logging);

    logger.print(readCmd.toString());

    logger.flush();


    connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/workingdb", "postgres", "");

    statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

    String command;

    while (query.hasNextLine()) {

      String tempCommand = query.nextLine();

      bufferedCmd.append(tempCommand);

      try {

        currCommand = tempCommand.split("\\s");

        if (!tempCommand.contains(";")) {

          bufferedCmd.append(" ");

          System.out.println("Please enter ';' to execute the command");

        } else {

          if (voidCommands.contains(currCommand[0].toUpperCase())) {

            command = bufferedCmd.toString();

            bufferedCmd.delete(0, bufferedCmd.length());

            statement.execute(command);

          } else {

            command = bufferedCmd.toString();

            bufferedCmd.delete(0, bufferedCmd.length());

            results = statement.executeQuery(command);

            if (!results.last()) {
              System.out.println("NO RECORD");
            } else {
              results.beforeFirst();
            }

            while (results.next()) {

              System.out.print(results.getString("first_name") + " ");

              System.out.print(results.getString("second_name") + " ");

              System.out.print(results.getString("egn") + " ");

              System.out.print(results.getInt("age") + " ");

              System.out.println();
            }
          }
        }

      } catch (SQLException exception) {

        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        Calendar cal = Calendar.getInstance();

        System.out.println(exception.getMessage());

        logger.write("<<" + dateFormat.format(cal.getTime()) + ">> " + exception.getMessage() + '\n');

        logger.flush();

      }

    }

  }

}