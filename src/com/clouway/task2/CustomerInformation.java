package com.clouway.task2;

/**
 * @author georgi.hristov@clouway.com
 */
public class CustomerInformation {

  private final String name;
  private final String eng;
  private final String age;
  private final String email;


  public CustomerInformation(String name , String eng, String age, String email){
    this.name = name;
    this.eng = eng;
    this.age = age;
    this.email = email;
  }

  public void displayInformation(){
    System.out.println("-----------------[Record]-----------------");
    System.out.println("Customer's name: " + name);
    System.out.println("Customer's egn: " + eng);
    System.out.println("Customer's age " + age);
    System.out.println("Customer's email" + email);
    System.out.println("------------------------------------------");
  }



}
