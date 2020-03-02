package challenge;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.Scanner;

/**
 * This is a program that reads the .csv file with the user input
 * file name. Then collect the valid data and save them into database
 * file, invalid data into filename-bad.csv file, and count the numbers
 * of all the data, valid data, and invalid data and save them into the
 * log file.
 * 
 * @author Jinsu Jung
 *
 */
public class MS3 {
	
	/**
	 * Starts a program
	 * @param args command argument
	 * @throws IOException file Exception
	 */
    public static void main(String[] args) throws IOException {
        Scanner console = new Scanner(System.in); 
	    System.out.print("Enter file name: ");
	    String filename = console.nextLine();
	    console.close();
	    System.out.println("Please wait (Creating file)...");
	    String tablename = "'"+filename+"'";
        String line="";
	    BufferedReader br=null;         //buffered read
	    BufferedWriter bw=null;         //buffered write
	    Connection connection=null;
	    Statement statement=null;
	    FileWriter fw=new FileWriter("doc/"+filename+".log",false);   //log write
	      
	    int received=0;            //Number of received
	    int successful=0;         //Number of successful
	    int failed=0;            //Number of failed
	      
	   
	      
	    try {
	        //open files for read and write
	        br=Files.newBufferedReader(Paths.get("doc/"+filename+".csv"));
	        bw=Files.newBufferedWriter(Paths.get("doc/"+filename+"-bad.csv"),Charset.forName("UTF-8"));
	         
	        //Check for the SQLite JDBC
	        Class.forName("org.sqlite.JDBC");   
	        //Connect to the JDBC
	        connection=DriverManager.getConnection("jdbc:sqlite:"+"doc/"+filename+".db");
	        //Using the JDBC
	        statement=connection.createStatement();
	         
	        statement.execute("DROP TABLE IF EXISTS "+ tablename);
	        statement.execute("CREATE TABLE "+ tablename+"(\n"+
	                        "A VARCHAR(255), "+
	                        "B VARCHAR(255), "+
	                        "C VARCHAR(255), "+
	                        "D VARCHAR(255), "+
	                        "E VARCHAR(2000), "+
	                        "F VARCHAR(255), "+
	                        "G VARCHAR(255), "+
	                        "H VARCHAR(255), "+
	                        "I VARCHAR(255), "+
	                        "J VARCHAR(255))");
	         
            line=br.readLine();      //Removing the First row, (A,B,C,...)
	      
	    //Reading the line until End Of File
	    while((line=br.readLine()) != null) {
	        String[] str = new String[10];
	        boolean b = false;
	        int index = 0;
	        String linesplit="";
	        line = line.replaceAll("'", "''");
	         
	         
	        //parsing data
	        for(int i=0;i<line.length();i++) {
	            if(index == 10) {
	                break;
	            }
	            b=false;
	            if(line.charAt(i)==',' && b==false) {
	                str[index]=linesplit;
	                index++;
	                linesplit="";
	                continue;
	            }
	            else if(line.charAt(i)==',' && b==true) {
	                linesplit=linesplit+line.charAt(i);
	                continue;
	            }
	            else if(line.charAt(i)=='"') {
	                if(b) {
	                	b=false;
	                }
	                else {
	                	b=true;
	                }
	                continue;
	            }
	            else {
	                if(i==line.length()-1) {
	                    str[index]=linesplit;
	                    index++;
	                    break;
	                }
	                else {
	                    linesplit=linesplit+line.charAt(i);
	                    continue;
	                }
	            }
	        }
	           
	        if(str[0]==null && str[1]==null && str[2]==null && str[3]==null && str[4]==null && 
	                str[5]==null && str[6]==null && str[7]==null && str[8]==null && str[9]==null) {
	            continue;
	        }
	         
	        received++;
	         
	        if(line.contains(",,")) {
	            failed++;
	            bw.write(line);
	            bw.newLine();
	            continue;
	        }
	        else {
	            successful++;
	            statement.execute("INSERT INTO "+tablename+"(A,B,C,D,E,F,G,H,I,J) VALUES ('"+str[0]+"','"+str[1]+"','"+str[2]+"','"+str[3]+"','"+str[4]+"','"+str[5]+"','"+str[6]+"','"+str[7]+"','"+str[8]+"','"+str[9]+"')");
	            continue;
	        }
        }
	      
	      
	      //Write log file
	    fw.write(received+" of records received\n");
	    fw.append(successful+" of records successful\n");
	    fw.append(failed+" of records failed\n");
	      
        fw.close();
	    bw.flush();
	    bw.close();
	    System.out.println("Done");
	    }catch(FileNotFoundException e) {
	        e.printStackTrace();
	    }catch(IOException e) {
	        e.printStackTrace();
	    }catch(ClassNotFoundException e){
	        e.printStackTrace();
	    }catch (SQLException e) {
	        e.printStackTrace();
	    }finally {
	        try {
	            if(connection!=null){
	                connection.close();
	            }
	            if(br!=null) {
	                br.close();
	            }
	        }catch(IOException e) {
	            e.printStackTrace();
	        } catch (SQLException e) {
	            System.out.println("Couldn't find org.sqlite.JDBC");
	            e.printStackTrace();
	        }
	    }   
    }
}