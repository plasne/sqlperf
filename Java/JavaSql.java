package custom;

import java.io.*;
import java.nio.charset.Charset;
import java.sql.*;
import java.util.Date;
import java.util.ArrayList;
import java.util.Iterator;
import java.text.SimpleDateFormat;

class Parameter {
  public String key;
  public String type;
  public String value;

  public Parameter(String key, String type, String value) {
    this.key = key;
    this.type = type;
    this.value = value;
  }

}

public class JavaSql {

  private static Parameter find(ArrayList<Parameter> parameters, String key) {
    for (Iterator<Parameter> i = parameters.iterator(); i.hasNext();) {
      Parameter parameter = i.next();
      if (parameter.key.equals(key)) return parameter;
    }
    return null;
  }

  public static void main(String args[]) {

    if (args.length < 1) {
      System.out.println("javasql.exe [path] [server] [database] [username] [password] [scenario] [action]");
      System.exit(-1);
    }

    String path = (args.length > 0) ? args[0] : "settings.txt";
    String server = (args.length > 1) ? args[1] : "pelasne-sqlsvr.database.windows.net";
    String database = (args.length > 2) ? args[2] : "pelasne-sql";
    String username = (args.length > 3) ? args[3] : "plasne";
    String password = (args.length > 4) ? args[4] : "???";
    String scenario = (args.length > 5) ? args[5] : "original";
    String action =  (args.length > 6) ? args[6] : "show";

    String template1 = null;
    String template2 = null;
    ArrayList<Parameter> full_parameters = new ArrayList<Parameter>();
    try {
      InputStream fis = new FileInputStream(path);
      InputStreamReader isr = new InputStreamReader(fis, Charset.forName("UTF-8"));
      BufferedReader br = new BufferedReader(isr);
      template1 = br.readLine();
      template2 = br.readLine();
      String line;
      while ((line = br.readLine()) != null) {
        String[] parts = line.split(",");
        full_parameters.add(new Parameter(parts[0], parts[1], parts[2]));
      }
    } catch (Exception e) {
      e.printStackTrace();
    }

    ArrayList<String> parameters = new ArrayList<String>();
    for (Iterator<Parameter> i = full_parameters.iterator(); i.hasNext();) {
      Parameter parameter = i.next();
      parameters.add(parameter.value);
    }

    String pre_query = null;
    String query = null;
    switch(scenario) {

      case "original":
        ArrayList<String> uppers = new ArrayList<String>();
        for (Iterator<Parameter> i = full_parameters.iterator(); i.hasNext();) {
          Parameter parameter = i.next();
          uppers.add("(UPPER(cgbpsecacc4_.COL_0) = UPPER('" + parameter.value + "'))");
        }
        query = template1.replace("@range", String.join(" OR ", uppers.toArray(new String[0])));
        break;

      case "original-param":
        ArrayList<String> uppers_param = new ArrayList<String>();
        for (int i = 253; i < full_parameters.size(); i++) {
          uppers_param.add("(UPPER(cgbpsecacc4_.COL_0) = UPPER(@P" + i + "))");
        }
        query = template2.replace("@range", String.join(" OR ", uppers_param.toArray(new String[0])));
        break;

    }

    switch(action) {

      case "show":
        System.out.println("PRE-QUERY:");
        System.out.println("");
        System.out.println(pre_query);
        System.out.println("");
        System.out.println("QUERY:");
        System.out.println("");
        System.out.println(query);
        break;

      case "run":
        Connection connection = null;
        PreparedStatement command = null;
        ResultSet rs = null;

        try {

          Date start = new Date();

          String connectionString = "jdbc:sqlserver://" + server + ":1433;database=" + database + ";user=" + username + ";password=" + password + ";encrypt=true;trustServerCertificate=true;loginTimeout=30;";
          Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
          connection = DriverManager.getConnection(connectionString);

          Boolean use_parameters = (scenario == "original-param");

          if (use_parameters) {
            for (int i = 0; i < full_parameters.size(); i++) {
              query = query.replace("@P" + i, "?");
            }
          }

          command = connection.prepareStatement(query);

          if (use_parameters) {
            for (int i = 0; i < full_parameters.size(); i++) {
              Parameter parameter = find(full_parameters, "@P" + i);
              switch(parameter.type) {
                case "INT":
                  command.setInt(i + 1, Integer.parseInt(parameter.value));
                  break;
                case "DATETIME2":
                  SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSSSSSS");
                  Date parsed = formatter.parse(parameter.value);
                  java.sql.Date sd = new java.sql.Date(parsed.getTime());
                  command.setDate(i + 1, sd);
                  break;
                case "NVARCHAR":
                  command.setNString(i + 1, parameter.value);
                  break;
              }
            }
          }

          rs = command.executeQuery();
          ResultSetMetaData metadata = rs.getMetaData();
          while (rs.next()) {
            ArrayList<String> columns = new ArrayList<String>();
            for (int i = 0; i < metadata.getColumnCount(); i++) {
              columns.add(rs.getString(i + 1));
            }
            System.out.println( String.join(", ", columns.toArray(new String[0])) );
          }

          Date end = new Date();
          System.out.println((end.getTime() - start.getTime()) + " ms elapsed.");

        } catch (Exception e) {
          e.printStackTrace();
        } finally {
          if (rs != null) try { rs.close(); } catch(Exception e) { };
          if (command != null) try { command.close(); } catch(Exception e) { };
          if (connection != null) try { connection.close(); } catch(Exception e) { };
        }
        break;

    }

  }

}
