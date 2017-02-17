package cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class CassandraMaven {
	public static void main(String args[]){
		String serverIP = "127.0.0.1";
		String keyspace = "hr";

		final Cluster cluster = Cluster.builder()
		  .addContactPoints(serverIP)
		  .build();
		final Session session = cluster.connect(keyspace);
		System.out.println("*********Cluster Information *************");
        System.out.println(" Cluster Name is: " + cluster.getClusterName() );
        System.out.println(" Driver Version is: " + cluster.getDriverVersion() );
        System.out.println(" Cluster Configuration is: " + cluster.getConfiguration() );
        System.out.println(" Cluster Metadata is: " + cluster.getMetadata() );
        System.out.println(" Cluster Metrics is: " + cluster.getMetrics() ); 

     // Retrieve all User details from Users table
        System.out.println("\n*********Retrive User Data Example *************");        
        getUsersAllDetails(session);
         
        // Insert new User into users table
        System.out.println("\n*********Insert User Data Example *************");        
        session.execute("INSERT INTO emp (empid, emp_dept, emp_first, emp_last) VALUES (4, 'IT', 'Good', 'ForNothing')");
        getUsersAllDetails(session);
         
        // Update user data in users table
        System.out.println("\n*********Update User Data Example *************");        
        session.execute("update emp set emp_first = 'ReallyGood' where empid = 4");
        getUsersAllDetails(session);
         
        // Delete user from users table
        System.out.println("\n*********Delete User Data Example *************");        
        session.execute("delete FROM emp where empid = 4");
        getUsersAllDetails(session);
         
        // Close Cluster and Session objects
        System.out.println("\nIs Cluster Closed :" + cluster.isClosed());
        System.out.println("Is Session Closed :" + session.isClosed());     
        cluster.close();
        session.close();
        System.out.println("Is Cluster Closed :" + cluster.isClosed());
        System.out.println("Is Session Closed :" + session.isClosed());
    }
     
    private static void getUsersAllDetails(final Session inSession){        
        // Use select to get the users table data
        ResultSet results = inSession.execute("SELECT * FROM emp");
        for (Row row : results) {
        	System.out.println(row.getInt("empid") + " " +
                    row.getString("emp_dept") + " " + row.getString("emp_first") + " " + row.getString("emp_last") + "\n");
        }
    }
}
