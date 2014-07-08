package uk.dundee.computing.aec.lib;


import java.util.ArrayList;
import java.util.List;







import com.datastax.driver.core.*;

public final class Keyspaces {



	public Keyspaces(){

	}

	public static void SetUpKeySpaces(Cluster c){
		try{
			//Add some keyspaces here
			String createkeyspace="create keyspace if not exists mzmlKeyspace  WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};";
			String CreateTempTable = "CREATE TABLE if not exists mzmltemp ("+
					"name varchar,"+
					"scan int ,"+
					"msLevel float,"+
					"retTime float,"+
					"precursorIonMZ float,"+
					"precursorIonIntensity float,"+
					"precursorIonCharge float,"+
					"mzArray varchar,"+
					"intensityArray varchar,"+
					" PRIMARY KEY (name,scan)"+
					") WITH CLUSTERING ORDER BY (scan DESC);";
			String CreateTable = "CREATE TABLE if not exists mzml ("+
					"name varchar,"+
					"scan int ,"+
					"msLevel float,"+
					"retTime float,"+
					"precursorIonMZ float,"+
					"precursorIonIntensity float,"+
					"precursorIonCharge float,"+
					"mzArray blob,"+
					"intensityArray blob,"+
					" PRIMARY KEY (name,scan)"+
					") WITH CLUSTERING ORDER BY (scan DESC);";
			Session session = c.connect();
			try{
				PreparedStatement statement = session
						.prepare(createkeyspace);
				BoundStatement boundStatement = new BoundStatement(
						statement);
				ResultSet rs = session
						.execute(boundStatement);

			}catch(Exception et){
				System.out.println("Can't create mzMLKeyspace "+et);
			}

			//now add some column families 
			session.close();
			session = c.connect("mzMLKeyspace");
			System.out.println(""+CreateTempTable);

			try{
				SimpleStatement cqlQuery = new SimpleStatement(CreateTempTable);
				session.execute(cqlQuery);
			}catch(Exception et){
				System.out.println("Can't create tweet table "+et);
			}
			try{
				SimpleStatement cqlQuery = new SimpleStatement(CreateTable);
				session.execute(cqlQuery);
			}catch(Exception et){
				System.out.println("Can't create tweet table "+et);
			}
			session.close();

		}catch(Exception et){
			System.out.println("Other keyspace or coulm definition error" +et);
		}

	}
}
