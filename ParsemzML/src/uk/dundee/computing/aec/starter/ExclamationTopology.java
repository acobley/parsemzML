package uk.dundee.computing.aec.starter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import uk.dundee.computing.aec.lib.Keyspaces;
import uk.dundee.computing.aec.spout.MzMLSpout;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.Map;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.NoHostAvailableException;

import org.apache.commons.codec.binary.Base64;

/**
 * This is a basic example of a Storm topology.
 */
public class ExclamationTopology {

	public static class ConvertToBlobBolt extends BaseRichBolt {
		OutputCollector _collector;
		String ComponentId;
		@Override
		public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
			_collector = collector;
			ComponentId=context.getThisComponentId();
		}

		@Override
		public void execute(Tuple tuple) {
			Date d=new Date();
			Base64 bs= new Base64();
			byte[] mzdata=bs.decode(tuple.getStringByField("mzArray")) ;
			byte[] intensitydata=bs.decode(tuple.getStringByField("intensityArray")) ;
			String name =tuple.getStringByField("name");
			int count= tuple.getIntegerByField("scan");
			_collector.emit(tuple, new Values(name,count,mzdata,intensitydata,d.toString()));
			_collector.ack(tuple);
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("name","scan","mzArray","intensityArray","date"));
		}
	}
	public static class SaverBolt extends BaseRichBolt {
		OutputCollector _collector;
		String ComponentId;
		public static java.util.UUID getTimeUUID()
		{
			return java.util.UUID.fromString(new com.eaio.uuid.UUID().toString());
		}

		Cluster cluster;
		Session session;
		PreparedStatement InsertStatement;


		@Override
		public void prepare(Map conf, TopologyContext context, OutputCollector collector) {

			//cluster = Cluster.builder().addContactPoint("192.168.2.10").build(); //vagrant cassandra cluster
			try {
				cluster = Cluster.builder().addContactPoint("192.168.2.10").build(); //vagrant cassandra cluster
				session = cluster.connect(); 
			}catch(NoHostAvailableException et){
				try{
					cluster = Cluster.builder().addContactPoint("127.0.0.1").build(); //localhost
					session = cluster.connect();
				}catch(NoHostAvailableException et1){
					//can't get to a cassandra cluster bug out
					return;

				}
			} 
			Keyspaces kp = new Keyspaces();
			kp.SetUpKeySpaces(cluster);
			InsertStatement = session.prepare("insert into mzMLKeyspace.mzml (name,scan,mzarray,intensityarray)" +
					" VALUES (?, ?, ?,?);");
			_collector = collector;
			ComponentId=context.getThisComponentId();
		}

		@Override
		public void execute(Tuple tuple) {

			byte[] mzdata=tuple.getBinaryByField("mzArray");
			byte[] intensitydata=tuple.getBinaryByField("intensityArray") ;
			String name =tuple.getStringByField("name");
			int count= tuple.getIntegerByField("scan");

			ByteBuffer mzbf = ByteBuffer.wrap(mzdata);
			ByteBuffer intensitybf = ByteBuffer.wrap(intensitydata);
			//String Value =tuple.getString(0) ;
			String d=tuple.getStringByField("date");
			if (d==null)
				d="no time";

			BoundStatement boundStatement = new BoundStatement(InsertStatement);
			session.execute(boundStatement.bind(name,count,mzbf,intensitybf));

			_collector.ack(tuple);
		}
		@Override
		public void cleanup(){

			//cluster.shutdown();
			session.close();
			cluster.close();
		}
		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("word"));
		}


	}

	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("mzML", new MzMLSpout(), 1);
		builder.setBolt("blob", new ConvertToBlobBolt(), 3).shuffleGrouping("mzML");
		builder.setBolt("Saver", new SaverBolt(), 4).shuffleGrouping("blob");
		Config conf = new Config();

		conf.setDebug(true);

		if (args != null && args.length > 0) {
			conf.setNumWorkers(3);

			StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
		}
		else {

			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("test", conf, builder.createTopology());
			Utils.sleep(50000);
			cluster.killTopology("test");
			cluster.shutdown();

		}
	}

}
