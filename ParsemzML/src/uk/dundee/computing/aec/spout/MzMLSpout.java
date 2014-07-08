package uk.dundee.computing.aec.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Date;
import java.util.Map;
import java.util.Random;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import uk.dundee.computing.aec.lib.Keyspaces;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.NoHostAvailableException;

public class MzMLSpout extends BaseRichSpout {
	Cluster cluster;
    Session session;
    String xmlFile="561L1AIL00.mzML";
    PreparedStatement SelectStatement=null;
    int Count=0;
    HandlemzML mzML=null;
	class HandlemzML extends DefaultHandler{
		String tmpValue;
	    String scan;
	    String msLevel;
	    String retTime;
	    String mzArray;
	    String intensityArray;
	    String precursorIonMZ;
	    String precursorIonCharge;
	    String precursorIonIntensity;
		int newFlag=0;
		
		String inFile;
		int MaxCount=0;
		
		private StringBuffer contentBuffer = new StringBuffer();
		PreparedStatement statement=null;
		
		
		
		public HandlemzML(String inputFile,Cluster cluster,Session session,PreparedStatement statement) {	
			System.out.println("Parsing File");
			inFile=inputFile;
			this.statement=statement;
	        parseDocument(inputFile);
	    }
	 
		private void parseDocument(String inputFile) {
	        SAXParserFactory factory = SAXParserFactory.newInstance();
	        try {
	            SAXParser parser = factory.newSAXParser();
	            parser.parse(inputFile, this);

	        } catch (ParserConfigurationException e) {
	            System.out.println("ParserConfig error");
	        } catch (SAXException e) {
	            System.out.println("SAXException : xml not well formed");
	        } catch (IOException e) {
	            System.out.println("IO error");
	        }
	    }
	    
	    @Override
	    public void startElement(String s, String s1, String elementName, Attributes attributes) throws SAXException {

	    	if (elementName.equalsIgnoreCase("spectrum")) {
				newFlag = 1;
				scan = attributes.getValue("index");
			    msLevel = "";
			    retTime = "";
			    mzArray = "";
			    intensityArray = "";
			    precursorIonMZ = "";
			    precursorIonCharge = "";
			    precursorIonIntensity = "";		    
			}
	    	contentBuffer.setLength(0);
	    	
	    	if (elementName.equalsIgnoreCase("cvParam")) {
	    		String testName = attributes.getValue("name");
	    		String testValue = attributes.getValue("value");
	    	
	    		if (testName.equals("ms level")){
	    			msLevel = testValue;
	    			if (msLevel.equals("1")){    				
						precursorIonMZ = "0";
						precursorIonIntensity = "0";
						precursorIonCharge = "0";    				
	    			}
	    		}
	    		if (testName.equals("scan start time")){
	    			retTime = testValue;
	    		}    		
				if (testName.equals("selected ion m/z")){
	    			precursorIonMZ = testValue;
	    		} 
	    		if (testName.equals("peak intensity")){
	    			precursorIonIntensity = testValue;
	    		} 
	    		if (testName.equals("charge state")){
	    			precursorIonCharge = testValue;
	    		} 			

			}        
	    	
	    }
	    @Override
	    public void endElement(String s, String s1, String element) throws SAXException {
	    	
	    	if (element.equalsIgnoreCase("binary")) {
	    		if (mzArray.equals("")){
	    			mzArray = contentBuffer.toString();
	    		} else {
	    			intensityArray = contentBuffer.toString();
	    		}
	    	}   
	    	
	    	if (element.equalsIgnoreCase("spectrum")) {    		        	 		    	            	
	    		try {   
	    			
	    			BoundStatement boundStatement = new BoundStatement(statement);
	    			int iScan=0;
	    			float fmsLevel=0;
	    			float fretTime=0;

	    			float fprecursorIonMZ=0;
	    			float fprecursorIonCharge=0;
	    			float fprecursorIonIntensity=0;
	    			try{
	    			   iScan=Integer.parseInt(scan);
	    			   fmsLevel=Float.parseFloat(msLevel);
		    		   fretTime=Float.parseFloat(retTime);
                       fprecursorIonMZ=Float.parseFloat(precursorIonMZ);
		    		   fprecursorIonCharge=Float.parseFloat(precursorIonCharge);
		    		   fprecursorIonIntensity=Float.parseFloat(precursorIonIntensity);
		    		   
	    			}catch(Exception et){
	    				System.out.println("Can't convert scan int" +et);
	    			}
	    		    session.execute(boundStatement.bind(mzArray,iScan,inFile,fmsLevel ,fretTime,fprecursorIonMZ,fprecursorIonIntensity ,fprecursorIonCharge,intensityArray ));
					MaxCount++;
				} catch (Exception e) {
					e.printStackTrace();
				}
	    	
	    	}
	    	    	
	    }
	    @Override
	    public void characters(char[] ac, int i, int j) throws SAXException {
	        //tmpValue = new String(ac, i, j);
	        contentBuffer.append(ac, i, j); 
	    }
	    public int getMax(){
	    	return MaxCount;
	    }

	}
	
	  SpoutOutputCollector _collector;
	    

	  @Override
	  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
	    _collector = collector;
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

    	 
    	 PreparedStatement insertStatement = session.prepare("insert into mzMLKeyspace.mzMLTemp"+
                 "(mzArray,"+
                 "scan,"+
                 "name,"+
                 "msLevel,"+
                 "retTime,"+
                 "precursorIonMZ,"+
                 "precursorIonIntensity,"+
                 "precursorIonCharge,"+           
                 "intensityArray"+
                 ") VALUES (?, ?, ?,?,?,?,?,?,?) USING TTL 600;");
    	 SelectStatement=session.prepare("select * from mzMLKeyspace.mzMLTemp where name= ? and scan=?;");
    	 Keyspaces kp = new Keyspaces();
    	 kp.SetUpKeySpaces(cluster);
    	 try{
    	  mzML=new HandlemzML(xmlFile,cluster,session,insertStatement);
    	 }catch(Exception et){
    		 System.out.println("Can't load mzML parser");
    	 }
	  }

	  @Override
	  public void nextTuple() {
		  Utils.sleep(100);
		  ResultSet rs=null;
		  if (mzML.getMax()>Count){

			  BoundStatement boundStatement = new BoundStatement(SelectStatement);
			  rs=session.execute(boundStatement.bind(xmlFile,Count));
			  Count++;

			  Date d= new Date();
			  String mzArray="";
			  String intensityArray="";
			  String name="";
			  int count=0;
			  if (!rs.isExhausted()){
				  Row rr=rs.one();
				  mzArray=rr.getString("mzArray");
				  intensityArray=rr.getString("intensityArray");
				  name=rr.getString("name");
				  count=rr.getInt("scan");
			  }


			  _collector.emit(new Values(name,count,mzArray,intensityArray,d.toString()));
		  }
	  }

	  @Override
	  public void ack(Object id) {
	  }

	  @Override
	  public void fail(Object id) {
	  }

	  @Override
	  public void declareOutputFields(OutputFieldsDeclarer declarer) {
	    declarer.declare(new Fields("name","scan","mzArray","intensityArray","date"));
	  }
	  
	  @Override
	  public void close(){
		  session.close();
		  cluster.close();
	  }

	}