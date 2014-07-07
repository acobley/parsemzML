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

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class MzMLSpout extends BaseRichSpout {
	Cluster cluster;
    Session session;
    
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
		String outputFile;
		String outFile;
		private StringBuffer contentBuffer = new StringBuffer();
		
		
		BufferedWriter writer = null;{
			try
			{
				writer = new BufferedWriter( new FileWriter( outputFile ));			
			} catch ( IOException e){
				}
		}
		
		public HandlemzML(String inputFile,Cluster cluster,Session session) {		
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
					writer.write(outFile+"\t"+scan+"\t"+msLevel+"\t"+retTime+"\t"+precursorIonMZ+"\t"+precursorIonIntensity+"\t"+precursorIonCharge+"\t"+mzArray+"\t"+intensityArray);
					writer.newLine();
				} catch (IOException e) {
					e.printStackTrace();
				}
	    	
	    	}
	    	    	
	    }
	    @Override
	    public void characters(char[] ac, int i, int j) throws SAXException {
	        //tmpValue = new String(ac, i, j);
	        contentBuffer.append(ac, i, j); 
	    }

	}
	
	  SpoutOutputCollector _collector;
	    

	  @Override
	  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
	    _collector = collector;
       	//cluster = Cluster.builder().addContactPoint("192.168.2.10").build(); //vagrant cassandra cluster
    	cluster = Cluster.builder().addContactPoint("127.0.0.1").build(); //vagrant cassandra cluster
   	     

    	 session = cluster.connect();
    	 HandlemzML mxML=new HandlemzML("",cluster,session);

	  }

	  @Override
	  public void nextTuple() {
	    Utils.sleep(100);
	    String[] sentences = new String[]{ "A","B","C","D" };
	    String sentence="";
		Date d= new Date();
	    _collector.emit(new Values(sentence,d.toString()));
	  }

	  @Override
	  public void ack(Object id) {
	  }

	  @Override
	  public void fail(Object id) {
	  }

	  @Override
	  public void declareOutputFields(OutputFieldsDeclarer declarer) {
	    declarer.declare(new Fields("word","date"));
	  }

	}