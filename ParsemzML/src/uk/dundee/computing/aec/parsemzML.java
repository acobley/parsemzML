package uk.dundee.computing.aec;

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

public class parsemzML extends DefaultHandler {
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
	static String outputFile;
	static String outFile;
	private StringBuffer contentBuffer = new StringBuffer();
	
	static BufferedWriter writer = null;{
		try
		{
			writer = new BufferedWriter( new FileWriter( outputFile ));			
		} catch ( IOException e){
			}
	}
	
	public parsemzML(String inputFile) {		
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
    
    public static void main(String[] args) {
    	outputFile =  args[1];
    	outFile = new File(outputFile).getName();
        new ParsemzML(args[0]);
        
        try {
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
    }
}
