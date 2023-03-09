package com.hpcl.co.in.Minio;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.sap.aii.mapping.api.*;

import io.minio.BucketExistsArgs;
import io.minio.GetPresignedObjectUrlArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import io.minio.http.Method;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.sap.engine.interfaces.messaging.api.auditlog.*;
import com.sap.engine.interfaces.messaging.api.exception.MessagingException;
import com.sap.engine.interfaces.messaging.api.*;



public class FileUploadMinio extends AbstractTransformation{
	@Override
    public void transform(TransformationInput transformationInput, TransformationOutput transformationOutput) throws StreamTransformationException {	
	
		
        MessageKey key = null;
        AuditAccess audit = null;
        final String DASH = "-";   
        
        String msgID=transformationInput.getInputHeader().getMessageId();
        String uuidTimeLow = msgID.substring(0, 8);  
        String uuidTimeMid = msgID.substring(8, 12);  
        String uuidTimeHighAndVersion = msgID.substring(12, 16);  
        String uuidClockSeqAndReserved = msgID.substring(16, 18);  
        String uuidClockSeqLow = msgID.substring(18, 20);  
        String uuidNode = msgID.substring(20, 32);  
        String msgUUID =   uuidTimeLow + DASH + uuidTimeMid + DASH + uuidTimeHighAndVersion + DASH  + uuidClockSeqAndReserved + uuidClockSeqLow + DASH + uuidNode;
        try {
			audit = PublicAPIAccessFactory.getPublicAPIAccess().getAuditAccess();
		} catch (MessagingException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

        key = new MessageKey(msgUUID, MessageDirection.OUTBOUND);	
		
		try {
			
			getTrace().addInfo("Java Mapping Program Started!");
			String rfqNumber = "";
            String strFileName = "";
            String strDirectory = "";
            String dir = "";
            String slash = "/";
            String colon = ":";
            
            
          //Get the handler for processing the attachments of source message
            InputAttachments attachment = transformationInput.getInputAttachments();
			String url = transformationInput.getInputParameters().getString("url");
			String bucketName = transformationInput.getInputParameters().getString("bucketname");
			String accesskey = transformationInput.getInputParameters().getString("accesskey");
			String secretkey = transformationInput.getInputParameters().getString("secretkey");
			String auth = "?accessKey="+accesskey+"&secretKey="+secretkey;

			InputStream inputstream = transformationInput.getInputPayload().getInputStream();
            OutputStream outputstream = transformationOutput.getOutputPayload().getOutputStream();
            
            //an instance of factory that gives a document builder  
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();  
            //an instance of builder to parse the specified xml file  
            DocumentBuilder db = dbf.newDocumentBuilder();  
            Document doc = db.parse(inputstream);             
            doc.getDocumentElement().normalize(); 
            getTrace().addInfo("XML Read Started");
            NodeList list = doc.getElementsByTagName("n0:MT_Minio_Req");
            
            for (int temp = 0; temp < list.getLength(); temp++) {
            	
            	Node node = list.item(temp);
            	
            	if (node.getNodeType() == Node.ELEMENT_NODE) {
            		
            		Element element = (Element) node;
            		// get value for RFQNumber
            		rfqNumber = element.getElementsByTagName("RFQ").item(0).getTextContent();
            		getTrace().addInfo("RFQ Number READ" +rfqNumber );
            		// get value for Filename
            		strFileName = element.getElementsByTagName("Filename").item(0).getTextContent();
            		getTrace().addInfo("File Name READ" +strFileName );
            		
            	}
            	
            }
            String ip = "INPUT";
            String path = rfqNumber+slash+ip+slash+strFileName;

          //Fetch the collection of attachments in the source message
            for (String id : attachment.getAllContentIds(false)) {
            //Read the content of the attachment and assign the content to output
            	outputstream.write((attachment.getAttachment(id)).getContent());
            	
        }  
            //Code to convert OutputStream to InputStream
            ByteArrayOutputStream bos = (ByteArrayOutputStream)outputstream;
            ByteArrayInputStream inStream = new ByteArrayInputStream( bos.toByteArray());
            byte[] bytes = inStream.toString().getBytes(StandardCharsets.UTF_8); 
            
            InputStream input = new ByteArrayInputStream(bytes);
            
            audit.addAuditLogEntry(key, AuditLogStatus.SUCCESS, "AccessKey "+accesskey+" SecretKey "+secretkey);
            
            MinioClient s3Client =
				    MinioClient.builder()
					    .endpoint(url)
						.credentials(accesskey, secretkey)
						.build();    
           
            boolean found =
  	    		  s3Client.bucketExists(BucketExistsArgs.builder().bucket(bucketName).build());
            
            
            if(found){
            	audit.addAuditLogEntry(key, AuditLogStatus.SUCCESS, "Bucket Found Successfully!!");
            	getTrace().addDebugMessage("Bucket " +bucketName+ " found Successfully");            	
            	s3Client.putObject(
            		    PutObjectArgs.builder().bucket(bucketName).object(path).stream(
            		    		input, -1, 10485760)
            		        .contentType("application/pdf")
            		        .build());
            	audit.addAuditLogEntry(key, AuditLogStatus.SUCCESS, "200 - File Uploaded Successfully!!");
            	
            	//GET Created File URL
            	Map<String, String> reqParams = new HashMap<String, String>();
                reqParams.put("response-content-type", "application/json");
                
                audit.addAuditLogEntry(key, AuditLogStatus.SUCCESS, "Path "+path);
                
                String urlString = s3Client.getPresignedObjectUrl(GetPresignedObjectUrlArgs.builder()
                        .method(Method.GET)
                        .bucket("saprfqdev")
                        .object(path)
                        .expiry(1, TimeUnit.HOURS)
                        .extraQueryParams(reqParams)
                        .build());
            	
                audit.addAuditLogEntry(key, AuditLogStatus.SUCCESS, "FileURL "+urlString);
                urlString = urlString.replace("%2F","/");
            	URL	endpointUrl = new URL(urlString);
                // Create Dynamic Configuration Object
                DynamicConfiguration conf = transformationInput.getDynamicConfiguration();
                String urlvar = endpointUrl.toString();
                urlvar = urlvar.replace("%2F","/");
                DynamicConfigurationKey key1 = DynamicConfigurationKey.create( "http:/"+"/sap.com/xi/XI/System/REST","endpoint"); 
                conf.put(key1,urlvar);
            	
            	
            }else {
            	
            	getTrace().addDebugMessage("Bucket " +bucketName+ " not found");
            	
            }
			
			
		}catch(Exception e) {
			getTrace().addDebugMessage(e.getMessage());
			audit.addAuditLogEntry(key, AuditLogStatus.ERROR, "Exception Raised: "+e.toString());
            throw new StreamTransformationException(e.toString());
		}
		
	}
	
	

}
