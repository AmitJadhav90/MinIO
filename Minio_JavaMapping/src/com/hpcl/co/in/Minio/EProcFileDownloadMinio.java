package com.hpcl.co.in.Minio;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import com.sap.aii.mapping.api.*;

import io.minio.BucketExistsArgs;
import io.minio.GetObjectArgs;
import io.minio.MinioClient;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.sap.engine.interfaces.messaging.api.auditlog.*;
import com.sap.engine.interfaces.messaging.api.exception.MessagingException;
import com.sap.aii.mapping.api.StreamTransformation;
import com.sap.aii.mapping.api.AbstractTransformation;
import com.sap.engine.interfaces.messaging.api.*;

public class EProcFileDownloadMinio extends AbstractTransformation {

	public static final int DEFAULT_BUFFER_SIZE = 8192; 
	public void transform(TransformationInput transformationInput, TransformationOutput transformationOutput) throws StreamTransformationException {	
	
        MessageKey key = null;
        AuditAccess audit = null;
        final String DASH = "-";  
        String slash = "/";
        String attachments = "";
        String rfq = "";
        
        getTrace().addInfo("Java Mapping Program Started!");
        String msgID = transformationInput.getInputHeader().getMessageId();
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
			
//        	getTrace().addInfo("Java Mapping Program Started!");

        	/******* Reading Dynamic details below *********/
			String url = transformationInput.getInputParameters().getString("url");
			String bucketName = transformationInput.getInputParameters().getString("bucketname");
			String accesskey = transformationInput.getInputParameters().getString("accesskey");
			String secretkey = transformationInput.getInputParameters().getString("secretkey");        	

        	getTrace().addInfo("URL "+url);
        	getTrace().addInfo("AccessKey "+accesskey);
        	getTrace().addInfo("Secretkey "+accesskey);
        	getTrace().addInfo("BucketName "+bucketName);
			
			/******* Reading InputStream and XML *********/
			InputStream instream = transformationInput.getInputPayload().getInputStream();
  		    OutputStream outstream = transformationOutput.getOutputPayload().getOutputStream();
            //an instance of factory that gives a document builder  
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();  
            //an instance of builder to parse the specified xml file  
            DocumentBuilder db = dbf.newDocumentBuilder();  
            Document doc = db.parse(instream);             
            doc.getDocumentElement().normalize(); 
            
            getTrace().addInfo("XML Read Started");
            getTrace().addInfo("Root Element "+doc.getDocumentElement().getNodeName());
            /******* Reading Attachment Names  *********/
            NodeList list = doc.getElementsByTagName("Request");
            for (int i = 0; i < list.getLength(); i++) {
            	
            	Node node = list.item(i);
            	getTrace().addInfo("Item" +list.item(i));
            	  if(node.getNodeType() == Node.ELEMENT_NODE){
            		  Element eElem = (Element) node;
            		  attachments = eElem.getElementsByTagName("ATTACHMENTS").item(0).getTextContent();
            		  getTrace().addInfo("AttachmentName" +attachments );
            
            	  }
            	  
            }
            MinioClient s3Client =
        		    MinioClient.builder()
        			    .endpoint(url)//"https://miniodev.hpcl.co.in/"
        				.credentials(accesskey,secretkey) //"saprfqdevuser" , "R#f1622QPsa"
        				.build();	
            getTrace().addInfo("Minio Bucket Found" +s3Client.bucketExists(BucketExistsArgs.builder().bucket(bucketName).build()));
            String op = "OUTPUT";
            
            OutputStream out = new ByteArrayOutputStream();
            String[] arrOfStr = attachments.split(",");
            
            for(int i =0; i<=arrOfStr.length; i++)
            {
            	String filename = arrOfStr[i];
            	String path = op+slash+filename;
            	getTrace().addInfo("FilePath " +path);
            	audit.addAuditLogEntry(key, AuditLogStatus.SUCCESS, "Attachments "+path);
            	try (InputStream stream = s3Client.getObject(
  					  GetObjectArgs.builder()
  					  .bucket(bucketName)
  					  .object(path)//"OUTPUT/MinIOTest.pdf"
  					  .build())) {
  					  // Read data from stream
            		
            		getTrace().addInfo("Filestream downloaded");
            		int read;
            		byte[] bytes = new byte[DEFAULT_BUFFER_SIZE];
            		
            		while ((read = stream.read(bytes)) != -1) {
            			out.write(bytes, 0, read);
                    }
            		
            		// Write attachment
            		OutputAttachments outputAttachments = transformationOutput.getOutputAttachments();
            		byte[] attachbytes = new byte[stream.available()];
            		audit.addAuditLogEntry(key, AuditLogStatus.SUCCESS, "Bytes "+bytes.toString());
            		stream.read(bytes);
//            		
            		Attachment newAttachment = outputAttachments.create(filename,"application/pdf", bytes);
            		outputAttachments.setAttachment(newAttachment);
            		getTrace().addInfo("File" +filename+ " is attached");
            
            		
  					}
          
            	
            }
         // Copy Input content to Output content
            getTrace().addInfo("Copy InputStream to Output");
            byte[] b = new byte[instream.available()];
            instream.read(b);
            outstream.write(b);
            
        	
		} catch (Exception e) {
			// TODO: handle exception
		}
        
	
	
	}
}
