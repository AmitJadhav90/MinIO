/*
* File created from Enterprise Services Repository
 * Wed, 15 Mar 2023 12:19:35 IST
 * User: d_amits
 */
package com.sap.xi.tf;

// beginning of import 93473a7344419b15c4219cc2b6c64c6f
import com.sap.aii.mapping.api.*;
import com.sap.aii.mapping.lookup.*;
import com.sap.aii.mappingtool.tf7.rt.*;
import java.io.*;
import java.lang.reflect.*;
import java.util.*;
import io.minio.*;
import java.io.InputStream;
import java.nio.file.Files;
import org.apache.commons.io.IOUtils;
// end of import 93473a7344419b15c4219cc2b6c64c6f

/**
 * Class encapsulates the user-defined functions of the message mapping MM_RFQExitRequest_To_SAPExitRequest 
 * from namespace urn:hpcl.in:india:Transaction:MM:RFQ:ExitInterface in software 
 * component version HPCLERP_MM, 1.0 of hpcl.in
 * <p>
 * Enter the source text between '// beginning of [function or section name] [GUID]' 
 * and '// end of [function or section name] [GUID]' (example: '// beginning of cleanUp 
 * 7e26d344ee3a9ad7648ebff5b3eb58' and '// end of cleanUp 7e26d344ee3a9ad7648ebff5b3eb58'); 
 * only this section will be included in the import
 * </p>
 * <p>
 * The Java documentation for the classes GlobalContainer, Container, ResultList 
 * are located in SAP NetWeaver Developer Studio under menu option Help > Help Contents 
 * > SAP NetWeaver CE Developer Studio Documentation >API Reference > SAP NetWeaver 
 * 7.10 for Process Integration > com.sap.aii.mappingtool.tf7.rt
 * </p>
 * <p>
 * If you use other Java archives in addition to the exported Java archives, you 
 * must import them to the Enterprise Services Repository as imported archives and 
 * enter them in the function library editor as used archives. Create the imported 
 * archives in the software component version of the function library or in an underlying 
 * software component version. The JARs of the logging API, the JCo JAR, and the 
 * SAP XML toolkit JAR are exceptions: they do not need to be imported to the Enterprise 
 * Services Repository.
 * </p>
 * <p>
 * Java mapping programs (user-defined functions and imported archives) must not 
 * be stateful. For example, during a Java mapping, they cannot write data to a database 
 * table. The Integration Server cannot follow such side effects, and if an attempt 
 * is made to resend a message not yet accepted by the receiver, it is possible that 
 * write-to database accesses during a Java mapping may be executed more than once.
 * </p>
 * <p>
 * The Java mapping programs must be J2EE-compatible; in particular, the same restrictions 
 * apply as for Enterprise Java Beans (see paragraph 25.1.2 of the EJB 2.1 specification). 
 * For example, you cannot do the following: load a JDBC driver and use it directly 
 * (instead use the lookup API), use a classloader, create or use threads, use network 
 * sockets, or read or write files to or from the file system.
 * </p>
 * <p>
 * Source Text Must Be Compatible with Java Version 1.8
 * </p>
 * <p>
 * File is coded in UTF-8. File can only be correctly imported by using UTF-8 coding
 */
public class _MM_RFQExitRequest_To_SAPExitRequest_{
// beginning of attributes and methods 72418d956989a1e71aecbea9d5a90ecf

// end of attributes and methods 72418d956989a1e71aecbea9d5a90ecf



  public void init(GlobalContainer container) throws StreamTransformationException{
  // beginning of init f2bfdf97b7d432d057584464aabdb643

  // end of init f2bfdf97b7d432d057584464aabdb643
  }


  public String AttachFileFromMinIO(String bucketName, String accesskey, String secretkey, String url, String attachments, String tenderno, Container container) throws StreamTransformationException{
  // beginning of AttachFileFromMinIO cb0344d7584a586b5e036448748150e5
String[] arrOfStr = attachments.split(";");
String slash = "/";
String ip = "INPUT";
int DEFAULT_BUFFER_SIZE = 8192; 

try{

getTrace().addInfo("-------------AttachFileFromMinIO Started-----------------");

            		// Write attachment
             	GlobalContainer globalContainer = container.getGlobalContainer();
               OutputAttachments outputAttachments = globalContainer.getOutputAttachments();

MinioClient s3Client =
        		    MinioClient.builder()
        			    .endpoint(url)//"https://miniodev.hpcl.co.in/"
        				.credentials(accesskey,secretkey) //"saprfqdevuser" , "R#f1622QPsa"
        				.build();

OutputStream out = new ByteArrayOutputStream();
//getTrace().addInfo("Minio Bucket Found " +s3Client.bucketExists(BucketExistsArgs.builder().bucket(bucketName).build()));
for(int i =0; i<=arrOfStr.length; i++)
{
getTrace().addInfo("arrOfStr " +arrOfStr[i]);

String filename = arrOfStr[i];
String path = tenderno+slash+ip+slash+filename;    //"5100000477/INPUT/5100000477_1_Revised SHE.PDF";  //op+slash+filename;
getTrace().addInfo("Filename " +filename);

      try ( InputStream istream = s3Client.getObject(
  					  GetObjectArgs.builder()
  					  .bucket(bucketName)
  					  .object(path)//"OUTPUT/MinIOTest.pdf"
  					  .build())) {
  					  // Read data from stream
            		
            		getTrace().addInfo("Filestream downloaded");
            		
//            	  byte[] attachbytes = new byte[DEFAULT_BUFFER_SIZE];

                byte[] bytes = IOUtils.toByteArray(istream);
        
//            	  istream.read(attachbytes);
//               getTrace().addInfo("InputStream" +istream.toString());
//               getTrace().addInfo("streambytes" +attachbytes.toString());
//								getTrace().addInfo("streambytes" +attachbytes.length);
//               OutputAttachments outputAttachments = transformationOutput.getOutputAttachments();
            		Attachment newAttachment = outputAttachments.create(filename,"application/pdf", bytes);
            		outputAttachments.setAttachment(newAttachment);
            		getTrace().addInfo("File" +filename+ " is attached");
  					}
}
}catch(Exception e){
  
       return e.toString();
}

return attachments;
  // end of AttachFileFromMinIO cb0344d7584a586b5e036448748150e5
  }


  public void cleanUp(GlobalContainer container) throws StreamTransformationException{
  // beginning of cleanUp 7e26d344ee3a9ad7648ebff5b3eb584b
  
  // end of cleanUp 7e26d344ee3a9ad7648ebff5b3eb584b
  }
}