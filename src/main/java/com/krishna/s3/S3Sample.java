package com.krishna.s3;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.List;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SDKGlobalConfiguration;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.BucketVersioningConfiguration;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.EmailAddressGrantee;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.Grant;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ListVersionsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.Permission;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.S3VersionSummary;
import com.amazonaws.services.s3.model.SetBucketVersioningConfigurationRequest;
import com.amazonaws.services.s3.model.VersionListing;


public class S3Sample {


	public static void main(String[] args) throws IOException {
		
		AmazonS3 s3client = initializeAmazonS3();
		createBucketinS3(s3client,"testingpoc");
		listBucktesinS3(s3client);
		listBucketObjects(s3client,"testingpoc");	
		listBucketObjectsOfSpecificFolder(s3client,"testingpoc","TEST/DEV/");
		listBucketObjectsOfSpecificFolder(s3client,"testingpoc","sharepoc");
		createNewFolderinS3("testingpoc","sharepoc",s3client);
		uploadFileToBucket("testingpoc","sharepoc",s3client);
		readFromS3(s3client,"testingpoc","sharepoc","Test.txt");
		listS3FileVersion(s3client,"testingpoc","sharepoc");
		chekcBucketVestionStatus(s3client);
		setBucketVersioning(s3client,"testingpoc");
		listBucketPermissions(s3client,"testingpoc");
		listPerticularFilePermission(s3client,"testingpoc","sharepoc/Test.txt");
		//setPermissionToS3Object(s3client,"testingpoc","sharepoc/Test.txt");
		 
    }

	
	private static void listPerticularFilePermission(AmazonS3 s3client, String bucketName, String fileName) {
		try {
            AccessControlList acl = s3client.getObjectAcl(bucketName, fileName);
            List<Grant> grants = acl.getGrantsAsList();
            for (Grant grant : grants) {
                System.out.format(" File Permissions:%s  %s: %s\n", fileName ,grant.getGrantee().getIdentifier(),
                        grant.getPermission().toString());
            }
        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
            System.exit(1);
        }
		
	}

	private static void listBucketPermissions(AmazonS3 s3client, String string) {
		
		AccessControlList bucketAcl = s3client.getBucketAcl("sg00cstaccp");
		List<Grant> grantList = bucketAcl.getGrantsAsList();
		grantList.forEach((temp) -> {
			System.out.println("All permissions:"+temp.getPermission());
		});
		
	}

	private static void setBucketVersioning(AmazonS3 s3client,String bucketName) {
		BucketVersioningConfiguration configuration = 
    			new BucketVersioningConfiguration().withStatus("Enabled");
        
		SetBucketVersioningConfigurationRequest setBucketVersioningConfigurationRequest = 
				new SetBucketVersioningConfigurationRequest(bucketName,configuration);
		
		s3client.setBucketVersioningConfiguration(setBucketVersioningConfigurationRequest);
		
		
	}

	private static void chekcBucketVestionStatus(AmazonS3 s3client) {
		BucketVersioningConfiguration conf = s3client.getBucketVersioningConfiguration("sg00cstaccp");
		 System.out.println("bucket versioning configuration status:    " + conf.getStatus());
		
	}

	private static void listS3FileVersion(AmazonS3 s3client, String bucketName, String folderName) {
		// Retrieve the list of versions. If the bucket contains more versions
        // than the specified maximum number of results, Amazon S3 returns
        // one page of results per request.
       try {
    	   ListVersionsRequest request = new ListVersionsRequest()
            .withBucketName(bucketName).withPrefix(folderName)
            .withMaxResults(2);
        
        VersionListing versionListing = s3client.listVersions(request); 
        int numVersions = 0, numPages = 0;
        while(true) {
            numPages++;
            for (S3VersionSummary objectSummary : 
                versionListing.getVersionSummaries()) {
                System.out.printf("Retrieved object %s, version %s\n", 
                                        objectSummary.getKey(), 
                                        objectSummary.getVersionId());
                numVersions++;
            }
            // Check whether there are more pages of versions to retrieve. If
            // there are, retrieve them. Otherwise, exit the loop.
            if(versionListing.isTruncated()) {
                versionListing = s3client.listNextBatchOfVersions(versionListing);
            }
            else {
                break;
            }
        }
        System.out.println(numVersions + " object versions retrieved in " + numPages + " pages");
     } 
    catch(AmazonServiceException e) {
        // The call was transmitted successfully, but Amazon S3 couldn't process 
        // it, so it returned an error response.
        e.printStackTrace();
    }
    catch(SdkClientException e) {
        // Amazon S3 couldn't be contacted for a response, or the client
        // couldn't parse the response from Amazon S3.
        e.printStackTrace();
    }
}
		


	public static void readFromS3(AmazonS3 s3client, String bucketName, String folderName, String fileName) throws IOException {
	   

		S3Object s3object = s3client.getObject(new GetObjectRequest(
	            bucketName, folderName+"/"+fileName));
	    System.out.println("Printing The Content Type:"+s3object.getObjectMetadata().getContentType());
	    System.out.println("Printing the Content Length:"+s3object.getObjectMetadata().getContentLength());

	    BufferedReader reader = new BufferedReader(new InputStreamReader(s3object.getObjectContent()));
	    String line;
	    while((line = reader.readLine()) != null) {
	      // can copy the content locally as well
	      // using a buffered writer
	      System.out.println("Lines Inside The File:"+line);
	    }
	    
	    System.out.println("File Content Reading Done..");
	  }
	
	
	
	/**
	 * @param bucketName
	 * @param folderName
	 * @param s3client, When Uploading the File can do the Encryption, 1. Client Side Encryption 2. Server Side Encryption
	 * when uploading the File can set the file permissions also 1.uploading the policy 2.set Through CannedAccessControlList class
	 */
	public static void uploadFileToBucket(String bucketName, String folderName,AmazonS3 s3client) {

		String fileUrl = "";
		try {
			
			File inFile = new File("C:/Users/krishnareddyk/Desktop/R/R-LinearRegression.txt");
		    final String fileName = "sometext.txt";
			//File file = new File(S3Sample.class.getResource(fileName).toURI());
//			String folderNameInS3 = Credentials.prjctSubTempFolder + Credentials.SLASH + folderName;
		    String folderNameInS3 = folderName;
			
			PutObjectRequest putRequest1 = new PutObjectRequest(bucketName, folderNameInS3+"/"+"Test.txt", inFile).withCannedAcl(CannedAccessControlList.PublicRead);
			ObjectMetadata objectMetadata1 = new ObjectMetadata();
            objectMetadata1.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
            putRequest1.setMetadata(objectMetadata1);
            PutObjectResult response1 = s3client.putObject(putRequest1);
            System.out.println("Uploaded object encryption status is " +
                    response1.getSSEAlgorithm());
            
			fileUrl = Credentials.endPointUrl + "/" + bucketName + "/" + folderNameInS3 + "/"+"Test.txt";
			System.out.println("File Uploaded to S3 To This Path:"+fileUrl);
		}

		catch (Exception e) {
			System.out.println("Error While Uploading file to S3:"+e.getMessage());
		}
		
	}
	
	private static void createNewFolderinS3(String bucketName, String folderName, AmazonS3 client) {
		 // create meta-data for your folder and set content-length to 0
		ObjectMetadata metadata = new ObjectMetadata();
	    metadata.setContentLength(0);
	 // create empty content
	    InputStream emptyContent = new ByteArrayInputStream(new byte[0]);
	    // create a PutObjectRequest passing the folder name suffixed by /
	    
	    PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName,folderName , emptyContent, metadata);

	    // send request to S3 to create folder
	    client.putObject(putObjectRequest);
	    System.out.println("Creating the New MetaData Folder:"+folderName);
	    
	}


	/**
	 * @param s3client
	 * @param bucketName
	 * @param folderName
	 *  This method is to list the objects under specific folder of bucket, but in output objects can find the folder name also,
	 *  like ACCEP_LOA/DEV/abc.txt
	 */
	private static void listBucketObjectsOfSpecificFolder(AmazonS3 s3client, String bucketName, String folderName) {
		
		ListObjectsRequest lor = new ListObjectsRequest().withBucketName(bucketName).withPrefix(folderName);
		ObjectListing objectListing = s3client.listObjects(lor);
		for (S3ObjectSummary summary: objectListing.getObjectSummaries()) {
			System.out.println("listBucketObjectsOfSpecificFolder:"+summary.getKey()+"Storage Class:"+summary.getStorageClass());
		}
	}


	private static void listBucketObjects(AmazonS3 s3client, String buketName) {
		ObjectListing objectListing = s3client.listObjects(buketName);
		List<S3ObjectSummary> objectList = objectListing.getObjectSummaries();
	
		while (true) {
             for ( Iterator<?> iterator = objectListing.getObjectSummaries().iterator(); iterator.hasNext(); ) {
                 S3ObjectSummary objectSummary = (S3ObjectSummary) iterator.next();
                 System.out.println("Objects Summary:"+objectSummary.toString());
             }
		 }
	}


	/**
	 * @param s3client
	 * @param bucketName should not contain any uppercase characters
	 */
	private static void createBucketinS3(AmazonS3 s3client,String bucketName) {
		if (!(s3client.doesBucketExistV2(bucketName))) {
			s3client.createBucket(bucketName);
			s3client.setRegion(Region.getRegion(Regions.US_EAST_1));
			System.out.println("Bucket Got Created:"+bucketName);
		}else {
			System.out.println("Bucket Got Created Already.."+bucketName);
		}
		
	}


	public static void listBucktesinS3(AmazonS3 s3client) {

		  for (Bucket bucket:s3client.listBuckets()){
	            System.out.println("Bucket Names:"+bucket.getName()+"      , "+bucket.getOwner()+"    ,"+bucket.getCreationDate()+"   ,"+bucket.getClass());
	        }
	}


	public static AmazonS3 initializeAmazonS3() {
		System.setProperty(SDKGlobalConfiguration.DISABLE_CERT_CHECKING_SYSTEM_PROPERTY, "true");
		BasicAWSCredentials credentials = new BasicAWSCredentials(Credentials.access_key_id, Credentials.secret_access_key);
		AwsClientBuilder.EndpointConfiguration symphonyEndpoint = new AwsClientBuilder.EndpointConfiguration(
				Credentials.endPointUrl, "");
		AmazonS3 s3client = AmazonS3ClientBuilder.standard().withEndpointConfiguration(symphonyEndpoint)
				.withCredentials(new AWSStaticCredentialsProvider(credentials)).withPathStyleAccessEnabled(true)
				.build();
		System.out.println("S3 Configuration Done..");
		return s3client;
	}
}