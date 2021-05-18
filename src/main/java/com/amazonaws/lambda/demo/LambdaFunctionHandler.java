package com.amazonaws.lambda.demo;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent.DynamodbStreamRecord;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.util.StringUtils;

public class LambdaFunctionHandler implements RequestHandler<DynamodbEvent, Integer> {

	private static final String FILE_NAME = "filename";
	private static final String MIME_TYPE = "mime-type";
	private static final String FULLTEXT_REF = "fulltext-ref";
	private static final String DYNAMO_DB_RECORD_ID = "dynamodb-record-id";
	private static final String BUCKET_NAME = "mybucket-05-2021";
	private static final String FULLTEXT_COLLECTION = "FullTextCollection";
	// private static final String META_COLLECTION="MetaCollection";

	@Override
	public Integer handleRequest(DynamodbEvent event, Context context) {
		context.getLogger().log("Received event: " + event);
		for (DynamodbStreamRecord record : event.getRecords()) {
			context.getLogger().log(record.getEventID());
			context.getLogger().log(record.getEventName());
			context.getLogger().log(record.getDynamodb().toString());
			String fileName = record.getDynamodb().getNewImage().get(FILE_NAME).getS();
			String mimeType = record.getDynamodb().getNewImage().get(MIME_TYPE).getS();
			String fullTextRef = record.getDynamodb().getNewImage().get(FULLTEXT_REF).getS();
			uploadContentToS3(fileName, mimeType, fullTextRef, context);
			context.getLogger()
					.log("filename ::" + fileName + "mimeType ::" + mimeType + " fullTextRef :" + fullTextRef);
		}
		return event.getRecords().size();
	}

	private void uploadContentToS3(String fileName, String mimeType, String fullTextRef, Context context) {
		Regions clientRegion = Regions.US_EAST_1;
		String fileObjKeyName = getFilePath(fileName);
		try {
			String content = getItemFromDynamoDB(fullTextRef, context);
			AmazonS3 s3Client = AmazonS3ClientBuilder.standard().withRegion(clientRegion).build();
			// Upload a file as a new object with ContentType and title specified.
			String fileArray[] = fileName.split("\\.");
			File file = File.createTempFile(fileArray[0], fileArray[1]);
			if (!StringUtils.isNullOrEmpty(content)) {
				Files.write(Paths.get(file.getPath()), content.getBytes(), StandardOpenOption.CREATE);
			}
			PutObjectRequest request = new PutObjectRequest(BUCKET_NAME, fileObjKeyName, file);
			ObjectMetadata metadata = new ObjectMetadata();
			metadata.setContentType(mimeType);
			metadata.addUserMetadata(DYNAMO_DB_RECORD_ID, fullTextRef);
			request.setMetadata(metadata);
			s3Client.putObject(request);
		} catch (AmazonServiceException e) {
			context.getLogger().log("AWS Exception ::" + e.getStackTrace()[0] + ":" + e.getMessage());
		} catch (SdkClientException e) {
			context.getLogger().log("AWS Exception ::" + e.getStackTrace()[0] + ":" + e.getMessage());
		} catch (IOException e) {
			context.getLogger().log("IO Exception ::" + e.getStackTrace()[0] + ":" + e.getMessage());
		}
	}

	private String getItemFromDynamoDB(String fullTextRef, Context context) {
		AmazonDynamoDB client = AmazonDynamoDBClientBuilder.defaultClient();
		DynamoDB dynamoDB = new DynamoDB(client);
		Table table = dynamoDB.getTable(FULLTEXT_COLLECTION);
		Item item = table.getItem("id", fullTextRef);
		if (item != null) {
			context.getLogger().log("Item Info ::" + item.toJSONPretty());
			return item.getString("ftValue");
		}
		return null;
	}

	private String getFilePath(String fileName) {
		if (StringUtils.isNullOrEmpty(fileName)) {
			return "other/" + fileName;
		}
		if (fileName.contains("html")) {
			return "html/" + fileName;
		}
		if (fileName.contains("xml")) {
			return "xml/" + fileName;
		}
		return "other/" + fileName;
	}
}
