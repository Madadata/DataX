package com.alibaba.datax.plugin.writer.s3writer;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerConfiguration;
import com.amazonaws.services.s3.transfer.TransferProgress;
import com.amazonaws.services.s3.transfer.Upload;
import java.io.File;
import java.io.IOException;
import java.util.List;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;

public class S3Util {

    private final String bucket;
    private final String accessKey;
    private final String secretKey;
    private final String endpoint;
    private final AmazonS3 s3Client;

    public S3Util(String bucket, String accessKey, String secretKey, String endpoint) {
        this.bucket = bucket;
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.endpoint = endpoint;

        this.s3Client = new AmazonS3Client(new BasicAWSCredentials(this.accessKey, this.secretKey));
        this.s3Client.setEndpoint(this.endpoint);
    }

    public void upload(String from, String to) {
        TransferManager transferManager = new TransferManager(this.s3Client);

        Upload upload = transferManager.upload(this.bucket, to, new File(from));
        TransferProgress p = upload.getProgress();
        while (upload.isDone() == false) {
            int percent = (int) (p.getPercentTransferred());
            System.out.print("\r" + from + " - " + "[ " + percent + "% ] "
                + p.getBytesTransferred() + " / " + p.getTotalBytesToTransfer());
            try {
                Thread.sleep(500);
            } catch (Exception e) {

            }
        }
        try {
            upload.waitForCompletion();
            s3Client.setObjectAcl(this.bucket, to, CannedAccessControlList.PublicRead);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            transferManager.shutdownNow();
        }
        System.out.print("\r" + from + " - " + "[ 100% ] "
            + p.getBytesTransferred() + " / " + p.getTotalBytesToTransfer());
    }
}