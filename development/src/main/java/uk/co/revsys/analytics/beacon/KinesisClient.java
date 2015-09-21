package uk.co.revsys.analytics.beacon;

import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.google.appengine.repackaged.org.apache.commons.codec.binary.Hex;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import com.google.appengine.repackaged.org.apache.commons.codec.binary.Base64;
import java.util.Date;
import java.util.TimeZone;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.apache.commons.io.IOUtils;
import org.json.JSONObject;

public class KinesisClient {

    private String accessKey;
    private String secretKey;

    private SimpleDateFormat datestampFormat;
    private SimpleDateFormat timestampFormat;
    private MessageDigest digest;

    public KinesisClient(String accessKey, String secretKey) throws NoSuchAlgorithmException {
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        datestampFormat = new SimpleDateFormat("YYYYMMdd");
        datestampFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        timestampFormat = new SimpleDateFormat("YYYYMMdd'T'HHmmss'Z'");
        timestampFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        digest = MessageDigest.getInstance("SHA-256");
    }

    public PutRecordResult putRecord(String region, String stream, String partitionKey, ByteBuffer data) throws IOException, InvalidKeyException, NoSuchAlgorithmException {
        String encodedData = new String(Base64.encodeBase64(data.array()), Charset.forName("UTF-8"));
        String payload = "{\"PartitionKey\": \"" + partitionKey + "\", \"StreamName\": \"" + stream + "\", \"Data\": \"" + encodedData + "\"}";
        URL url = new URL("https://kinesis." + region + ".amazonaws.com");
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        Date now = new Date();
        String datestamp = datestampFormat.format(now);
        String timestamp = timestampFormat.format(now);
        connection.setDoOutput(true);
        String signature = getSignature(datestamp, timestamp, region, payload);
        connection.setRequestProperty("Authorization", "AWS4-HMAC-SHA256 Credential=" + accessKey + "/" + datestamp + "/" + region + "/kinesis/aws4_request, SignedHeaders=content-type;host;x-amz-date;x-amz-target, Signature=" + signature);
        connection.setRequestProperty("X-Amz-Target", "Kinesis_20131202.PutRecord");
        connection.setRequestProperty("X-Amz-Date", timestamp);
        connection.setRequestProperty("Content-Type", "application/x-amz-json-1.1; charset=UTF-8");
        connection.setRequestMethod("POST");

        OutputStreamWriter writer = new OutputStreamWriter(connection.getOutputStream());
        writer.write(payload);
        writer.close();

        if (connection.getResponseCode() == HttpURLConnection.HTTP_OK) {
            String response = IOUtils.toString(connection.getInputStream());
            JSONObject json = new JSONObject(response);
            PutRecordResult result = new PutRecordResult();
            result.setSequenceNumber(json.getString("SequenceNumber"));
            result.setShardId(json.getString("ShardId"));
            return result;
        } else {
            throw new IOException(IOUtils.toString(connection.getErrorStream()));
        }
    }

    private byte[] computeHmacSHA256(byte[] key, String data) throws NoSuchAlgorithmException, InvalidKeyException, IllegalStateException,
            UnsupportedEncodingException {
        String algorithm = "HmacSHA256";
        String charsetName = "UTF-8";

        Mac sha256_HMAC = Mac.getInstance(algorithm);
        SecretKeySpec secret_key = new SecretKeySpec(key, algorithm);
        sha256_HMAC.init(secret_key);

        return sha256_HMAC.doFinal(data.getBytes(charsetName));
    }

    private byte[] computeHmacSHA256(String key, String data) throws NoSuchAlgorithmException, InvalidKeyException, IllegalStateException,
            UnsupportedEncodingException {
        return computeHmacSHA256(key.getBytes(), data);
    }

    private String getSignature(String datestamp, String timestamp, String region, String payload)
            throws InvalidKeyException, NoSuchAlgorithmException, IllegalStateException, UnsupportedEncodingException {
        String stringToSign = getStringToSign(datestamp, timestamp, payload);
        byte[] dateKey = computeHmacSHA256("AWS4" + secretKey, datestamp);
        byte[] dateRegionKey = computeHmacSHA256(dateKey, region);
        byte[] dateRegionServiceKey = computeHmacSHA256(dateRegionKey, "kinesis");
        byte[] signingKey = computeHmacSHA256(dateRegionServiceKey, "aws4_request");
        byte[] signature = computeHmacSHA256(signingKey, stringToSign);
        return Hex.encodeHexString(signature);
    }

    private String getStringToSign(String datestamp, String timestamp, String payload) throws UnsupportedEncodingException {
        String canonicalString = getCanonicalString(timestamp, payload);
        String hashedCanonicalString = sha256(canonicalString);
        String stringToSign = "AWS4-HMAC-SHA256\n" + timestamp + "\n" + datestamp + "/us-east-1/kinesis/aws4_request\n" + hashedCanonicalString;
        return stringToSign;
    }

    private String getCanonicalString(String timestamp, String payload) throws UnsupportedEncodingException {
        String hashedPayload = sha256(payload);
        String canonicalString = "POST\n/\n\ncontent-type:application/x-amz-json-1.1; charset=UTF-8\nhost:kinesis.us-east-1.amazonaws.com\nx-amz-date:" + timestamp + "\nx-amz-target:Kinesis_20131202.PutRecord\n\ncontent-type;host;x-amz-date;x-amz-target\n" + hashedPayload;
        return canonicalString;
    }

    private String sha256(String base) throws UnsupportedEncodingException {
        byte[] hash = digest.digest(base.getBytes("UTF-8"));
        return Hex.encodeHexString(hash);
    }

}
