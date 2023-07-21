package com.emc.mongoose.storage.driver.s3;

import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

import com.emc.mongoose.api.common.Constants;

import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.util.AsciiString;
import io.netty.util.AttributeKey;

/**
 Created by kurila on 02.08.16.
 */
public interface AmzS3Api {

	String PREFIX_KEY_X_AMZ = "x-amz-";

	String AUTH_PREFIX = "AWS ";
	
	String AUTH_V4_PREFIX = "AWS4-HMAC-SHA256 ";

	String KEY_X_AMZ_COPY_SOURCE = PREFIX_KEY_X_AMZ + "copy-source";

	String KEY_X_AMZ_SECURITY_TOKEN = PREFIX_KEY_X_AMZ + "security-token";

	String AMZ_DEFAULT_REGION = "us-east-1";
	String AMZ_DATE_HEADER = "x-amz-date";
	String AMZ_PAYLOAD_HEADER = "x-amz-content-sha256";
	String AMZ_UNSIGNED_PAYLOAD = "UNSIGNED-PAYLOAD";
	String AMZ_EMPTY_BODY_SHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
	
	AsciiString HEADERS_CANONICAL[] = {
		HttpHeaderNames.CONTENT_MD5,
		HttpHeaderNames.CONTENT_TYPE,
		//HttpHeaderNames.RANGE,
		HttpHeaderNames.DATE
	};
	AsciiString HEADERS_CANONICAL_V4[] = {
			HttpHeaderNames.CONTENT_MD5,
			HttpHeaderNames.CONTENT_TYPE,
			//HttpHeaderNames.RANGE,
			HttpHeaderNames.DATE,
			HttpHeaderNames.HOST,
	};
	
	String URL_ARG_VERSIONING = "versioning";

	String SIGN_METHOD = "HmacSHA1";
	
	String SIGN_V4_METHOD = "HmacSHA256";

	byte[] VERSIONING_ENABLE_CONTENT = (
		"<VersioningConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">" +
			"<Status>Enabled</Status></VersioningConfiguration>"
		).getBytes(StandardCharsets.US_ASCII);

	byte[] VERSIONING_DISABLE_CONTENT = (
		"<VersioningConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">" +
			"<Status>Suspended</Status></VersioningConfiguration>"
		).getBytes(StandardCharsets.US_ASCII);

	String KEY_UPLOAD_ID = "uploadId";

	AttributeKey<String> KEY_ATTR_UPLOAD_ID = AttributeKey.newInstance(KEY_UPLOAD_ID);

	String COMPLETE_MPU_HEADER = "<CompleteMultipartUpload>\n";
	String COMPLETE_MPU_PART_NUM_START = "\t<Part>\n\t\t<PartNumber>";
	String COMPLETE_MPU_PART_NUM_END = "</PartNumber>\n\t\t<ETag>";
	String COMPLETE_MPU_PART_ETAG_END = "</ETag>\n\t</Part>\n";
	String COMPLETE_MPU_FOOTER = "</CompleteMultipartUpload>";

	int MAX_KEYS_LIMIT = 1000;
	String QNAME_ITEM = "Contents";
	String QNAME_ITEM_ID = "Key";
	String QNAME_ITEM_SIZE = "Size";
	String QNAME_IS_TRUNCATED = "IsTruncated";
	
	// TODO Copied from master branch DateUtil
	TimeZone TZ_UTC = TimeZone.getTimeZone("UTC");
	// e.g. 20210901T182320Z
	String PATTERN_AMAZON = "yyyyMMdd'T'HHmmss'Z'"; // close to ISO_INSTANT but not quite
	DateFormat FMT_DATE_AMAZON = new SimpleDateFormat(PATTERN_AMAZON, Constants.LOCALE_DEFAULT) {
		{
			setTimeZone(TZ_UTC);
		}
	};
}
