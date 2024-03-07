package com.emc.mongoose.storage.driver.s3;

import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.logging.log4j.Level;
import org.xml.sax.SAXException;

import com.emc.mongoose.api.common.env.DateUtil;
import com.emc.mongoose.api.common.exception.OmgShootMyFootException;
import com.emc.mongoose.api.model.data.DataInput;
import com.emc.mongoose.api.model.io.IoType;
import com.emc.mongoose.api.model.io.task.IoTask;
import com.emc.mongoose.api.model.io.task.composite.data.CompositeDataIoTask;
import com.emc.mongoose.api.model.io.task.partial.data.PartialDataIoTask;
import com.emc.mongoose.api.model.item.DataItem;
import com.emc.mongoose.api.model.item.Item;
import com.emc.mongoose.api.model.item.ItemFactory;
import com.emc.mongoose.api.model.storage.Credential;
import com.emc.mongoose.storage.driver.net.http.base.HttpStorageDriverBase;
import com.emc.mongoose.ui.config.load.LoadConfig;
import com.emc.mongoose.ui.config.storage.StorageConfig;
import com.emc.mongoose.ui.log.LogUtil;
import com.emc.mongoose.ui.log.Loggers;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.EmptyHttpHeaders;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpStatusClass;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.AsciiString;

/**
 Created by kurila on 01.08.16.
 */
public class AmzS3StorageDriver<I extends Item, O extends IoTask<I>>
extends HttpStorageDriverBase<I, O> {
	

	private static final Base64.Encoder BASE64_ENCODER = Base64.getEncoder();
	private static final ThreadLocal<SAXParser> THREAD_LOCAL_XML_PARSER = new ThreadLocal<>();
	private static final ThreadLocal<StringBuilder>
		BUCKET_LIST_QUERY = ThreadLocal.withInitial(StringBuilder::new);
	
	// for v4 only
	private static final ConcurrentHashMap<String, SigningKeyCreateFunction> signingKeyCreateFunctionCache = new ConcurrentHashMap<>();
	private static final ConcurrentHashMap<String, byte[]> signingKeyCache = new ConcurrentHashMap<>();
	// for v2 only
	private static final ConcurrentHashMap<String, Mac> MAC_BY_SECRET = new ConcurrentHashMap<>();
	private static final Function<String, Mac> GET_MAC_BY_SECRET = secret -> {
		final SecretKeySpec secretKey = new SecretKeySpec(secret.getBytes(UTF_8), AmzS3Api.SIGN_METHOD);
		try {
			final Mac mac = Mac.getInstance(AmzS3Api.SIGN_METHOD);
			mac.init(secretKey);
			return mac;
		} catch(final NoSuchAlgorithmException | InvalidKeyException e) {
			throw new AssertionError(e);
		}
	};
	
	private int authVersion = 2;
	private boolean validateNewPaths = true;

	public AmzS3StorageDriver(
		final String stepId, final DataInput itemDataInput, final LoadConfig loadConfig,
		final StorageConfig storageConfig, final boolean verifyFlag
	) throws OmgShootMyFootException, InterruptedException {
		super(stepId, itemDataInput, loadConfig, storageConfig, verifyFlag);
		requestAuthTokenFunc = null; // do not use
		
		// TODO Add system property to set S3 auth version (e.g. java -Ds3.auth.version=4 -jar mongoose.jar ...)
		String authVersion = System.getProperty("s3.auth.version");
		if (authVersion != null) {
			this.authVersion = Integer.valueOf(authVersion);
			Loggers.MSG.info("Set S3 auth version {}", this.authVersion);
		}

		// TODO Add system property to disable validating new paths (e.g. java -Dvalidate.new.paths=false -jar mongoose.jar ...)
		String validateNewPaths = System.getProperty("validate.new.paths");
		if (validateNewPaths != null) {
			this.validateNewPaths = Boolean.valueOf(validateNewPaths);
			Loggers.MSG.info("Set validate new paths to {}", this.validateNewPaths);
		}
	}

	private final class SigningKeyCreateFunctionImpl implements SigningKeyCreateFunction {
		String secret;

		public SigningKeyCreateFunctionImpl(final String secret) {
			this.secret = secret;
		}

		@Override
		public byte[] apply(final String datestamp) {
			return getSignatureKey(secret, datestamp);
		}
	}
	
	private byte[] HmacSHA256(String data, byte[] key) throws NoSuchAlgorithmException, InvalidKeyException {
		Mac mac = Mac.getInstance(AmzS3Api.SIGN_V4_METHOD);
		mac.init(new SecretKeySpec(key, "RawBytes"));
		return mac.doFinal(data.getBytes(UTF_8));
	}

	private byte[] getSignatureKey(String key, String dateStamp) {
		byte[] kSigning = new byte[0];
		try {
			byte[] kSecret = ("AWS4" + key).getBytes(UTF_8);
			byte[] kDate = HmacSHA256(dateStamp, kSecret);
			byte[] kRegion = HmacSHA256(AmzS3Api.AMZ_DEFAULT_REGION, kDate);
			byte[] kService = HmacSHA256("s3", kRegion);
			kSigning = HmacSHA256("aws4_request", kService);
			int[] m = new int[100];
			for (int i = 0; i < kSigning.length; i++) {
				m[i] = kSigning[i] & 0xFF;
			}
			Loggers.MSG.info("sign key", m);
		} catch (NoSuchAlgorithmException | InvalidKeyException e) {
			Loggers.MSG.warn("Couldn't complete HmacSHA256 for auth v4. Either secret is incorrect " +
					"or used algorithm doesn't exist. \n{}", e.getMessage());
		}
		return kSigning;
	}
	
	@Override
	protected String requestNewPath(final String path) {
		if (!validateNewPaths) {
			return path;
		}
		
		// check the destination bucket if it exists w/ HEAD request
		final String nodeAddr = storageNodeAddrs[0];
		HttpHeaders reqHeaders = new DefaultHttpHeaders();
		reqHeaders.set(HttpHeaderNames.HOST, nodeAddr);
		reqHeaders.set(HttpHeaderNames.CONTENT_LENGTH, 0);
		reqHeaders.set(HttpHeaderNames.DATE, DATE_SUPPLIER.get());
		applyDynamicHeaders(reqHeaders);
		applySharedHeaders(reqHeaders);

		final Credential credential = pathToCredMap.getOrDefault(path, this.credential);
		applyAuthHeaders(reqHeaders, HttpMethod.HEAD, path, credential);
		final FullHttpRequest checkBucketReq = new DefaultFullHttpRequest(
			HttpVersion.HTTP_1_1, HttpMethod.HEAD, path, Unpooled.EMPTY_BUFFER, reqHeaders,
			EmptyHttpHeaders.INSTANCE
		);
		final FullHttpResponse checkBucketResp;
		try {
			checkBucketResp = executeHttpRequest(checkBucketReq);
		} catch(final InterruptedException e) {
			throw new CancellationException();
		} catch(final ConnectException e) {
			LogUtil.exception(Level.WARN, e, "Failed to connect to the storage node");
			return null;
		}

		boolean bucketExistedBefore = true;
		if(checkBucketResp != null) {
			if(HttpResponseStatus.NOT_FOUND.equals(checkBucketResp.status())) {
				bucketExistedBefore = false;
			} else if(!HttpStatusClass.SUCCESS.equals(checkBucketResp.status().codeClass())) {
				Loggers.ERR.warn(
					"The bucket checking response is: {}", checkBucketResp.status().toString()
				);
			}
			checkBucketResp.release();
		}

		// create the destination bucket if it doesn't exists
		if(!bucketExistedBefore) {
			reqHeaders = new DefaultHttpHeaders();
			reqHeaders.set(HttpHeaderNames.HOST, nodeAddr);
			reqHeaders.set(HttpHeaderNames.CONTENT_LENGTH, 0);
			reqHeaders.set(HttpHeaderNames.DATE, DATE_SUPPLIER.get());
			applyMetaDataHeaders(reqHeaders);
			applyAuthHeaders(reqHeaders, HttpMethod.PUT, path, credential);
			final FullHttpRequest putBucketReq = new DefaultFullHttpRequest(
				HttpVersion.HTTP_1_1, HttpMethod.PUT, path, Unpooled.EMPTY_BUFFER, reqHeaders,
				EmptyHttpHeaders.INSTANCE
			);
			final FullHttpResponse putBucketResp;
			try {
				putBucketResp = executeHttpRequest(putBucketReq);
			} catch(final InterruptedException e) {
				throw new CancellationException();
			} catch(final ConnectException e) {
				LogUtil.exception(Level.WARN, e, "Failed to connect to the storage node");
				return null;
			}

			if(!HttpStatusClass.SUCCESS.equals(putBucketResp.status().codeClass())) {
				Loggers.ERR.warn(
					"The bucket creating response is: {}", putBucketResp.status().toString()
				);
				return null;
			}
			putBucketResp.release();
		}

		// check the bucket versioning state
		final String bucketVersioningReqUri = path + "?" + AmzS3Api.URL_ARG_VERSIONING;
		reqHeaders = new DefaultHttpHeaders();
		reqHeaders.set(HttpHeaderNames.HOST, nodeAddr);
		reqHeaders.set(HttpHeaderNames.CONTENT_LENGTH, 0);
		reqHeaders.set(HttpHeaderNames.DATE, DATE_SUPPLIER.get());
		applyAuthHeaders(reqHeaders, HttpMethod.GET, bucketVersioningReqUri, credential);
		final FullHttpRequest getBucketVersioningReq = new DefaultFullHttpRequest(
			HttpVersion.HTTP_1_1, HttpMethod.GET, bucketVersioningReqUri, Unpooled.EMPTY_BUFFER,
			reqHeaders, EmptyHttpHeaders.INSTANCE
		);
		final FullHttpResponse getBucketVersioningResp;
		try {
			getBucketVersioningResp = executeHttpRequest(getBucketVersioningReq);
		} catch(final InterruptedException e) {
			throw new CancellationException();
		} catch(final ConnectException e) {
			LogUtil.exception(Level.WARN, e, "Failed to connect to the storage node");
			return null;
		}

		final boolean versioningEnabled;
		if(!HttpStatusClass.SUCCESS.equals(getBucketVersioningResp.status().codeClass())) {
			Loggers.ERR.warn(
				"The bucket versioning checking response is: {}",
				getBucketVersioningResp.status().toString()
			);
			return null;
		} else {
			final String content = getBucketVersioningResp
				.content()
				.toString(StandardCharsets.US_ASCII);
			versioningEnabled = content.contains("Enabled");
		}
		getBucketVersioningResp.release();

		final FullHttpRequest putBucketVersioningReq;
		if(!versioning && versioningEnabled) {
			// disable bucket versioning
			reqHeaders = new DefaultHttpHeaders();
			reqHeaders.set(HttpHeaderNames.HOST, nodeAddr);
			reqHeaders.set(HttpHeaderNames.DATE, DATE_SUPPLIER.get());
			reqHeaders.set(HttpHeaderNames.CONTENT_LENGTH, AmzS3Api.VERSIONING_DISABLE_CONTENT.length);
			applyAuthHeaders(reqHeaders, HttpMethod.PUT, bucketVersioningReqUri, credential);
			putBucketVersioningReq = new DefaultFullHttpRequest(
				HttpVersion.HTTP_1_1, HttpMethod.PUT, bucketVersioningReqUri,
				Unpooled.wrappedBuffer(AmzS3Api.VERSIONING_DISABLE_CONTENT).retain(), reqHeaders,
				EmptyHttpHeaders.INSTANCE
			);
			final FullHttpResponse putBucketVersioningResp;
			try {
				putBucketVersioningResp = executeHttpRequest(putBucketVersioningReq);
			} catch(final InterruptedException e) {
				throw new CancellationException();
			} catch(final ConnectException e) {
				LogUtil.exception(Level.WARN, e, "Failed to connect to the storage node");
				return null;
			}

			if(!HttpStatusClass.SUCCESS.equals(putBucketVersioningResp.status().codeClass())) {
				Loggers.ERR.warn("The bucket versioning setting response is: {}",
					putBucketVersioningResp.status().toString()
				);
				putBucketVersioningResp.release();
				return null;
			}
			putBucketVersioningResp.release();
		} else if(versioning && !versioningEnabled) {
			// enable bucket versioning
			reqHeaders = new DefaultHttpHeaders();
			reqHeaders.set(HttpHeaderNames.HOST, nodeAddr);
			reqHeaders.set(HttpHeaderNames.DATE, DATE_SUPPLIER.get());
			reqHeaders.set(HttpHeaderNames.CONTENT_LENGTH, AmzS3Api.VERSIONING_ENABLE_CONTENT.length);
			applyAuthHeaders(reqHeaders, HttpMethod.PUT, bucketVersioningReqUri, credential);
			putBucketVersioningReq = new DefaultFullHttpRequest(
				HttpVersion.HTTP_1_1, HttpMethod.PUT, bucketVersioningReqUri,
				Unpooled.wrappedBuffer(AmzS3Api.VERSIONING_ENABLE_CONTENT).retain(), reqHeaders,
				EmptyHttpHeaders.INSTANCE
			);
			final FullHttpResponse putBucketVersioningResp;
			try {
				putBucketVersioningResp = executeHttpRequest(putBucketVersioningReq);
			} catch(final InterruptedException e) {
				throw new CancellationException();
			} catch(final ConnectException e) {
				LogUtil.exception(Level.WARN, e, "Failed to connect to the storage node");
				return null;
			}

			if(!HttpStatusClass.SUCCESS.equals(putBucketVersioningResp.status().codeClass())) {
				Loggers.ERR.warn("The bucket versioning setting response is: {}",
					putBucketVersioningResp.status().toString()
				);
				putBucketVersioningResp.release();
				return null;
			}
			putBucketVersioningResp.release();
		}

		return path;
	}
	
	@Override
	protected final String requestNewAuthToken(final Credential credential) {
		throw new AssertionError("Should not be invoked");
	}
	
	@Override
	public final List<I> list(
		final ItemFactory<I> itemFactory, final String path, final String prefix, final int idRadix,
		final I lastPrevItem, final int count
	) throws IOException {

		final int countLimit = count < 1 || count > AmzS3Api.MAX_KEYS_LIMIT ? AmzS3Api.MAX_KEYS_LIMIT : count;

		final String nodeAddr = storageNodeAddrs[0];
		final HttpHeaders reqHeaders = new DefaultHttpHeaders();
		reqHeaders.set(HttpHeaderNames.HOST, nodeAddr);
		reqHeaders.set(HttpHeaderNames.CONTENT_LENGTH, 0);
		reqHeaders.set(HttpHeaderNames.DATE, DATE_SUPPLIER.get());
		applyDynamicHeaders(reqHeaders);
		applySharedHeaders(reqHeaders);

		final StringBuilder queryBuilder = BUCKET_LIST_QUERY.get();
		queryBuilder.setLength(0);
		queryBuilder.append(path).append('?');
		if(prefix != null && !prefix.isEmpty()) {
			queryBuilder.append("prefix=").append(prefix);
		}
		if(lastPrevItem != null) {
			if('?' != queryBuilder.charAt(queryBuilder.length() - 1)) {
				queryBuilder.append('&');
			}
			String lastItemName = lastPrevItem.getName();
			if(lastItemName.contains("/")) {
				lastItemName = lastItemName.substring(lastItemName.lastIndexOf('/') + 1);
			}
			queryBuilder.append("marker=").append(lastItemName);
		}
		if('?' != queryBuilder.charAt(queryBuilder.length() - 1)) {
			queryBuilder.append('&');
		}
		queryBuilder.append("max-keys=").append(countLimit);
		final String query = queryBuilder.toString();

		applyAuthHeaders(reqHeaders, HttpMethod.GET, path, credential);

		final FullHttpRequest checkBucketReq = new DefaultFullHttpRequest(
			HttpVersion.HTTP_1_1, HttpMethod.GET, query, Unpooled.EMPTY_BUFFER, reqHeaders,
			EmptyHttpHeaders.INSTANCE
		);
		final List<I> buff = new ArrayList<>(countLimit);
		final FullHttpResponse listResp;
		try {
			listResp = executeHttpRequest(checkBucketReq);
			final ByteBuf listRespContent = listResp.content();
			SAXParser listRespParser = THREAD_LOCAL_XML_PARSER.get();
			if(listRespParser == null) {
				listRespParser = SAXParserFactory.newInstance().newSAXParser();
				THREAD_LOCAL_XML_PARSER.set(listRespParser);
			} else {
				listRespParser.reset();
			}
			final BucketXmlListingHandler<I> listingHandler = new BucketXmlListingHandler<>(
				buff, path, itemFactory, idRadix
			);
			try(final InputStream contentStream = new ByteBufInputStream(listRespContent)) {
				listRespParser.parse(contentStream, listingHandler);
			}
			listRespContent.release();
			if(buff.size() == 0) {
				throw new EOFException();
			}
			if(!listingHandler.isTruncated()) {
				buff.add(null); // poison
			}
		} catch(final InterruptedException e) {
			throw new CancellationException();
		} catch(final SAXException | ParserConfigurationException e) {
			LogUtil.exception(Level.WARN, e, "Failed to init the XML response parser");
		} catch(final ConnectException e) {
			LogUtil.exception(Level.WARN, e, "Failed to connect to the storage node");
		}

		return buff;
	}

	@Override
	protected HttpRequest getHttpRequest(final O ioTask, final String nodeAddr)
	throws URISyntaxException {

		final HttpRequest httpRequest;
		final IoType ioType = ioTask.getIoType();

		if(ioTask instanceof CompositeDataIoTask) {
			if(IoType.CREATE.equals(ioType)) {
				final CompositeDataIoTask mpuTask = (CompositeDataIoTask) ioTask;
				if(mpuTask.allSubTasksDone()) {
					httpRequest = getCompleteMpuRequest(mpuTask, nodeAddr);
				} else { // this is the initial state of the task
					httpRequest = getInitMpuRequest(ioTask, nodeAddr);
				}
			} else {
				throw new AssertionError(
					"Non-create multipart operations are not implemented yet"
				);
			}
		} else if(ioTask instanceof PartialDataIoTask) {
			if(IoType.CREATE.equals(ioType)) {
				httpRequest = getUploadPartRequest((PartialDataIoTask) ioTask, nodeAddr);
			} else {
				throw new AssertionError(
					"Non-create multipart operations are not implemented yet"
				);
			}
		} else {
			httpRequest = super.getHttpRequest(ioTask, nodeAddr);
		}

		return httpRequest;
	}

	@Override
	protected final HttpMethod getTokenHttpMethod(final IoType ioType) {
		throw new AssertionError("Not implemented yet");
	}

	@Override
	protected final HttpMethod getPathHttpMethod(final IoType ioType) {
		switch(ioType) {
			case UPDATE:
				throw new AssertionError("Not implemnted yet");
			case READ:
				return HttpMethod.GET;
			case DELETE:
				return HttpMethod.DELETE;
			default:
				return HttpMethod.PUT;
		}
	}

	@Override
	protected final String getTokenUriPath(
		final I item, final String srcPath, final String dstPath, final IoType ioType
	) {
		throw new AssertionError("Not implemented");
	}

	@Override
	protected final String getPathUriPath(
		final I item, final String srcPath, final String dstPath, final IoType ioType
	) {
		final String itemName = item.getName();
		if(itemName.startsWith("/")) {
			return itemName;
		} else {
			return "/" + itemName;
		}
	}

	@Override
	protected void applyMetaDataHeaders(final HttpHeaders httpHeaders) {
	}

	private HttpRequest getInitMpuRequest(final O ioTask, final String nodeAddr) {
		final I item = ioTask.getItem();
		final String srcPath = ioTask.getSrcPath();
		if(srcPath != null && !srcPath.isEmpty()) {
			throw new AssertionError(
				"Multipart copy operation is not implemented yet"
			);
		}
		final String uriPath = getDataUriPath(item, srcPath, ioTask.getDstPath(), IoType.CREATE) +
			"?uploads";
		final HttpHeaders httpHeaders = new DefaultHttpHeaders();
		if(nodeAddr != null) {
			httpHeaders.set(HttpHeaderNames.HOST, nodeAddr);
		}
		httpHeaders.set(HttpHeaderNames.DATE, DATE_SUPPLIER.get());
		httpHeaders.set(HttpHeaderNames.CONTENT_LENGTH, 0);
		final HttpMethod httpMethod = HttpMethod.POST;
		final HttpRequest httpRequest = new DefaultHttpRequest(
			HTTP_1_1, httpMethod, uriPath, httpHeaders
		);
		applyMetaDataHeaders(httpHeaders);
		applyDynamicHeaders(httpHeaders);
		applySharedHeaders(httpHeaders);
		applyAuthHeaders(httpHeaders, httpMethod, uriPath, ioTask.getCredential());
		return httpRequest;
	}

	private HttpRequest getUploadPartRequest(
		final PartialDataIoTask ioTask, final String nodeAddr
	) {
		final I item = (I) ioTask.getItem();

		final String srcPath = ioTask.getSrcPath();
		final String uriPath = getDataUriPath(item, srcPath, ioTask.getDstPath(), IoType.CREATE) +
			"?partNumber=" + (ioTask.getPartNumber() + 1) +
			"&uploadId=" + ioTask.getParent().get(AmzS3Api.KEY_UPLOAD_ID);

		final HttpHeaders httpHeaders = new DefaultHttpHeaders();
		if(nodeAddr != null) {
			httpHeaders.set(HttpHeaderNames.HOST, nodeAddr);
		}
		httpHeaders.set(HttpHeaderNames.DATE, DATE_SUPPLIER.get());
		final HttpMethod httpMethod = HttpMethod.PUT;
		final HttpRequest httpRequest = new DefaultHttpRequest(
			HTTP_1_1, httpMethod, uriPath, httpHeaders
		);
		try {
			httpHeaders.set(HttpHeaderNames.CONTENT_LENGTH, ((DataItem) item).size());
		} catch(final IOException ignored) {
		}
		applyMetaDataHeaders(httpHeaders);
		applyDynamicHeaders(httpHeaders);
		applySharedHeaders(httpHeaders);
		applyAuthHeaders(httpHeaders, httpMethod, uriPath, ioTask.getCredential());
		return httpRequest;
	}

	private final static ThreadLocal<StringBuilder>
		THREAD_LOCAL_STRB = new ThreadLocal<StringBuilder>() {
			@Override
			protected final StringBuilder initialValue() {
				return new StringBuilder();
			}
		};

	private FullHttpRequest getCompleteMpuRequest(
		final CompositeDataIoTask mpuTask, final String nodeAddr
	) {
		final StringBuilder content = THREAD_LOCAL_STRB.get();
		content.setLength(0);
		content.append(AmzS3Api.COMPLETE_MPU_HEADER);

		final List<PartialDataIoTask> subTasks = mpuTask.getSubTasks();
		int nextPartNum;
		String nextEtag;
		for(final PartialDataIoTask subTask : subTasks) {
			nextPartNum = subTask.getPartNumber() + 1;
			nextEtag = mpuTask.get(Integer.toString(nextPartNum));
			content
				.append(AmzS3Api.COMPLETE_MPU_PART_NUM_START)
				.append(nextPartNum)
				.append(AmzS3Api.COMPLETE_MPU_PART_NUM_END)
				.append(nextEtag)
				.append(AmzS3Api.COMPLETE_MPU_PART_ETAG_END);
		}
		content.append(AmzS3Api.COMPLETE_MPU_FOOTER);

		final String srcPath = mpuTask.getSrcPath();
		final I item = (I) mpuTask.getItem();
		final String uploadId = mpuTask.get(AmzS3Api.KEY_UPLOAD_ID);
		final String uriPath = getDataUriPath(item, srcPath, mpuTask.getDstPath(), IoType.CREATE) +
			"?uploadId=" + uploadId;

		final HttpHeaders httpHeaders = new DefaultHttpHeaders();
		httpHeaders.set(HttpHeaderNames.HOST, nodeAddr);
		httpHeaders.set(HttpHeaderNames.DATE, DATE_SUPPLIER.get());
		final HttpMethod httpMethod = HttpMethod.POST;
		final String contentStr = content.toString();
		final FullHttpRequest httpRequest = new DefaultFullHttpRequest(
			HTTP_1_1, httpMethod, uriPath, Unpooled.wrappedBuffer(contentStr.getBytes()),
			httpHeaders, EmptyHttpHeaders.INSTANCE
		);
		httpHeaders.set(HttpHeaderNames.CONTENT_LENGTH, content.length());
		applyMetaDataHeaders(httpHeaders);
		applyDynamicHeaders(httpHeaders);
		applySharedHeaders(httpHeaders);
		applyAuthHeaders(httpHeaders, httpMethod, uriPath, mpuTask.getCredential());

		return httpRequest;
	}

	@Override
	public void complete(final Channel channel, final O ioTask) {
		if(channel != null && ioTask instanceof CompositeDataIoTask) {
			final CompositeDataIoTask compositeIoTask = (CompositeDataIoTask) ioTask;
			if(compositeIoTask.allSubTasksDone()) {
				Loggers.MULTIPART.info(
					"{},{},{}", compositeIoTask.getItem().getName(),
					compositeIoTask.get(AmzS3Api.KEY_UPLOAD_ID), compositeIoTask.getLatency()
				);
			} else {
				final String uploadId = channel.attr(AmzS3Api.KEY_ATTR_UPLOAD_ID).get();
				if(uploadId == null) {
					ioTask.setStatus(IoTask.Status.RESP_FAIL_NOT_FOUND);
				} else {
					// multipart upload has been initialized as a result of this I/O task
					compositeIoTask.put(AmzS3Api.KEY_UPLOAD_ID, uploadId);
				}
			}
		}
		super.complete(channel, ioTask);
	}

	@Override
	protected final void appendHandlers(final ChannelPipeline pipeline) {
		super.appendHandlers(pipeline);
		pipeline.addLast(new AmzS3ResponseHandler<>(this, verifyFlag));
	}

	@Override
	protected final void applyCopyHeaders(final HttpHeaders httpHeaders, final String srcPath)
	throws URISyntaxException {
		httpHeaders.set(AmzS3Api.KEY_X_AMZ_COPY_SOURCE, srcPath);
	}
	
	private static final byte[] HEX_ARRAY = "0123456789abcdef".getBytes(StandardCharsets.US_ASCII);
	protected static String bytesToHex(byte[] bytes) {
		byte[] hexChars = new byte[bytes.length * 2];
		for (int j = 0; j < bytes.length; j++) {
			int v = bytes[j] & 0xFF;
			hexChars[j * 2] = HEX_ARRAY[v >>> 4];
			hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
		}
		return new String(hexChars, StandardCharsets.UTF_8);
	}
	
	@Override
	protected final void applyAuthHeaders(
		final HttpHeaders httpHeaders, final HttpMethod httpMethod, final String dstUriPath,
		final Credential credential
	) {
		final String uid;
		final String secret;
		if(credential != null) {
			uid = credential.getUid();
			secret = credential.getSecret();
		} else if(this.credential != null) {
			uid = this.credential.getUid();
			secret = this.credential.getSecret();
		} else {
			return;
		}
		
		if(uid == null || secret == null) {
			return;
		}
		
		try {
			if (authVersion == 2) {
				final var mac = computeIfAbsent(MAC_BY_SECRET, secret, GET_MAC_BY_SECRET);
				final var canonicalForm = getCanonical(httpHeaders, httpMethod, dstUriPath);
				final var sigData = mac.doFinal(canonicalForm.getBytes());
				httpHeaders.set(
						HttpHeaderNames.AUTHORIZATION,
						AmzS3Api.AUTH_PREFIX + uid + ':' + BASE64_ENCODER.encodeToString(sigData));
			} else if (authVersion == 4) {
				// Remove port from the host
				httpHeaders.set(HttpHeaderNames.HOST, httpHeaders.get(HttpHeaderNames.HOST).split(":")[0]);
				
				// Set dates
				Date now = new Date();
				httpHeaders.set(HttpHeaderNames.DATE, DateUtil.FMT_DATE_RFC1123.format(now));
				httpHeaders.set(AmzS3Api.AMZ_DATE_HEADER, AmzS3Api.FMT_DATE_AMAZON.format(now));
				
				// Set payload header
				if (Integer.valueOf(httpHeaders.get(HttpHeaderNames.CONTENT_LENGTH)) > 0) {
					httpHeaders.set(AmzS3Api.AMZ_PAYLOAD_HEADER, AmzS3Api.AMZ_UNSIGNED_PAYLOAD);
				} else {
					httpHeaders.set(AmzS3Api.AMZ_PAYLOAD_HEADER, AmzS3Api.AMZ_EMPTY_BODY_SHA256);
				}
				
				final var datetime = httpHeaders.get(AmzS3Api.AMZ_DATE_HEADER);
				final var date = datetime.substring(0,8); // 8 chars as Pattern("yyyyMMdd") goes
				
				final var signingKeyCreateFunction = computeIfAbsent(signingKeyCreateFunctionCache, secret, SigningKeyCreateFunctionImpl::new);
				final var signingKey  = computeIfAbsent(signingKeyCache, date, signingKeyCreateFunction);
				final Map<String, String> sortedHeaders = getNonCanonicalHeaders(httpHeaders);
				final var canonicalForm = getCanonicalV4(httpHeaders, sortedHeaders, httpMethod, dstUriPath);
				byte[] encodedhash = MessageDigest.getInstance("SHA-256").digest(
						canonicalForm.getBytes(StandardCharsets.UTF_8));
				String stringToSign = "AWS4-HMAC-SHA256\n" + datetime + "\n" + date
						+ "/" + AmzS3Api.AMZ_DEFAULT_REGION + "/s3/aws4_request\n" + bytesToHex(encodedhash);
				final var sigData = HmacSHA256(stringToSign, signingKey);
				// 240 is an educated guess based on how v4 auth header looks generally
				StringBuilder sb = new StringBuilder(240);
				sb.append(AmzS3Api.AUTH_V4_PREFIX)
						.append("Credential=")
						.append(uid)
						.append('/')
						.append(date)
						.append("/")
						.append(AmzS3Api.AMZ_DEFAULT_REGION)
						.append("/")
						.append("s3/aws4_request,SignedHeaders=")
						.append(String.join(";", sortedHeaders.keySet()))
						.append(",Signature=")
						.append(bytesToHex(sigData));
				httpHeaders.set(
						HttpHeaderNames.AUTHORIZATION, sb.toString());
			} else {
				throw new AssertionError("Not implemented yet");
			}
		} catch (NoSuchAlgorithmException | InvalidKeyException e) {
			Loggers.MSG.warn("Couldn't complete HmacSHA256 for auth v4. Either secret is incorrect " +
					"or used algorithm doesn't exist. \n{}", e.getMessage());
		}
	}
	
	protected Map<String,String> getNonCanonicalHeaders(final HttpHeaders httpHeaders) {
		String headerName;
		final Map<String, String> sortedHeaders = new TreeMap<>();
		if (sharedHeaders != null) {
			for (final var header : sharedHeaders) {
				headerName = header.getKey().toLowerCase();
				if (headerName.startsWith(AmzS3Api.PREFIX_KEY_X_AMZ)) {
					sortedHeaders.put(headerName, header.getValue());
				}
			}
		}
		for (final var header : httpHeaders) {
			headerName = header.getKey().toLowerCase();
			if (headerName.startsWith(AmzS3Api.PREFIX_KEY_X_AMZ)) {
				sortedHeaders.put(headerName, header.getValue());
			}
		}
		return sortedHeaders;
	}
	protected String getCanonicalV4(final HttpHeaders httpHeaders, final Map<String, String> sortedNonCanonicalHeaders,
									final HttpMethod httpMethod, final String dstUriPath) {
		final var buffCanonical = new StringBuilder();
		buffCanonical.append(httpMethod.name());
		buffCanonical.append('\n');
		int queryIndex = dstUriPath.indexOf("?");
		if (queryIndex != -1) {
			buffCanonical.append(dstUriPath.substring(0,  queryIndex));
			buffCanonical.append('\n');
			// Parse the query string
			String query = dstUriPath.substring(queryIndex + 1);
			buffCanonical.append(query);
			if (!query.contains("=")) {
				buffCanonical.append('=');
			}
		} else {
			// No query string
			buffCanonical.append(dstUriPath);
			buffCanonical.append('\n');
		}
		for (final var header : AmzS3Api.HEADERS_CANONICAL_V4) {
			if (httpHeaders.contains(header)) {
				for (final var headerValue : httpHeaders.getAll(header)) {
					sortedNonCanonicalHeaders.put(header.toString(), headerValue);
				}
			} else if (sharedHeaders != null && sharedHeaders.contains(header)) {
				for (final var headerValue : sharedHeaders.getAll(header)) {
					sortedNonCanonicalHeaders.put(header.toString(), headerValue);
				}
			}
		}

		// x-amz-*
		for (final var sortedHeader : sortedNonCanonicalHeaders.entrySet()) {
			buffCanonical
					.append('\n')
					.append(sortedHeader.getKey())
					.append(':')
					.append(sortedHeader.getValue());
		}
		buffCanonical.append("\n\n");

		for (final var sortedHeader : sortedNonCanonicalHeaders.keySet()) {
			buffCanonical
					.append(sortedHeader)
					.append(';');
		}

		buffCanonical.deleteCharAt(buffCanonical.length() - 1); // to remove last ;
		buffCanonical.append('\n');
		
		// Don't sign the payload
		if (Integer.valueOf(httpHeaders.get(HttpHeaderNames.CONTENT_LENGTH)) > 0) {
			buffCanonical.append(AmzS3Api.AMZ_UNSIGNED_PAYLOAD);
		} else {
			buffCanonical.append(AmzS3Api.AMZ_EMPTY_BODY_SHA256);
		}
		
		if (Loggers.MSG.isTraceEnabled()) {
			Loggers.MSG.trace("Canonical representation:\n{}", buffCanonical);
		}
		return buffCanonical.toString();
	}

	
	protected String getCanonical(
		final HttpHeaders httpHeaders, final HttpMethod httpMethod, final String dstUriPath
	) {
		final StringBuilder buffCanonical = new StringBuilder();
		buffCanonical.append(httpMethod.name());
		
		for(final AsciiString headerName : AmzS3Api.HEADERS_CANONICAL) {
			if(httpHeaders.contains(headerName)) {
				for(final String headerValue: httpHeaders.getAll(headerName)) {
					buffCanonical.append('\n').append(headerValue);
				}
			} else if(sharedHeaders != null && sharedHeaders.contains(headerName)) {
				buffCanonical.append('\n').append(sharedHeaders.get(headerName));
			} else {
				buffCanonical.append('\n');
			}
		}

		// x-amz-*
		String headerName;
		Map<String, String> sortedHeaders = new TreeMap<>();
		if(sharedHeaders != null) {
			for(final Map.Entry<String, String> header : sharedHeaders) {
				headerName = header.getKey().toLowerCase();
				if(headerName.startsWith(AmzS3Api.PREFIX_KEY_X_AMZ)) {
					sortedHeaders.put(headerName, header.getValue());
				}
			}
		}
		for(final Map.Entry<String, String> header : httpHeaders) {
			headerName = header.getKey().toLowerCase();
			if(headerName.startsWith(AmzS3Api.PREFIX_KEY_X_AMZ)) {
				sortedHeaders.put(headerName, header.getValue());
			}
		}
		for(final Map.Entry<String, String> sortedHeader : sortedHeaders.entrySet()) {
			buffCanonical
				.append('\n').append(sortedHeader.getKey())
				.append(':').append(sortedHeader.getValue());
		}
		
		buffCanonical.append('\n');
		buffCanonical.append(dstUriPath);
		
		if(Loggers.MSG.isTraceEnabled()) {
			Loggers.MSG.trace("Canonical representation:\n{}", buffCanonical);
		}
		
		return buffCanonical.toString();
	}

	private <K, V> V computeIfAbsent(ConcurrentHashMap<K, V> map, K key, Function<? super K, ? extends V> func) {
		// Avoid locking on computeIfAbsent with older JDKs
		V value = map.get(key);
		if (value != null) {
			return value;
		} else {
			return map.computeIfAbsent(key, func);
		}
	}
	
	@Override
	public String toString() {
		return String.format(super.toString(), "amzs3");
	}
}
