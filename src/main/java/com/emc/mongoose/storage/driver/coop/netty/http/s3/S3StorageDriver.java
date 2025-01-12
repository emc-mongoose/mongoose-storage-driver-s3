package com.emc.mongoose.storage.driver.coop.netty.http.s3;

import static com.emc.mongoose.base.item.op.Operation.SLASH;
import static com.emc.mongoose.storage.driver.coop.netty.http.s3.S3Api.TAGGING_ENTRY_END;
import static com.emc.mongoose.storage.driver.coop.netty.http.s3.S3Api.TAGGING_ENTRY_MIDDLE;
import static com.emc.mongoose.storage.driver.coop.netty.http.s3.S3Api.TAGGING_ENTRY_START;
import static com.emc.mongoose.storage.driver.coop.netty.http.s3.S3Api.TAGGING_FOOTER;
import static com.emc.mongoose.storage.driver.coop.netty.http.s3.S3Api.TAGGING_HEADER;
import static com.github.akurilov.commons.io.el.ExpressionInput.ASYNC_MARKER;
import static com.github.akurilov.commons.io.el.ExpressionInput.INIT_MARKER;
import static com.github.akurilov.commons.io.el.ExpressionInput.SYNC_MARKER;
import static com.github.akurilov.commons.lang.Exceptions.throwUnchecked;
import static io.netty.handler.codec.http.HttpMethod.DELETE;
import static io.netty.handler.codec.http.HttpMethod.GET;
import static io.netty.handler.codec.http.HttpMethod.PUT;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.emc.mongoose.base.config.ConstantValueInputImpl;
import com.emc.mongoose.base.config.el.CompositeExpressionInputBuilder;
import com.emc.mongoose.base.data.DataInput;
import com.emc.mongoose.base.env.DateUtil;
import com.emc.mongoose.base.config.IllegalConfigurationException;
import com.emc.mongoose.base.item.DataItem;
import com.emc.mongoose.base.item.Item;
import com.emc.mongoose.base.item.ItemFactory;
import com.emc.mongoose.base.item.op.OpType;
import com.emc.mongoose.base.item.op.Operation;
import com.emc.mongoose.base.item.op.composite.data.CompositeDataOperation;
import com.emc.mongoose.base.item.op.data.DataOperation;
import com.emc.mongoose.base.item.op.partial.data.PartialDataOperation;
import com.emc.mongoose.base.logging.LogUtil;
import com.emc.mongoose.base.logging.Loggers;
import com.emc.mongoose.base.storage.Credential;
import com.emc.mongoose.storage.driver.coop.netty.http.HttpStorageDriverBase;
import com.emc.mongoose.storage.driver.coop.netty.http.s3.S3Api.AMZChecksum;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.confuse.Config;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
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
import io.netty.handler.codec.http.HttpStatusClass;
import io.netty.handler.codec.http.HttpVersion;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.CRC32;
import java.util.zip.CRC32C;
import java.util.zip.Checksum;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import org.apache.logging.log4j.Level;
import org.xml.sax.SAXException;

/** Created by kurila on 01.08.16. */
public class S3StorageDriver<I extends Item, O extends Operation<I>>
				extends HttpStorageDriverBase<I, O> {

	protected static final Base64.Encoder BASE64_ENCODER = Base64.getEncoder();
	protected static final ThreadLocal<SAXParser> THREAD_LOCAL_XML_PARSER = new ThreadLocal<>();
	protected static final ThreadLocal<StringBuilder> BUFF_CANONICAL = ThreadLocal.withInitial(StringBuilder::new),
					BUCKET_LIST_QUERY = ThreadLocal.withInitial(StringBuilder::new);
	//for v4 only
	// TODO: compare to ConcurrentHashMap
	protected final ThreadLocal<Map<String, SigningKeyCreateFunction>> signingKeyCreateFunctionCache = ThreadLocal.withInitial(HashMap::new);
	protected final ThreadLocal<Map<String, byte[]>> signingKeyCache = ThreadLocal.withInitial(HashMap::new);
	// for v2 only
	protected static final ThreadLocal<Map<String, Mac>> MAC_BY_SECRET = ThreadLocal.withInitial(HashMap::new);
	protected static final Function<String, Mac> GET_MAC_BY_SECRET = secret -> {
		final var secretKey = new SecretKeySpec(secret.getBytes(UTF_8), S3Api.SIGN_METHOD);
		try {
			final var mac = Mac.getInstance(S3Api.SIGN_METHOD);
			mac.init(secretKey);
			return mac;
		} catch (final NoSuchAlgorithmException | InvalidKeyException e) {
			throw new AssertionError(e);
		}
	};
	private static final ThreadLocal<MessageDigest> THREAD_LOCAL_MD5 = ThreadLocal.withInitial(
		() -> {
			try {
				return MessageDigest.getInstance("MD5");
			} catch(final NoSuchAlgorithmException e) {
				throw new AssertionError(e);
			}
		}
	);
	// sha256 digest and md5 digest are used for different purposes
	private static final ThreadLocal<MessageDigest> THREAD_LOCAL_SHA256 = ThreadLocal.withInitial(
			() -> {
				try {
					return MessageDigest.getInstance("SHA-256");
				} catch(final NoSuchAlgorithmException e) {
					throw new AssertionError(e);
				}
			}
	);

	// Additional S3 checksums
	// https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/s3-checksums.html
	// https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html
	// https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html
	static class ChecksumMessageDigest extends MessageDigest {
		private final Checksum checksum;

		public ChecksumMessageDigest(Checksum checksum, String algorithm) {
			super(algorithm);
			this.checksum = checksum;
		}

		@Override
		protected void engineUpdate(byte input) {
			checksum.update(input);
		}

		@Override
		protected void engineUpdate(byte[] input, int offset, int len) {
			checksum.update(input, offset, len);
		}

		@Override
		protected byte[] engineDigest() {
			ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
			buffer.putInt((int) (checksum.getValue() & 0xFFFFFFFFL)); // Amazon is expecting this
			return buffer.array();
		}

		@Override
		protected void engineReset() {
			checksum.reset();
		}
	}
	private static final ThreadLocal<MessageDigest> THREAD_LOCAL_CRC32 = ThreadLocal.withInitial(
			() -> {
				return new ChecksumMessageDigest(new CRC32(), "CRC32");
			});
	private static final ThreadLocal<MessageDigest> THREAD_LOCAL_CRC32C = ThreadLocal.withInitial(
			() -> {
				return new ChecksumMessageDigest(new CRC32C(), "CRC32C");
			});
	private static final ThreadLocal<MessageDigest> THREAD_LOCAL_SHA1 = ThreadLocal.withInitial(
			() -> {
				try {
					return MessageDigest.getInstance("SHA-1");
				} catch (NoSuchAlgorithmException e) {
					throw new AssertionError(e);
				}
			});

	protected final boolean fsAccess;
	protected final boolean taggingEnabled;
	protected final Input<String> taggingContentInput;
	protected final int authVersion;
	protected final boolean versioning;
	protected final String awsRegion;
	protected final String checksumAlgorithm;

	public S3StorageDriver(
					final String stepId,
					final DataInput itemDataInput,
					final Config storageConfig,
					final boolean verifyFlag,
					final int batchSize)
					throws IllegalConfigurationException, InterruptedException {
		super(stepId, itemDataInput, storageConfig, verifyFlag, batchSize);
		final var objectConfig = storageConfig.configVal("object");
		fsAccess = objectConfig.boolVal("fsAccess");
		final var taggingConfig = objectConfig.configVal("tagging");
		taggingEnabled = taggingConfig.boolVal("enabled");
		if (taggingEnabled) {
			Loggers.MSG.info("{}: S3 Object Tagging Mode Enabled", stepId);
		}
		final var tags = taggingConfig.<String>mapVal("tags");
		final StringBuilder exprBuff = new StringBuilder();
		exprBuff.append(TAGGING_HEADER);
		for (final var tagEntry : tags.entrySet()) {
			final var k = tagEntry.getKey();
			final var v = tagEntry.getValue();
			exprBuff
				.append(TAGGING_ENTRY_START)
				.append(k)
				.append(TAGGING_ENTRY_MIDDLE)
				.append(v)
				.append(TAGGING_ENTRY_END);
		}
		exprBuff.append(TAGGING_FOOTER);
		final var taggingContentExpr = exprBuff.toString();
		if (taggingEnabled) {
			Loggers.MSG.debug("{}: tagging content pattern: {}", stepId, taggingContentExpr);
		}
		if(
			taggingContentExpr.contains(ASYNC_MARKER) ||
				taggingContentExpr.contains(SYNC_MARKER) ||
				taggingContentExpr.contains(INIT_MARKER)
		) {
			taggingContentInput = CompositeExpressionInputBuilder
				.newInstance()
				.expression(exprBuff.toString())
				.build();
		} else {
			taggingContentInput = new ConstantValueInputImpl<>(taggingContentExpr);
		}
		versioning = objectConfig.boolVal("versioning");
		if (versioning) {
			Loggers.MSG.info("Versioning is enabled. Make sure that if you use input items list it has version ids");
		}
		var authVersionTemp = storageConfig.intVal("auth-version");
		if ((authVersionTemp != 2) && (authVersionTemp != 4)) {
			throw new AssertionError("Only 2 and 4 versions are supported");
		}
		authVersion = authVersionTemp;
		requestAuthTokenFunc = null; // do not use
		
		// Validate the checksum algorithm
		if (storageConfig.boolVal("checksum-enabled")) {
			checksumAlgorithm = storageConfig.stringVal("checksum-algorithm");
			if (!Pattern.matches(S3Api.amzChecksumRegex(), checksumAlgorithm)) {
				throw new IllegalArgumentException("Invalid checksum algorithm: " + checksumAlgorithm);
			}
			Loggers.MSG.info("Checksum algorithm: {}", checksumAlgorithm);
		} else {
			checksumAlgorithm = null;
		}

		// Look for an AWS endpoint, e.g. "s3.us-east-1.amazonaws.com:80"
		Pattern awsPattern = Pattern.compile("s3\\.([^\\.]+)\\.amazonaws\\.com:[0-9]+");
		Matcher awsMatcher = null;
		if (storageNodeAddrs.length == 1 && (awsMatcher = awsPattern.matcher(storageNodeAddrs[0])).matches()) {
			// Extract the AWS region
			awsRegion = awsMatcher.group(1);
		} else {
			awsRegion = S3Api.AMZ_DEFAULT_REGION;
		}
	}

	private final class SigningKeyCreateFunctionImpl
			implements SigningKeyCreateFunction {

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
		Mac mac = Mac.getInstance(S3Api.SIGN_V4_METHOD);
		mac.init(new SecretKeySpec(key, "RawBytes"));
		return mac.doFinal(data.getBytes(UTF_8));
	}

	private byte[] getSignatureKey(String key, String dateStamp) {
		byte[] kSigning = new byte[0];
		try {
			byte[] kSecret = ("AWS4" + key).getBytes(UTF_8);
			byte[] kDate = HmacSHA256(dateStamp, kSecret);
			byte[] kRegion = HmacSHA256(awsRegion, kDate);
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
		final var relPath = path.startsWith(SLASH) ? path.substring(1) : path;
		final var slashPos = relPath.indexOf(SLASH);
		final var bucketPath = SLASH + (slashPos > 0 ? relPath.substring(0, slashPos) : relPath);
		final var uriQuery = uriQuery();
		final var uri = uriQuery == null || uriQuery.isEmpty() ? bucketPath : bucketPath + uriQuery;
		// check the destination bucket if it exists w/ HEAD request
		final var nodeAddr = storageNodeAddrs[0];
		var reqHeaders = (HttpHeaders) new DefaultHttpHeaders();
		reqHeaders.set(HttpHeaderNames.HOST, nodeAddr);
		reqHeaders.set(HttpHeaderNames.CONTENT_LENGTH, 0);
		applyDynamicHeaders(reqHeaders);
		applySharedHeaders(reqHeaders);
		final var credential = pathToCredMap.getOrDefault(uri, this.credential);
		applyAuthHeaders(reqHeaders, HttpMethod.HEAD, uri, credential);
		final FullHttpRequest checkBucketReq = new DefaultFullHttpRequest(
						HttpVersion.HTTP_1_1,
						HttpMethod.HEAD,
						uri,
						Unpooled.EMPTY_BUFFER,
						reqHeaders,
						EmptyHttpHeaders.INSTANCE);
		FullHttpResponse checkBucketResp = null;
		try {
			checkBucketResp = executeHttpRequest(checkBucketReq);
		} catch (final InterruptedException e) {
			throwUnchecked(e);
		} catch (final ConnectException e) {
			LogUtil.exception(Level.WARN, e, "Failed to connect to the storage node");
			return null;
		}
		var bucketExistedBefore = true;
		if (checkBucketResp != null) {
			if (!HttpStatusClass.SUCCESS.equals(checkBucketResp.status().codeClass())) {
				Loggers.MSG.info(
								"The bucket checking response is: {}", checkBucketResp.status().toString());
				bucketExistedBefore = false;
			}
			checkBucketResp.release();
		}
		// create the destination bucket if it doesn't exists
		if (!bucketExistedBefore) {
			reqHeaders = new DefaultHttpHeaders();
			reqHeaders.set(HttpHeaderNames.HOST, nodeAddr);
			reqHeaders.set(HttpHeaderNames.CONTENT_LENGTH, 0);
			applyMetaDataHeaders(reqHeaders);
			applyDynamicHeaders(reqHeaders);
			applySharedHeaders(reqHeaders);
			applyAuthHeaders(reqHeaders, HttpMethod.PUT, uri, credential);
			final FullHttpRequest putBucketReq = new DefaultFullHttpRequest(
							HttpVersion.HTTP_1_1,
							HttpMethod.PUT,
							uri,
							Unpooled.EMPTY_BUFFER,
							reqHeaders,
							EmptyHttpHeaders.INSTANCE);
			final FullHttpResponse putBucketResp;
			try {
				putBucketResp = executeHttpRequest(putBucketReq);
				if (!HttpStatusClass.SUCCESS.equals(putBucketResp.status().codeClass())) {
					Loggers.ERR.warn("The bucket creating response is: {}", putBucketResp.status().toString());
					return null;
				}
				putBucketResp.release();
			} catch (final InterruptedException e) {
				throwUnchecked(e);
			} catch (final ConnectException e) {
				LogUtil.exception(Level.WARN, e, "Failed to connect to the storage node");
				return null;
			}
		}

		// check the bucket versioning state
		if (versioning) {
			final var bucketVersioningReqUri = bucketPath + "?" + S3Api.URL_ARG_VERSIONING;
			reqHeaders = new DefaultHttpHeaders();
			reqHeaders.set(HttpHeaderNames.HOST, nodeAddr);
			reqHeaders.set(HttpHeaderNames.CONTENT_LENGTH, 0);
			applyDynamicHeaders(reqHeaders);
			applySharedHeaders(reqHeaders);
			applyAuthHeaders(reqHeaders, HttpMethod.GET, bucketVersioningReqUri, credential);
			final FullHttpRequest getBucketVersioningReq = new DefaultFullHttpRequest(
					HttpVersion.HTTP_1_1,
					HttpMethod.GET,
					bucketVersioningReqUri,
					Unpooled.EMPTY_BUFFER,
					reqHeaders,
					EmptyHttpHeaders.INSTANCE);
			final FullHttpResponse getBucketVersioningResp;
			try {
				getBucketVersioningResp = executeHttpRequest(getBucketVersioningReq);
				if (getBucketVersioningResp == null) {
					Loggers.ERR.warn("Response timeout");
				} else {
					try {
						handleCheckBucketVersioningResponse(
								getBucketVersioningResp, nodeAddr, bucketVersioningReqUri);
					} finally {
						getBucketVersioningResp.release();
					}
				}
			} catch (final InterruptedException e) {
				throwUnchecked(e);
			} catch (final ConnectException e) {
				LogUtil.exception(Level.WARN, e, "Failed to connect to the storage node");
			}
		}
		return path;
	}

	protected void handleCheckBucketVersioningResponse(
					final FullHttpResponse getBucketVersioningResp,
					final String nodeAddr,
					final String bucketVersioningReqUri) {
		var versioningEnabled = false;
		if (!HttpStatusClass.SUCCESS.equals(getBucketVersioningResp.status().codeClass())) {
			Loggers.ERR.warn(
							"The bucket versioning checking response is: {}",
							getBucketVersioningResp.status().toString());
		} else {
			final var content = getBucketVersioningResp.content().toString(StandardCharsets.US_ASCII);
			versioningEnabled = content.contains("Enabled");
		}
		if (versioning && !versioningEnabled) {
			enableBucketVersioning(nodeAddr, bucketVersioningReqUri);
		}
	}

	protected void enableBucketVersioning(
					final String nodeAddr, final String bucketVersioningReqUri) {
		final HttpHeaders reqHeaders = new DefaultHttpHeaders();
		reqHeaders.set(HttpHeaderNames.HOST, nodeAddr);
		reqHeaders.set(HttpHeaderNames.DATE, DateUtil.formatNowRfc1123());
		reqHeaders.set(HttpHeaderNames.CONTENT_LENGTH, S3Api.VERSIONING_ENABLE_CONTENT.length);
		applyAuthHeaders(reqHeaders, HttpMethod.PUT, bucketVersioningReqUri, credential);
		final var putBucketVersioningReq = (FullHttpRequest) new DefaultFullHttpRequest(
						HttpVersion.HTTP_1_1,
						HttpMethod.PUT,
						bucketVersioningReqUri,
						Unpooled.wrappedBuffer(S3Api.VERSIONING_ENABLE_CONTENT).retain(),
						reqHeaders,
						EmptyHttpHeaders.INSTANCE);
		final FullHttpResponse putBucketVersioningResp;
		try {
			putBucketVersioningResp = executeHttpRequest(putBucketVersioningReq);
			if (!HttpStatusClass.SUCCESS.equals(putBucketVersioningResp.status().codeClass())) {
				Loggers.ERR.warn(
								"The bucket versioning setting response is: {}",
								putBucketVersioningResp.status().toString());
			}
			putBucketVersioningResp.release();
		} catch (final InterruptedException e) {
			throwUnchecked(e);
		} catch (final ConnectException e) {
			LogUtil.exception(Level.WARN, e, "Failed to connect to the storage node");
		}
	}

	@Override
	protected final String requestNewAuthToken(final Credential credential) {
		throw new AssertionError("Should not be invoked");
	}

	@Override
	public List<I> list(
					final ItemFactory<I> itemFactory,
					final String path,
					final String prefix,
					final int idRadix,
					final I lastPrevItem,
					final int count)
					throws IOException {
		final var countLimit = count < 1 || count > S3Api.MAX_KEYS_LIMIT ? S3Api.MAX_KEYS_LIMIT : count;
		final var nodeAddr = storageNodeAddrs[0];
		final var reqHeaders = new DefaultHttpHeaders();
		reqHeaders.set(HttpHeaderNames.HOST, nodeAddr);
		reqHeaders.set(HttpHeaderNames.CONTENT_LENGTH, 0);
		applyDynamicHeaders(reqHeaders);
		applySharedHeaders(reqHeaders);
		final var uriBuilder = BUCKET_LIST_QUERY.get();
		uriBuilder.setLength(0);
		uriBuilder.append(path).append('?');
		if (prefix != null && !prefix.isEmpty()) {
			uriBuilder.append("prefix=").append(prefix);
		}
		if (lastPrevItem != null) {
			if ('?' != uriBuilder.charAt(uriBuilder.length() - 1)) {
				uriBuilder.append('&');
			}
			var lastItemName = lastPrevItem.name();
			if (lastItemName.contains("/")) {
				lastItemName = lastItemName.substring(lastItemName.lastIndexOf('/') + 1);
			}
			uriBuilder.append("marker=").append(lastItemName);
		}
		if ('?' != uriBuilder.charAt(uriBuilder.length() - 1)) {
			uriBuilder.append('&');
		}
		uriBuilder.append("max-keys=").append(countLimit);
		final var uri = uriBuilder.toString();
		applyAuthHeaders(reqHeaders, HttpMethod.GET, path, credential);
		final FullHttpRequest checkBucketReq = new DefaultFullHttpRequest(
						HttpVersion.HTTP_1_1,
						HttpMethod.GET,
						uri,
						Unpooled.EMPTY_BUFFER,
						reqHeaders,
						EmptyHttpHeaders.INSTANCE);
		final List<I> buff = new ArrayList<>(countLimit);
		try {
			final var listResp = executeHttpRequest(checkBucketReq);
			try {
				final var listRespContent = listResp.content();
				var listRespParser = THREAD_LOCAL_XML_PARSER.get();
				if (listRespParser == null) {
					listRespParser = SAXParserFactory.newInstance().newSAXParser();
					THREAD_LOCAL_XML_PARSER.set(listRespParser);
				} else {
					listRespParser.reset();
				}
				final var listingHandler = new BucketXmlListingHandler<>(buff, path, itemFactory, idRadix);
				try (final InputStream contentStream = new ByteBufInputStream(listRespContent)) {
					listRespParser.parse(contentStream, listingHandler);
				}
				if (buff.size() == 0) {
					throw new EOFException();
				}
				if (!listingHandler.isTruncated()) {
					buff.add(null); // poison
				}
			} finally {
				listResp.release();
			}
		} catch (final InterruptedException e) {
			throwUnchecked(e);
		} catch (final SAXException | ParserConfigurationException e) {
			LogUtil.exception(Level.WARN, e, "Failed to init the XML response parser");
		} catch (final ConnectException e) {
			LogUtil.exception(Level.WARN, e, "Failed to connect to the storage node");
		} catch (final NullPointerException e) {
			LogUtil.exception(Level.WARN, e, "Timeout response");
		}
		return buff;
	}

	@Override
	protected HttpRequest httpRequest(final O op, final String nodeAddr) throws URISyntaxException {
		final HttpRequest httpRequest;
		final OpType opType = op.type();
		if (op instanceof CompositeDataOperation) {
			if (OpType.CREATE.equals(opType)) {
				final var mpuOp = (CompositeDataOperation) op;
				if (mpuOp.allSubOperationsDone()) {
					httpRequest = completeMultipartUploadRequest(mpuOp, nodeAddr);
				} else { // this is the initial state of the task
					httpRequest = initMultipartUploadRequest(op, nodeAddr);
				}
			} else {
				throw new AssertionError("Non-create multipart operations are not implemented yet");
			}
		} else if (op instanceof PartialDataOperation) {
			if (OpType.CREATE.equals(opType)) {
				httpRequest = partUploadRequest((PartialDataOperation) op, nodeAddr);
			} else {
				throw new AssertionError("Non-create multipart operations are not implemented yet");
			}
		} else if (taggingEnabled) {
			httpRequest = objectTaggingRequest(op, nodeAddr);
		} else if (versioning) {
			httpRequest = objectVersioningRequest(op, nodeAddr);
		} else {
			httpRequest = super.httpRequest(op, nodeAddr);
		}
		return httpRequest;
	}

	@Override
	protected final HttpMethod tokenHttpMethod(final OpType opType) {
		throw new AssertionError("Not implemented yet");
	}

	@Override
	protected final HttpMethod pathHttpMethod(final OpType opType) {
		switch (opType) {
		case UPDATE:
			throw new AssertionError("Not implemented yet");
		case READ:
			return HttpMethod.GET;
		case DELETE:
			return DELETE;
		default:
			return HttpMethod.PUT;
		}
	}

	@Override
	protected final String tokenUriPath(
					final I item, final String srcPath, final String dstPath, final OpType opType) {
		throw new AssertionError("Not implemented");
	}

	@Override
	protected final String pathUriPath(
					final I item, final String srcPath, final String dstPath, final OpType opType) {
		final var itemName = item.name();
		if (itemName.startsWith(SLASH)) {
			return itemName;
		} else {
			return SLASH + itemName;
		}
	}

	// TODO Handle objectTaggingRequest()
	// TODO Handle other areas where applyAuthHeaders() is called
	@Override
	protected void applyChecksum(final HttpHeaders httpHeaders, final O op) {
		if (checksumAlgorithm == null || !(op.item() instanceof DataItem)) {
			return;
		}

		AMZChecksum amzChecksum = AMZChecksum.valueOf(checksumAlgorithm.toUpperCase());
		var dataItem = (DataItem) op.item();

		// Select digest
		MessageDigest digest = null;
		switch (amzChecksum) {
			case MD5:
				digest = THREAD_LOCAL_MD5.get();
				break;
			case CRC32:
				digest = THREAD_LOCAL_CRC32.get();
				break;
			case CRC32C:
				digest = THREAD_LOCAL_CRC32C.get();
				break;
			case SHA1:
				digest = THREAD_LOCAL_SHA1.get();
				break;
			case SHA256:
				digest = THREAD_LOCAL_SHA256.get();
				break;
			default:
				break;
		}

		try {
			// Reset the digest
			digest.reset();

			// Allocate temp buffer
			ByteBuffer dst = ByteBuffer.allocate(64 * 1024);
			int bytesRead = 0;
			String checksum = null;

			while (true) {
				// Limit to remaining bytes if buffer capacity exceeds data item size
				if (bytesRead + dst.capacity() > dataItem.size()) {
					dst.limit((int) dataItem.size() - bytesRead);
				}

				// Read and update digest
				bytesRead += dataItem.read(dst);
				dst.flip();
				digest.update(dst);
				dst.clear();

				// Compute checksum and exit when entire data item read
				if (bytesRead == dataItem.size()) {
					byte[] checksumBytes = digest.digest();
					checksum = BASE64_ENCODER.encodeToString(checksumBytes);
					break;
				}
			}

			// Add checksum header
			if (amzChecksum == AMZChecksum.MD5) {
				httpHeaders.set(HttpHeaderNames.CONTENT_MD5, checksum);
			} else {
				httpHeaders.set(S3Api.AMZ_CHECKSUM_PREFIX + amzChecksum.toString().toLowerCase(), checksum);
			}
		} catch (IOException e) {
			Loggers.ERR.info("Unable to compute checksum: {}", e.getMessage());
		} finally {
			// Always reset the data item
			dataItem.reset();
		}
	}

	@Override
	protected void applyMetaDataHeaders(final HttpHeaders httpHeaders) {}

	HttpRequest initMultipartUploadRequest(final O op, final String nodeAddr) {
		final var item = op.item();
		final var srcPath = op.srcPath();
		if (srcPath != null && !srcPath.isEmpty()) {
			throw new AssertionError("Multipart copy operation is not implemented yet");
		}
		final var uri = dataUriPath(item, srcPath, op.dstPath(), OpType.CREATE) + "?uploads";
		final var httpHeaders = new DefaultHttpHeaders();
		if (nodeAddr != null) {
			httpHeaders.set(HttpHeaderNames.HOST, nodeAddr);
		}
		httpHeaders.set(HttpHeaderNames.CONTENT_LENGTH, 0);
		final var httpMethod = HttpMethod.POST;
		final var httpRequest = (HttpRequest) new DefaultHttpRequest(HTTP_1_1, httpMethod, uri, httpHeaders);
		applyMetaDataHeaders(httpHeaders);
		applyDynamicHeaders(httpHeaders);
		applySharedHeaders(httpHeaders);
		applyAuthHeaders(httpHeaders, httpMethod, uri, op.credential());
		return httpRequest;
	}

	HttpRequest partUploadRequest(
					final PartialDataOperation partialDataOp, final String nodeAddr) {
		final var item = (I) partialDataOp.item();
		final var srcPath = partialDataOp.srcPath();
		final var uri = dataUriPath(item, srcPath, partialDataOp.dstPath(), OpType.CREATE)
						+ "?partNumber="
						+ (partialDataOp.partNumber() + 1)
						+ "&uploadId="
						+ partialDataOp.parent().get(S3Api.KEY_UPLOAD_ID);
		final HttpHeaders httpHeaders = new DefaultHttpHeaders();
		if (nodeAddr != null) {
			httpHeaders.set(HttpHeaderNames.HOST, nodeAddr);
		}
		final var httpMethod = HttpMethod.PUT;
		final HttpRequest httpRequest = new DefaultHttpRequest(HTTP_1_1, httpMethod, uri, httpHeaders);
		try {
			httpHeaders.set(HttpHeaderNames.CONTENT_LENGTH, ((DataItem) item).size());
		} catch (final IOException ignored) {}
		applyMetaDataHeaders(httpHeaders);
		applyDynamicHeaders(httpHeaders);
		applySharedHeaders(httpHeaders);
		applyAuthHeaders(httpHeaders, httpMethod, uri, partialDataOp.credential());
		return httpRequest;
	}

	private static final ThreadLocal<StringBuilder> THREAD_LOCAL_STRB = ThreadLocal.withInitial(StringBuilder::new);

	FullHttpRequest completeMultipartUploadRequest(
					final CompositeDataOperation mpuTask, final String nodeAddr) {
		final var content = THREAD_LOCAL_STRB.get();
		content.setLength(0);
		content.append(S3Api.COMPLETE_MPU_HEADER);
		final List<PartialDataOperation> subTasks = mpuTask.subOperations();
		int nextPartNum;
		String nextEtag;
		for (final var subTask : subTasks) {
			nextPartNum = subTask.partNumber() + 1;
			nextEtag = mpuTask.get(Integer.toString(nextPartNum));
			content
							.append(S3Api.COMPLETE_MPU_PART_NUM_START)
							.append(nextPartNum)
							.append(S3Api.COMPLETE_MPU_PART_NUM_END)
							.append(nextEtag)
							.append(S3Api.COMPLETE_MPU_PART_ETAG_END);
		}
		content.append(S3Api.COMPLETE_MPU_FOOTER);
		final var srcPath = mpuTask.srcPath();
		final var item = (I) mpuTask.item();
		final var uploadId = mpuTask.get(S3Api.KEY_UPLOAD_ID);
		final var uri = dataUriPath(item, srcPath, mpuTask.dstPath(), OpType.CREATE) + "?uploadId=" + uploadId;
		final HttpHeaders httpHeaders = new DefaultHttpHeaders();
		httpHeaders.set(HttpHeaderNames.HOST, nodeAddr);
		final var httpMethod = HttpMethod.POST;
		final var contentStr = content.toString();
		final FullHttpRequest httpRequest = new DefaultFullHttpRequest(
						HTTP_1_1,
						httpMethod,
						uri,
						Unpooled.wrappedBuffer(contentStr.getBytes()),
						httpHeaders,
						EmptyHttpHeaders.INSTANCE);
		httpHeaders.set(HttpHeaderNames.CONTENT_LENGTH, content.length());
		applyMetaDataHeaders(httpHeaders);
		applyDynamicHeaders(httpHeaders);
		applySharedHeaders(httpHeaders);
		applyAuthHeaders(httpHeaders, httpMethod, uri, mpuTask.credential());
		return httpRequest;
	}

	HttpRequest objectVersioningRequest(final O op, final String nodeAddr) throws URISyntaxException {
		final var srcPath = op.srcPath();
		final var item = (I) op.item();
		final var opType = op.type();
		final HttpHeaders httpHeaders = new DefaultHttpHeaders();
		// /<bucket>/<itemName>~<versionId>,
		// /bucket/30auvg3756cw~16788129946F0FF5,57ec29fc427c270,10,0/0
		var indexOfVersionStart = item.name().lastIndexOf('~');
		// versioning relies on the fact that ~ is used for versions only
		// so far ecs doesn't use ~ symbol in versionId. If that changes, then algorithm will break
		// another chance for it to break is when ~ is used in bucket name, versioning enabled, but
		// no actual version is provided. That can happen on the first run for creates
		String versionId = null;
		if (indexOfVersionStart != -1) {
			versionId = item.name().substring(indexOfVersionStart + 1);
			item.name(item.name().substring(0, indexOfVersionStart));
		}
		httpHeaders.set(HttpHeaderNames.HOST, nodeAddr);
		final var httpMethod = dataHttpMethod(opType);
		final var uri = dataUriPath(item, srcPath, op.dstPath(), op.type());
		final var httpRequest = (HttpRequest) new DefaultHttpRequest(HTTP_1_1, httpMethod, uri, httpHeaders);
		switch (opType) {
			case CREATE:
				if (srcPath == null || srcPath.isEmpty()) {
					if (item instanceof DataItem) {
						try {
							httpHeaders.set(HttpHeaderNames.CONTENT_LENGTH, ((DataItem) item).size());
						} catch (final IOException ignored) {}
					} else {
						httpHeaders.set(HttpHeaderNames.CONTENT_LENGTH, 0);
					}
				} else {
					applyCopyHeaders(httpHeaders, dataUriPath(item, srcPath, null, opType));
					httpHeaders.set(HttpHeaderNames.CONTENT_LENGTH, 0);
				}
				break;
			case READ:
				httpHeaders.set("x-amz-version-id", versionId);
				httpHeaders.set(HttpHeaderNames.CONTENT_LENGTH, 0);
				if (op instanceof DataOperation) {
					applyRangesHeaders(httpHeaders, (DataOperation) op);
				}
				break;
			case UPDATE:
				httpHeaders.set("x-amz-version-id", versionId);
				final var dataOp = (DataOperation) op;
				httpHeaders.set(HttpHeaderNames.CONTENT_LENGTH, dataOp.markedRangesSize());
				applyRangesHeaders(httpHeaders, dataOp);
				break;
			case DELETE:
				httpHeaders.set("x-amz-version-id", versionId);
				httpHeaders.set(HttpHeaderNames.CONTENT_LENGTH, 0);
				break;
		}
		applyMetaDataHeaders(httpHeaders);
		applyDynamicHeaders(httpHeaders);
		applySharedHeaders(httpHeaders);
		applyAuthHeaders(httpHeaders, httpRequest.method(), uri, op.credential());
		return httpRequest;
	}

	FullHttpRequest objectTaggingRequest(final O op, final String nodeAddr) {
		final var srcPath = op.srcPath();
		final var item = (I) op.item();
		final var uri = dataUriPath(item, srcPath, op.dstPath(), OpType.CREATE) + "?tagging";
		final HttpHeaders httpHeaders = new DefaultHttpHeaders();
		httpHeaders.set(HttpHeaderNames.HOST, nodeAddr);
		final var opType = op.type();
		var httpRequest = (FullHttpRequest) null;
		switch (opType) {
			case READ:
				httpRequest = new DefaultFullHttpRequest(
					HTTP_1_1,
					GET,
					uri,
					Unpooled.EMPTY_BUFFER,
					httpHeaders,
					EmptyHttpHeaders.INSTANCE
				);
				break;
			case UPDATE:
				final var content = taggingContentInput.get();
				final var contentBytes = content.getBytes();
				httpRequest = new DefaultFullHttpRequest(
					HTTP_1_1,
					PUT,
					uri,
					Unpooled.wrappedBuffer(contentBytes),
					httpHeaders,
					EmptyHttpHeaders.INSTANCE
				);
				httpHeaders.set(HttpHeaderNames.CONTENT_LENGTH, contentBytes.length);
				final var contentMd5 = THREAD_LOCAL_MD5.get().digest(contentBytes);
				httpHeaders.set(HttpHeaderNames.CONTENT_MD5, BASE64_ENCODER.encodeToString(contentMd5));
				break;
			case DELETE:
				httpRequest = new DefaultFullHttpRequest(
					HTTP_1_1,
					DELETE,
					uri,
					Unpooled.EMPTY_BUFFER,
					httpHeaders,
					EmptyHttpHeaders.INSTANCE
				);
				break;
			default:
				throw new AssertionError(stepId + ": object tagging is not supported for " + opType);
		}
		applyMetaDataHeaders(httpHeaders);
		applyDynamicHeaders(httpHeaders);
		applySharedHeaders(httpHeaders);
		applyAuthHeaders(httpHeaders, httpRequest.method(), uri, op.credential());
		return httpRequest;
	}

	@Override
	public void complete(final Channel channel, final O op) {
		if (channel != null && op instanceof CompositeDataOperation) {
			final var compositeOp = (CompositeDataOperation) op;
			if (compositeOp.allSubOperationsDone()) {
				Loggers.MULTIPART.info(
								"{},{},{}",
								compositeOp.item().name(),
								compositeOp.get(S3Api.KEY_UPLOAD_ID),
								compositeOp.latency());
			} else {
				final var uploadId = channel.attr(S3Api.KEY_ATTR_UPLOAD_ID).get();
				if (uploadId == null) {
					op.status(Operation.Status.RESP_FAIL_NOT_FOUND);
				} else {
					// multipart upload has been initialized as a result of this load operation
					compositeOp.put(S3Api.KEY_UPLOAD_ID, uploadId);
				}
			}
		}
		super.complete(channel, op);
	}

	@Override
	protected void appendHandlers(final Channel channel) {
		super.appendHandlers(channel);
		channel.pipeline().addLast(new S3ResponseHandler<>(this, verifyFlag, versioning));
	}

	@Override
	protected final void applyCopyHeaders(final HttpHeaders httpHeaders, final String srcPath)
					throws URISyntaxException {
		httpHeaders.set(S3Api.KEY_X_AMZ_COPY_SOURCE, srcPath);
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
					final HttpHeaders httpHeaders,
					final HttpMethod httpMethod,
					final String dstUriPath,
					final Credential credential) {
		final String uid;
		final String secret;
		if (credential != null) {
			uid = credential.getUid();
			secret = credential.getSecret();
		} else if (this.credential != null) {
			uid = this.credential.getUid();
			secret = this.credential.getSecret();
		} else {
			return;
		}
		if (uid == null || secret == null) {
			return;
		}

		try {
			if (authVersion == 2) {
				final var mac = MAC_BY_SECRET.get().computeIfAbsent(secret, GET_MAC_BY_SECRET);
				final var canonicalForm = getCanonical(httpHeaders, httpMethod, dstUriPath);
				final var sigData = mac.doFinal(canonicalForm.getBytes());
				httpHeaders.set(
						HttpHeaderNames.AUTHORIZATION,
						S3Api.AUTH_PREFIX + uid + ':' + BASE64_ENCODER.encodeToString(sigData));
			} else if (authVersion == 4) {
				// Remove port from the host
				httpHeaders.set(HttpHeaderNames.HOST, httpHeaders.get(HttpHeaderNames.HOST).split(":")[0]);
				
				// Set dates
				Date now = new Date();
				httpHeaders.set(HttpHeaderNames.DATE, DateUtil.FMT_DATE_RFC1123.format(now));
				httpHeaders.set(S3Api.AMZ_DATE_HEADER, DateUtil.FMT_DATE_AMAZON.format(now));
				
				// Set payload header
				if (Integer.valueOf(httpHeaders.get(HttpHeaderNames.CONTENT_LENGTH)) > 0) {
					httpHeaders.set(S3Api.AMZ_PAYLOAD_HEADER, S3Api.AMZ_UNSIGNED_PAYLOAD);
				} else {
					httpHeaders.set(S3Api.AMZ_PAYLOAD_HEADER, S3Api.AMZ_EMPTY_BODY_SHA256);
				}
				
				final var datetime = httpHeaders.get(S3Api.AMZ_DATE_HEADER);
				final var date = datetime.substring(0,8); // 8 chars as Pattern("yyyyMMdd") goes
				
				final var signingKeyCreateFunction = signingKeyCreateFunctionCache.get().
						computeIfAbsent(secret, SigningKeyCreateFunctionImpl::new);
				final var signingKey  = signingKeyCache.get().computeIfAbsent(date, signingKeyCreateFunction);
				final Map<String, String> sortedHeaders = getNonCanonicalHeaders(httpHeaders);
				final var canonicalForm = getCanonicalV4(httpHeaders, sortedHeaders, httpMethod, dstUriPath);
				byte[] encodedhash = THREAD_LOCAL_SHA256.get().digest(
						canonicalForm.getBytes(StandardCharsets.UTF_8));
				String stringToSign = "AWS4-HMAC-SHA256\n" + datetime + "\n" + date
						+ "/" + awsRegion + "/s3/aws4_request\n" + bytesToHex(encodedhash);
				final var sigData = HmacSHA256(stringToSign, signingKey);
				// 240 is an educated guess based on how v4 auth header looks generally
				StringBuilder sb = new StringBuilder(240);
				sb.append(S3Api.AUTH_V4_PREFIX)
						.append("Credential=")
						.append(uid)
						.append('/')
						.append(date)
						.append("/")
						.append(awsRegion)
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
				if (headerName.startsWith(S3Api.PREFIX_KEY_X_AMZ)) {
					sortedHeaders.put(headerName, header.getValue());
				}
			}
		}
		for (final var header : httpHeaders) {
			headerName = header.getKey().toLowerCase();
			if (headerName.startsWith(S3Api.PREFIX_KEY_X_AMZ)) {
				sortedHeaders.put(headerName, header.getValue());
			}
		}
		return sortedHeaders;
	}
	protected String getCanonicalV4(final HttpHeaders httpHeaders, final Map<String, String> sortedNonCanonicalHeaders,
									final HttpMethod httpMethod, final String dstUriPath) {
		final var buffCanonical = BUFF_CANONICAL.get();
		buffCanonical.setLength(0); // reset/clear
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
		for (final var header : S3Api.HEADERS_CANONICAL_V4) {
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
			buffCanonical.append(S3Api.AMZ_UNSIGNED_PAYLOAD);
		} else {
			buffCanonical.append(S3Api.AMZ_EMPTY_BODY_SHA256);
		}
		
		if (Loggers.MSG.isTraceEnabled()) {
			Loggers.MSG.trace("Canonical representation:\n{}", buffCanonical);
		}
		return buffCanonical.toString();
	}

	protected String getCanonical(
					final HttpHeaders httpHeaders, final HttpMethod httpMethod, final String dstUriPath) {
		final var buffCanonical = BUFF_CANONICAL.get();
		buffCanonical.setLength(0); // reset/clear
		buffCanonical.append(httpMethod.name());
		for (final var headerName : S3Api.HEADERS_CANONICAL) {
			if (httpHeaders.contains(headerName)) {
				for (final var headerValue : httpHeaders.getAll(headerName)) {
					buffCanonical.append('\n').append(headerValue);
				}
			} else if (sharedHeaders != null && sharedHeaders.contains(headerName)) {
				buffCanonical.append('\n').append(sharedHeaders.get(headerName));
			} else {
				buffCanonical.append('\n');
			}
		}
		// x-amz-*
		final Map<String, String> sortedHeaders = getNonCanonicalHeaders(httpHeaders);
		for (final var sortedHeader : sortedHeaders.entrySet()) {
			buffCanonical
							.append('\n')
							.append(sortedHeader.getKey())
							.append(':')
							.append(sortedHeader.getValue());
		}
		buffCanonical.append('\n');
		buffCanonical.append(dstUriPath);
		if (Loggers.MSG.isTraceEnabled()) {
			Loggers.MSG.trace("Canonical representation:\n{}", buffCanonical);
		}
		return buffCanonical.toString();
	}

	@Override
	public String toString() {
		return String.format(super.toString(), "s3");
	}
}
