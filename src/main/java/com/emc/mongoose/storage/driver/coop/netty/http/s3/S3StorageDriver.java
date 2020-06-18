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
import com.emc.mongoose.base.item.op.partial.data.PartialDataOperation;
import com.emc.mongoose.base.logging.LogUtil;
import com.emc.mongoose.base.logging.Loggers;
import com.emc.mongoose.base.storage.Credential;
import com.emc.mongoose.storage.driver.coop.netty.http.HttpStorageDriverBase;
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
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;
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

	protected final boolean fsAccess;
	protected final boolean taggingEnabled;
	protected final Input<String> taggingContentInput;
	protected final boolean versioning;

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
		requestAuthTokenFunc = null; // do not use
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
		channel.pipeline().addLast(new S3ResponseHandler<>(this, verifyFlag));
	}

	@Override
	protected final void applyCopyHeaders(final HttpHeaders httpHeaders, final String srcPath)
					throws URISyntaxException {
		httpHeaders.set(S3Api.KEY_X_AMZ_COPY_SOURCE, srcPath);
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
		final var mac = MAC_BY_SECRET.get().computeIfAbsent(secret, GET_MAC_BY_SECRET);
		final var canonicalForm = getCanonical(httpHeaders, httpMethod, dstUriPath);
		final var sigData = mac.doFinal(canonicalForm.getBytes());
		httpHeaders.set(
						HttpHeaderNames.AUTHORIZATION,
						S3Api.AUTH_PREFIX + uid + ':' + BASE64_ENCODER.encodeToString(sigData));
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
