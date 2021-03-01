package com.lion.rpcbrave;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.lion.utility.framework.web.i.ILIB;
import com.lion.utility.framework.web.i.constant.IConstant;
import com.lion.utility.rpc.entity.RPCRequest;
import com.lion.utility.rpc.entity.RPCResponse;
import com.lion.utility.tool.file.JsonLIB;

import brave.Tracer;
import brave.Tracer.SpanInScope;
import brave.Tracing;
import brave.propagation.B3Propagation;
import brave.propagation.Propagation;
import brave.propagation.Propagation.Getter;
import brave.propagation.Propagation.Setter;
import brave.propagation.TraceContext;
import brave.propagation.TraceContext.Extractor;
import brave.propagation.TraceContext.Injector;
import brave.propagation.TraceContextOrSamplingFlags;
import brave.sampler.Sampler;
import brave.Span;
import brave.Span.Kind;
import zipkin2.codec.Encoding;
import zipkin2.codec.SpanBytesEncoder;
import zipkin2.reporter.AsyncReporter;
import zipkin2.reporter.Sender;
import zipkin2.reporter.brave.AsyncZipkinSpanHandler;
import zipkin2.reporter.brave.ZipkinSpanHandler;
import zipkin2.reporter.kafka.KafkaSender;

public class BraveKafkaTest2_1 {
	// 采样率，是否采样是一个up-front决定，意味着，是否采样是在trace第一次操作的时候就被决定的，并且这个决定会一直向下传播到整个trace；也就是一个trace要么整个被采样，要么整个不被采样
	private static float samplerProbability = 0.5f;
	private static String bootstrapServers = "192.168.2.133:9092";
	private static String topic = "zipkin";
	private static String serviceName = "client";

	private static Tracing tracing;
	private static Tracer tracer;
	private static Extractor<Map<String, String>> extractor;
	private static Injector<Map<String, String>> injector;

	private static Getter<Map<String, String>, String> GETTER = new Getter<Map<String, String>, String>() {
		@Override
		public String get(Map<String, String> carrier, String key) {
			return carrier.get(key);
		}

		@Override
		public String toString() {
			return "Map::get";
		}
	};

	private static Setter<Map<String, String>, String> SETTER = new Setter<Map<String, String>, String>() {
		@Override
		public void put(Map<String, String> carrier, String key, String value) {
			carrier.put(key, value);
		}

		@Override
		public String toString() {
			return "Map::set";
		}
	};

	/**
	 * 初始化（调用一次）
	 */
	public static void init() {
		// 创建发送者对象
		Sender sender = KafkaSender.newBuilder()
				.bootstrapServers(BraveKafkaTest2_1.bootstrapServers)
				.topic(BraveKafkaTest2_1.topic)
				.encoding(Encoding.JSON)
				.build();
		ZipkinSpanHandler zipkinSpanHandler = AsyncZipkinSpanHandler.create(sender);

		// 创建当前服务的链路跟踪对象
		BraveKafkaTest2_1.tracing = Tracing.newBuilder()
				.localServiceName(BraveKafkaTest2_1.serviceName)
				.addSpanHandler(zipkinSpanHandler)
				.sampler(Sampler.create(BraveKafkaTest2_1.samplerProbability))
				.build();

		// 实际使用的对象
		BraveKafkaTest2_1.tracer = tracing.tracer();

		// extractor：数据提取对象，用于在carrier中提取TraceContext相关信息/采样标记信息到TraceContextOrSamplingFlags中
		BraveKafkaTest2_1.extractor = tracing.propagation().extractor(GETTER);
		// injector：用于将TraceContext中的各种数据注入到carrier中，其中carrier是指数据传输中的载体
		BraveKafkaTest2_1.injector = tracing.propagation().injector(SETTER);

//		当关闭应用前，应关闭对象？
//		tracing.close();
//		zipkinSpanHandler.close();
//		sender.close();
	}

	/**
	 * rpc client 方法调用
	 * 
	 * @throws Exception
	 */
	public static RPCRequest rpcClientHandler() throws Exception {
		RPCRequest rpcRequest = new RPCRequest();
		rpcRequest.setMsgId(1);
		rpcRequest.setMethodId("aaa.hello.client");

		String remoteIp = "127.0.0.1";
		int remotePort = 8001;

		// rpcClient会从ThreadLocal中获取parent traceContext，
		// 为新生成的span指定traceId及parentId如果没有parent traceContext，则生成的span为root span
		Span span = BraveKafkaTest2_1.tracer.nextSpan();
		// 将span绑定的traceContext中的属性信息复制到rpcRequest附加属性中，便于后续将其传至rpcserver处
		BraveKafkaTest2_1.injector.inject(span.context(), rpcRequest.getAttachmentMap());

		// span.isNoop()若为true，则不会记录到zipkin
		if (!span.isNoop()) {
			// 记录接口信息及远程ip端口
			span.kind(Kind.CLIENT);
			span.name(BraveKafkaTest2_1.serviceName + "-" + rpcRequest.getMethodId());
			span.remoteIpAndPort(remoteIp, remotePort);
			span.start();
		}

		// 将创建的span作为当前span（可以通过tracer.currentSpan()访问到），并设置查询范围
		try (SpanInScope scope = BraveKafkaTest2_1.tracer.withSpanInScope(span)) {
			// 将请求参数写入到span中
			if (rpcRequest.getParams() != null) {
				span.tag("args", JsonLIB.toJson(rpcRequest.getParams()));
			} else {
				span.tag("args", "");
			}

			// --------------------远程方法请求-------------------- //
//			Result result = invoker.invoke(invocation);
			BraveKafkaTest2_2.rpcServerHandler(rpcRequest);
			RPCResponse rpcResponse = new RPCResponse();
			rpcResponse.setiResult(ILIB.getIResultSucceed());

			// 写入失败信息
			if (rpcResponse.getiResult().getCode() != IConstant.RETURN_CODE_SUCCEED) {
				span.tag("rpc.errorMsg", "[" + rpcResponse.getiResult().getCode() + "]" + rpcResponse.getiResult().getMsg());
			}
		} catch (Exception e) {
			span.tag("rpc.errorMsg", e.getMessage());
			throw e;
		} finally {
			// span处理完成，上报到zipkin
			span.finish();
		}

		return rpcRequest;
	}

	public static void main(String[] args) throws Exception {
		BraveKafkaTest2_1.init();
		BraveKafkaTest2_2.init();

		while (true) {
			BraveKafkaTest2_1.rpcClientHandler();

			Thread.sleep(5000);

		}
	}

}