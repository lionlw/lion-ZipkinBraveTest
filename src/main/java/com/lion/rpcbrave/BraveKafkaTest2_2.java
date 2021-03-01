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

public class BraveKafkaTest2_2 {
	// 采样率，是否采样是一个up-front决定，意味着，是否采样是在trace第一次操作的时候就被决定的，并且这个决定会一直向下传播到整个trace；也就是一个trace要么整个被采样，要么整个不被采样
	private static float samplerProbability = 0.5f;
	private static String bootstrapServers = "192.168.2.133:9092";
	private static String topic = "zipkin";
	private static String serviceName = "server";

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
				.bootstrapServers(BraveKafkaTest2_2.bootstrapServers)
				.topic(BraveKafkaTest2_2.topic)
				.encoding(Encoding.JSON)
				.build();
		ZipkinSpanHandler zipkinSpanHandler = AsyncZipkinSpanHandler.create(sender);

		// 创建当前服务的链路跟踪对象
		BraveKafkaTest2_2.tracing = Tracing.newBuilder()
				.localServiceName(BraveKafkaTest2_2.serviceName)
				.addSpanHandler(zipkinSpanHandler)
				.sampler(Sampler.create(BraveKafkaTest2_2.samplerProbability))
				.build();

		// 实际使用的对象
		BraveKafkaTest2_2.tracer = tracing.tracer();

		// extractor：数据提取对象，用于在carrier中提取TraceContext相关信息/采样标记信息到TraceContextOrSamplingFlags中
		BraveKafkaTest2_2.extractor = tracing.propagation().extractor(GETTER);
		// injector：用于将TraceContext中的各种数据注入到carrier中，其中carrier是指数据传输中的载体
		BraveKafkaTest2_2.injector = tracing.propagation().injector(SETTER);

//		当关闭应用前，应关闭对象？
//		tracing.close();
//		zipkinSpanHandler.close();
//		sender.close();
	}

	/**
	 * rpc server 方法执行后调用
	 */
	public static void rpcServerHandler(RPCRequest rpcRequest) throws Exception {
		// rpcServer，从rpcRequest附加属性中提取traceContext的属性信息
		TraceContextOrSamplingFlags extracted = extractor.extract(rpcRequest.getAttachmentMap());
		// 生成span, 兼容初次调用
		Span span = extracted.context() != null ? tracer.joinSpan(extracted.context()) : tracer.nextSpan(extracted);

		// span.isNoop()若为true，则不会记录到zipkin
		if (!span.isNoop()) {
			// 记录接口信息及远程ip端口
			span.kind(Kind.SERVER);
			span.name(BraveKafkaTest2_2.serviceName + "-" + rpcRequest.getMethodId());
			span.start();
		}

		// 将创建的span作为当前span（可以通过tracer.currentSpan()访问到），并设置查询范围
		try (SpanInScope scope = BraveKafkaTest2_2.tracer.withSpanInScope(span)) {
			// 将请求参数写入到span中
			if (rpcRequest.getParams() != null) {
				span.tag("args", JsonLIB.toJson(rpcRequest.getParams()));
			} else {
				span.tag("args", "");
			}

			// --------------------本地方法请求-------------------- //
//			Result result = invoker.invoke(invocation);
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
	}

}