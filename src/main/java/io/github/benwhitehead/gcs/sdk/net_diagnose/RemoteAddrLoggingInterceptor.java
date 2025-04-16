package io.github.benwhitehead.gcs.sdk.net_diagnose;

import io.grpc.Attributes;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import io.grpc.ForwardingClientCallListener.SimpleForwardingClientCallListener;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class RemoteAddrLoggingInterceptor implements ClientInterceptor {
  private static final Object SENTINEL = new Object();

  private static final Logger LOGGER = LoggerFactory.getLogger(RemoteAddrLoggingInterceptor.class);

  private final Map<String, Object> seenHosts = Collections.synchronizedMap(new HashMap<>());

  RemoteAddrLoggingInterceptor() {}

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method,
      CallOptions callOptions, Channel next) {
    ClientCall<ReqT, RespT> call = next.newCall(method, callOptions);
    return new SimpleForwardingClientCall<>(call) {
      @Override
      public void start(Listener<RespT> responseListener, Metadata headers) {
        SimpleForwardingClientCallListener<RespT> listener =
            new SimpleForwardingClientCallListener<>(responseListener) {
              volatile boolean logged = false;

              @Override
              public void onHeaders(Metadata headers) {
                logRemoteAddr();
                super.onHeaders(headers);
              }

              @Override
              public void onClose(Status status, Metadata trailers) {
                logRemoteAddr();
                super.onClose(status, trailers);
              }

              private void logRemoteAddr() {
                if (logged) {
                  return;
                }
                Attributes attributes = getAttributes();
                SocketAddress socketAddress = attributes.get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR);
                logSocketAddress(socketAddress);
                logged = true;
              }
            };
        super.start(listener, headers);
      }
    };
  }

  private void logSocketAddress(SocketAddress socketAddress) {
    if (socketAddress instanceof InetSocketAddress) {
      InetSocketAddress inetSocketAddress = (InetSocketAddress) socketAddress;
      InetAddress address = inetSocketAddress.getAddress();
      String hostAddress = address.getHostAddress();
      String hostString =
          (
            hostAddress.contains(":")
                ? "[" + hostAddress + "]"
                : hostAddress
          ) + ":" + inetSocketAddress.getPort();
      Object prev = seenHosts.putIfAbsent(hostString, SENTINEL);
      if (prev == null) {
        LOGGER.info("io.grpc.Grpc.TRANSPORT_ATTR_REMOTE_ADDR = {}", hostString);
      }
    }
  }

}
