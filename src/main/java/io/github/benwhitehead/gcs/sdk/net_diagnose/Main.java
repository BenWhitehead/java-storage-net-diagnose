package io.github.benwhitehead.gcs.sdk.net_diagnose;

import com.google.cloud.ReadChannel;
import com.google.cloud.compute.v1.Subnetwork;
import com.google.cloud.compute.v1.SubnetworksClient;
import com.google.cloud.compute.v1.SubnetworksClient.ListPagedResponse;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.GrpcStorageOptions;
import com.google.cloud.storage.HttpStorageOptions;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BucketField;
import com.google.cloud.storage.Storage.BucketGetOption;
import com.google.cloud.storage.StorageOptions;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import java.io.IOException;
import java.nio.channels.Channels;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.text.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Main {
  static {
    org.slf4j.bridge.SLF4JBridgeHandler.removeHandlersForRootLogger();
    org.slf4j.bridge.SLF4JBridgeHandler.install();
  }
  private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

  private static final double NANOS_PER_SECOND = Duration.ofSeconds(1).toNanos();

  @SuppressWarnings("OptionalGetWithoutIsPresent")
  public static void main(String[] args) throws Exception {
    now();
    LOGGER.info("args = {}", Arrays.toString(args));
    if (args.length < 1) {
      System.err.println("Usage: ... <gsutil_uri>");
      System.exit(1);
    }

    LOGGER.info("env.GOOGLE_APPLICATION_CREDENTIALS = {}", System.getenv("GOOGLE_APPLICATION_CREDENTIALS"));
    TreeMap<String, String> properties = new TreeMap<>();
    System.getProperties().forEach((k, v) -> properties.put((String) k, (String) v));
    for (Entry<String, String> e : properties.entrySet()) {
      LOGGER.info("-D{}={}", e.getKey(), StringEscapeUtils.escapeJava(e.getValue()));
    }
    MDS.all(LOGGER::info);

    try (SubnetworksClient sub = SubnetworksClient.create()) {
      String projectNumber = MDS.projectNumber().get();
      String zone = MDS.zone().get();
      String region = zone.substring(zone.lastIndexOf("/") + 1, zone.lastIndexOf("-"));
      ListPagedResponse list = sub.list(projectNumber, region);
      List<Subnetwork> all = StreamSupport.stream(list.iterateAll().spliterator(), false)
          .collect(Collectors.toList());
      for (int i = 0; i < all.size(); i++) {
        Subnetwork subnetwork = all.get(i);
        log(String.format(Locale.US, "subnetwork[%d]", i), subnetwork);
      }
    }

    BlobId id = BlobId.fromGsUtilUri(args[0]);
    LOGGER.info("gsutil_uri = {}", id.toGsUtilUriWithGeneration());

    {
      now();
      HttpStorageOptions options = StorageOptions.http().build();
      try (Storage storage = options.getService()) {
        Bucket bucket = storage.get(id.getBucket(),
            BucketGetOption.fields(
                BucketField.NAME,
                BucketField.LOCATION,
                BucketField.CUSTOM_PLACEMENT_CONFIG,
                BucketField.HIERARCHICAL_NAMESPACE,
                BucketField.STORAGE_CLASS
            )
        );
        LOGGER.info("bucket.location = {}", bucket.getLocation());
        LOGGER.info("bucket.customPlacementConfig = {}", bucket.getCustomPlacementConfig());
        LOGGER.info("bucket.hierarchicalNamespace = {}", bucket.getHierarchicalNamespace());
        LOGGER.info("bucket.storageClass = {}", bucket.getStorageClass());

        LOGGER.info("--- JSON ----------------------");
        runForStorageInstance(storage, id);
      }
    }

    {
      now();
      GrpcStorageOptions options = StorageOptions.grpc()
          .setGrpcInterceptorProvider(() -> ImmutableList.of(new RemoteAddrLoggingInterceptor()))
          .setAttemptDirectPath(false)
          .build();
      try (Storage storage = options.getService()) {
        LOGGER.info("--- gRPC+CFE ------------------");
        runForStorageInstance(storage, id);
      }
    }

    {
      now();
      GrpcStorageOptions options = StorageOptions.grpc()
          .setGrpcInterceptorProvider(() -> ImmutableList.of(new RemoteAddrLoggingInterceptor()))
          .build();
      try (Storage storage = options.getService()) {
        LOGGER.info("--- gRPC+DP -------------------");
        runForStorageInstance(storage, id);
      }
    }
    now();
    LOGGER.info("\n\n");
  }

  private static void runForStorageInstance(Storage storage, BlobId id) throws IOException {
    DescriptiveStatistics latencyStats = new DescriptiveStatistics();
    for (int i = 1; i <= 5; i++) {
      long begin = System.nanoTime();
      try (ReadChannel reader = storage.reader(id)) {
        reader.setChunkSize(0);
        ByteStreams.copy(reader, Channels.newChannel(ByteStreams.nullOutputStream()));
      }
      long end = System.nanoTime();
      long delta = end - begin;
      double seconds = delta / NANOS_PER_SECOND;
      latencyStats.addValue(seconds);
      LOGGER.info(String.format(Locale.US, "read %02d duration % ,2.3fs", i, seconds));
    }
    LOGGER.info(String.format(
        "p50 = % ,2.3fs p90 = % ,2.3fs p99 = % ,2.3fs",
        latencyStats.getPercentile(50.0),
        latencyStats.getPercentile(90.0),
        latencyStats.getPercentile(99.0)
    ));
  }

  private static void now() {
    LOGGER.info("{}", Instant.now().atOffset(ZoneOffset.UTC));
  }
  
  private static void log(String prefix, Subnetwork subnetwork) {
    LOGGER.info("{}.creationTimestamp: {}",        prefix, subnetwork.getCreationTimestamp());
    LOGGER.info("{}.fingerprint: {}",              prefix, subnetwork.getFingerprint());
    LOGGER.info("{}.gatewayAddress: {}",           prefix, subnetwork.getGatewayAddress());
    LOGGER.info("{}.id: {}",                       prefix, subnetwork.getId());
    LOGGER.info("{}.ipCidrRange: {}",              prefix, subnetwork.getIpCidrRange());
    LOGGER.info("{}.ipv6CidrRange: {}",            prefix, subnetwork.getIpv6CidrRange());
    LOGGER.info("{}.kind: {}",                     prefix, subnetwork.getKind());
    LOGGER.info("{}.name: {}",                     prefix, subnetwork.getName());
    LOGGER.info("{}.network: {}",                  prefix, subnetwork.getNetwork());
    LOGGER.info("{}.privateIpGoogleAccess: {}",    prefix, subnetwork.getPrivateIpGoogleAccess());
    LOGGER.info("{}.privateIpv6GoogleAccess: {}",  prefix, subnetwork.getPrivateIpv6GoogleAccess());
    LOGGER.info("{}.purpose: {}",                  prefix, subnetwork.getPurpose());
    LOGGER.info("{}.region: {}",                   prefix, subnetwork.getRegion());
    LOGGER.info("{}.selfLink: {}",                 prefix, subnetwork.getSelfLink());
    LOGGER.info("{}.stackType: {}",                prefix, subnetwork.getStackType());
  }
}
