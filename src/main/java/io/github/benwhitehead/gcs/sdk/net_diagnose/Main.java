package io.github.benwhitehead.gcs.sdk.net_diagnose;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.GenericJson;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.gax.rpc.PermissionDeniedException;
import com.google.api.services.dns.Dns;
import com.google.api.services.dns.model.ManagedZone;
import com.google.api.services.dns.model.ManagedZonePrivateVisibilityConfig;
import com.google.api.services.dns.model.ManagedZonesListResponse;
import com.google.api.services.dns.model.ResourceRecordSet;
import com.google.api.services.dns.model.ResourceRecordSetsListResponse;
import com.google.cloud.ReadChannel;
import com.google.cloud.compute.v1.Firewall;
import com.google.cloud.compute.v1.FirewallsClient;
import com.google.cloud.compute.v1.GetNetworkRequest;
import com.google.cloud.compute.v1.ListFirewallsRequest;
import com.google.cloud.compute.v1.ListSubnetworksRequest;
import com.google.cloud.compute.v1.Network;
import com.google.cloud.compute.v1.NetworksClient;
import com.google.cloud.compute.v1.Subnetwork;
import com.google.cloud.compute.v1.SubnetworksClient;
import com.google.cloud.compute.v1.SubnetworksClient.ListPagedResponse;
import com.google.cloud.dns.DnsOptions;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.resourcemanager.v3.FoldersClient;
import com.google.cloud.resourcemanager.v3.GetProjectRequest;
import com.google.cloud.resourcemanager.v3.Organization;
import com.google.cloud.resourcemanager.v3.OrganizationsClient;
import com.google.cloud.resourcemanager.v3.Project;
import com.google.cloud.resourcemanager.v3.ProjectName;
import com.google.cloud.resourcemanager.v3.ProjectsClient;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobReadSession;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.GrpcStorageOptions;
import com.google.cloud.storage.HttpStorageOptions;
import com.google.cloud.storage.ReadProjectionConfigs;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BucketField;
import com.google.cloud.storage.Storage.BucketGetOption;
import com.google.cloud.storage.StorageOptions;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import com.google.identity.accesscontextmanager.v1.AccessContextManagerClient;
import com.google.identity.accesscontextmanager.v1.AccessContextManagerClient.ListAccessPoliciesPagedResponse;
import com.google.identity.accesscontextmanager.v1.AccessContextManagerClient.ListServicePerimetersPagedResponse;
import com.google.identity.accesscontextmanager.v1.AccessPolicy;
import com.google.identity.accesscontextmanager.v1.ListAccessPoliciesRequest;
import com.google.identity.accesscontextmanager.v1.ListServicePerimetersRequest;
import com.google.identity.accesscontextmanager.v1.ServicePerimeter;
import com.google.protobuf.Message;
import com.google.protobuf.TextFormat;
import com.google.protobuf.TextFormat.Printer;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.ScatteringByteChannel;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.text.StringEscapeUtils;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Main {
  static {
    org.slf4j.bridge.SLF4JBridgeHandler.removeHandlersForRootLogger();
    org.slf4j.bridge.SLF4JBridgeHandler.install();
  }
  private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);
  public static final Printer PRINTER = TextFormat.printer();

  private static final double NANOS_PER_SECOND = Duration.ofSeconds(1).toNanos();
  private static final BiConsumer<Storage, BlobId> readChannel = (s, id) -> {
    try (ReadChannel reader = s.reader(id)) {
      reader.setChunkSize(0);
      ByteStreams.copy(reader, Channels.newChannel(ByteStreams.nullOutputStream()));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  };
  private static final BiConsumer<Storage, BlobId> blobReadSessionChannel = (s, id) -> {
    try (BlobReadSession session = s.blobReadSession(id).get(5, TimeUnit.SECONDS)) {
      try (ScatteringByteChannel reader = session.readAs(ReadProjectionConfigs.asChannel())) {
        ByteStreams.copy(reader, Channels.newChannel(ByteStreams.nullOutputStream()));
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  };

  @SuppressWarnings("OptionalGetWithoutIsPresent")
  public static void main(String[] args) throws Exception {
    now();
    LOGGER.info("args = {}", Arrays.toString(args));
    if (args.length < 1) {
      System.err.println("Usage: ... <gsutil_uri>");
      System.exit(1);
    }

    reportEnvVar("CLOUDSDK_CONFIG");
    reportEnvVar("GOOGLE_APPLICATION_CREDENTIALS");
    reportJavaSystemProperties();
    MDS.all(LOGGER::info);

    reportSubnetworks();
    reportPerimeter();

    BlobId id = BlobId.fromGsUtilUri(args[0]);
    LOGGER.info("gsutil_uri = {}", id.toGsUtilUriWithGeneration());

    reportGcsObject(id);
    now();
    LOGGER.info("\n\n");
  }

  private static void reportEnvVar(String var) {
    LOGGER.info("env.{} = {}", var, System.getenv(var));
  }

  private static void reportGcsObject(BlobId id) {
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
        runForStorageInstance(storage, id, readChannel);
      } catch (Exception e) {
        LOGGER.warn("Error while probing via JSON", e);
      }
    }

    {
      now();
      GrpcStorageOptions options = StorageOptions.grpc()
          .setGrpcInterceptorProvider(() -> ImmutableList.of(new RemoteAddrLoggingInterceptor()))
          .setAttemptDirectPath(false)
          .build();
      try (Storage storage = options.getService()) {
        LOGGER.info("--- gRPC+CFE (ReadObject) -----");
        runForStorageInstance(storage, id, readChannel);
      } catch (Exception e) {
        LOGGER.warn("Error while probing via gRPC+CFE via ReadObject API", e);
      }
    }

    {
      now();
      GrpcStorageOptions options = StorageOptions.grpc()
          .setGrpcInterceptorProvider(() -> ImmutableList.of(new RemoteAddrLoggingInterceptor()))
          .build();
      try (Storage storage = options.getService()) {
        LOGGER.info("--- gRPC+DP (ReadObject) ------");
        runForStorageInstance(storage, id, readChannel);
      } catch (Exception e) {
        LOGGER.warn("Error while probing via gRPC+DP via ReadObject API", e);
      }
    }

    logPde("Error while probing via gRPC+DP via BidiReadObject API", () -> {
      now();
      GrpcStorageOptions options = StorageOptions.grpc()
          .setGrpcInterceptorProvider(() -> ImmutableList.of(new RemoteAddrLoggingInterceptor()))
          .build();
      try (Storage storage = options.getService()) {
        LOGGER.info("--- gRPC+DP (BidiReadObject) --");
        runForStorageInstance(storage, id, blobReadSessionChannel);
      } catch (RuntimeException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
  }

  @SuppressWarnings("OptionalGetWithoutIsPresent")
  private static void reportSubnetworks() throws IOException {
    logPde("Error while attempting to probe subnetwork configuration", () -> {
      try (
          NetworksClient net = NetworksClient.create();
          SubnetworksClient sub = SubnetworksClient.create();
          FirewallsClient firewalls = FirewallsClient.create()
      ) {
        String projectNumber = MDS.projectNumber().get();
        String zone = MDS.zone().get();
        String region = zone.substring(zone.lastIndexOf("/") + 1, zone.lastIndexOf("-"));

        List<String> networks = new ArrayList<>();
        {
          int i = 0;
          while (true) {
            String path = "/computeMetadata/v1/instance/network-interfaces/" + i + "/network";
            Optional<String> got = MDS.get(path);
            if (got.isPresent()) {
              networks.add(got.get());
            } else {
              break;
            }
            i++;
          }
        }

        for (int i = 0; i < networks.size(); i++) {
          String networkResourceName = networks.get(i);
          String networkName = networkResourceName.substring(networkResourceName.lastIndexOf("/") + 1);
          GetNetworkRequest nReq = GetNetworkRequest.newBuilder()
              .setProject(projectNumber)
              .setNetwork(networkName)
              .build();
          Network network = net.get(nReq);

          String networkPrefix = String.format(Locale.US, "network[%d]", i);
          log(networkPrefix, network);

          {
            ListFirewallsRequest lfReq = ListFirewallsRequest.newBuilder()
                .setProject(projectNumber)
                // mimic what compute firewall-rules list --filter='network=default' does
                // use eq instead of =
                // wrap the search term in a regex
                .setFilter(String.format(Locale.US, "network eq \".*\\b%s\\b.*\"", networkName))
                .build();
            FirewallsClient.ListPagedResponse list = firewalls.list(lfReq);
            List<Firewall> all = StreamSupport.stream(list.iterateAll().spliterator(), false)
                .collect(Collectors.toList());
            for (int j = 0; j < all.size(); j++) {
              Firewall firewall = all.get(j);
              log(String.format(Locale.US, "%s.firewall[%d]", networkPrefix, j), firewall);
            }
          }

          // report subnetworks
          {
            ListSubnetworksRequest lsReq = ListSubnetworksRequest.newBuilder()
                .setProject(projectNumber)
                .setRegion(region)
                .setFilter(String.format(Locale.US, "name=%s", networkName))
                .build();
            ListPagedResponse list = sub.list(lsReq);
            List<Subnetwork> all = StreamSupport.stream(list.iterateAll().spliterator(), false)
                .collect(Collectors.toList());
            for (int j = 0; j < all.size(); j++) {
              Subnetwork subnetwork = all.get(j);
              log(String.format(Locale.US, "%s.subnetwork[%d]", networkPrefix, j), subnetwork);
            }
          }

          // report dns-zones
          {
            DnsOptions options = DnsOptions.getDefaultInstance();
            HttpTransportOptions transportOptions = (HttpTransportOptions) options.getTransportOptions();
            HttpTransport transport = transportOptions.getHttpTransportFactory().create();
            HttpRequestInitializer initializer = transportOptions.getHttpRequestInitializer(options);
            Dns dns = new Dns.Builder(transport, new JacksonFactory(), initializer)
                .setRootUrl(options.getHost())
                .setApplicationName(options.getApplicationName())
                .build();

            String npt;
            int jj = 0;
            do {
              ManagedZonesListResponse zones = dns.managedZones().list(projectNumber).execute();
              npt = zones.getNextPageToken();

              List<ManagedZone> managedZones = zones.getManagedZones();
              for (ManagedZone mz : managedZones) {
                boolean anyMatch = Optional.ofNullable(mz.getPrivateVisibilityConfig())
                    .map(ManagedZonePrivateVisibilityConfig::getNetworks)
                    .stream()
                    .flatMap(List::stream)
                    .anyMatch(n -> n.getNetworkUrl().contains(networkName));
                if (anyMatch) {
                  String mzPrefix = String.format(Locale.US, "%s.dns-managed-zone[%d]",
                      networkPrefix, jj++);
                  log(mzPrefix, mz);

                  String rrsNpt;
                  int k = 0;
                  do {
                    ResourceRecordSetsListResponse rrsList = dns.resourceRecordSets()
                        .list(projectNumber, mz.getName())
                        .execute();
                    rrsNpt = rrsList.getNextPageToken();
                    for (int l = 0; l < rrsList.getRrsets().size(); k++, l++) {
                      ResourceRecordSet rrs = rrsList.getRrsets().get(k);
                      String rrsPrefix = String.format(Locale.US, "%s.rrs[%d]", mzPrefix, k);
                      log(rrsPrefix, rrs);
                    }
                  } while (rrsNpt != null);
                }
              }
            } while (npt != null);
          }
        }
      }
    });
  }

  private static void reportPerimeter() throws IOException {
    logPde("Error while attempting to probe project and org access policy configuration", () -> {
      try (
          AccessContextManagerClient acm = AccessContextManagerClient.create();
          ProjectsClient projects = ProjectsClient.create();
          FoldersClient folders = FoldersClient.create();
          OrganizationsClient orgs = OrganizationsClient.create()
      ) {
        GetProjectRequest pReq = GetProjectRequest.newBuilder()
            .setName(ProjectName.format(MDS.projectNumber().get()))
            .build();
        Project project = projects.getProject(pReq);
        log("project", project);
        String orgid;
        if (project.getParent().startsWith("organizations/")) {
          orgid = project.getParent();
        } else {
          // TODO: walk up folder
          throw new IllegalStateException("Unable to traverse project folders at this time");
        }
        logPde("Error attempting to prob organization configuration", () -> {
          // resourcemanager.organizations.get
          Organization organization = orgs.getOrganization(orgid);
          log("organization", organization);
        });
        ListAccessPoliciesRequest apReq = ListAccessPoliciesRequest.newBuilder()
            .setParent(orgid)
            .build();
        ListAccessPoliciesPagedResponse apPage = acm.listAccessPolicies(apReq);
        List<AccessPolicy> apAll = StreamSupport.stream(apPage.iterateAll().spliterator(), false)
            .collect(Collectors.toList());

        for (int i = 0; i < apAll.size(); i++) {
          AccessPolicy ap = apAll.get(i);
          String apPrefix = String.format(Locale.US, "accessPolicies[%d]", i);
          log(apPrefix, ap);
          ListServicePerimetersRequest spReq = ListServicePerimetersRequest.newBuilder()
              .setParent(ap.getName())
              .build();
          ListServicePerimetersPagedResponse spPage = acm.listServicePerimetersPagedCallable()
              .call(spReq);
          List<ServicePerimeter> spAll = StreamSupport.stream(spPage.iterateAll().spliterator(), false)
              .collect(Collectors.toList());
          for (int j = 0; j < spAll.size(); j++) {
            ServicePerimeter perimeter = spAll.get(j);
            log(String.format(Locale.US, "%s.perimeter[%d]", apPrefix, j), perimeter);
          }
        }
      }
    });
  }

  @FunctionalInterface
  interface ERunnable<E extends Exception> {
    void run() throws E;
  }

  private static <E extends Exception> void logPde(String description, ERunnable<E> r)  throws E {
    try {
      r.run();
    } catch (PermissionDeniedException pde) {
      LOGGER.warn("{}: {}", description, pde.getCause().getMessage());
    } catch (Throwable t) {
      PermissionDeniedException cause = findCause(t, PermissionDeniedException.class);
      if (cause != null) {
        LOGGER.warn("{}: {}", description, cause.getCause().getMessage());
        return;
      }
      GoogleJsonResponseException gjre = findCause(t, GoogleJsonResponseException.class);
      if (gjre != null && gjre.getStatusCode() == 403) {
        LOGGER.warn("{}: {}", description, gjre.getMessage());
        return;
      }
      throw t;
    }
  }

  @Nullable
  private static <T extends Throwable, C extends Class<T>> T findCause(Throwable t, C c) {
    if (t == null) {
      return null;
    }

    if (c.isAssignableFrom(t.getClass())) {
      return c.cast(t);
    } else {
      return findCause(t.getCause(), c);
    }
  }


  private static void reportJavaSystemProperties() {
    TreeMap<String, String> properties = new TreeMap<>();
    System.getProperties().forEach((k, v) -> properties.put((String) k, (String) v));
    for (Entry<String, String> e : properties.entrySet()) {
      LOGGER.info("-D{}={}", e.getKey(), StringEscapeUtils.escapeJava(e.getValue()));
    }
  }

  private static void runForStorageInstance(Storage storage, BlobId id, BiConsumer<Storage, BlobId> c) {
    DescriptiveStatistics latencyStats = new DescriptiveStatistics();
    for (int i = 1; i <= 5; i++) {
      long begin = System.nanoTime();
      c.accept(storage, id);
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

  private static void log(String prefix, Message message) {
    String[] lines = PRINTER.printToString(message).split("\n");
    logLines(prefix, lines);
  }

  private static void log(String prefix, GenericJson message) throws IOException {
    String[] lines = message.toPrettyString().split("\n");
    logLines(prefix, lines);
  }

  private static void logLines(String prefix, String[] lines) {
    for (int i = 0; i < lines.length; i++) {
      String line = lines[i];
      if (i == 0) {
        LOGGER.info("{}: {", prefix);
      }
      LOGGER.info("   {}", line);
      if (i == lines.length - 1) {
        LOGGER.info("}");
      }
    }
  }
}
