package io.github.benwhitehead.gcs.sdk.net_diagnose;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpResponseException;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.io.CharStreams;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;

final class MDS {

  private static final Supplier<HttpRequestFactory> requestFactory =
      Suppliers.memoize(
          () ->
              new NetHttpTransport.Builder()
                  .build()
                  .createRequestFactory(
                      request -> {
                        request.setCurlLoggingEnabled(false);
                        request.getHeaders().set("Metadata-Flavor", "Google");
                      }));
  private static final String baseUri = "http://metadata.google.internal";

  public static Optional<String> zone() throws IOException {
    return get("/computeMetadata/v1/instance/zone");
  }

  public static Optional<String> projectNumber() throws IOException {
    return get("/computeMetadata/v1/project/numeric-project-id");
  }

  public static void all(Consumer<String> c) throws IOException {
    try {
      walk("/", c);
    } catch (UnknownHostException ignore) {
      // ignore
    }
  }

  static void walk(String path, Consumer<String> c) throws IOException {
    List<String> strings = getAsString(path);

    for (String string : strings) {
      if (string.endsWith("/")) {
        walk(path + string, c);
      } else {
        leaf(path + string, c);
      }
    }
  }

  static void leaf(String path, Consumer<String> c) throws IOException {
    List<String> lines = getAsString(path);
    if (lines.size() == 1) {
      report(path, -1, lines.get(0), c);
    } else if (lines.size() > 1) {
      for (int i = 0; i < lines.size(); i++) {
        report(path, i, lines.get(i), c);
      }
    }
  }

  static void report(String path, int lineNumber, String s, Consumer<String> c) {
    final String value;
    if (path.endsWith("/ssh-keys")) {
      value = s.substring(0, s.indexOf(' '))
          + " <public_key> "
          + s.substring(s.lastIndexOf(' ') + 1);
    } else if (path.endsWith("/token")) {
      int begin = s.indexOf("access_token") + 35;
      value = s.substring(0, begin)
          + " <snip> "
          + s.substring(s.indexOf("\"", begin));
    } else {
      value = s;
    }

    if (lineNumber >= 0) {
      c.accept(String.format(Locale.US, "%s:%d=%s", path, lineNumber, value));
    } else {
      c.accept(String.format(Locale.US, "%s=%s", path, value));
    }
  }

  static List<String> getAsString(String path) throws IOException {
    GenericUrl url = new GenericUrl(baseUri + path);
    try {
      HttpRequest req = requestFactory.get().buildGetRequest(url);
      HttpResponse resp = req.execute();
      try (InputStream content = resp.getContent();
          Reader r = new InputStreamReader(content)) {
        return CharStreams.readLines(r).stream().collect(ImmutableList.toImmutableList());
      }
    } catch (HttpResponseException e) {
      return ImmutableList.of();
    }
  }

  static Optional<String> get(String path) throws IOException {
    GenericUrl url = new GenericUrl(baseUri + path);
    try {
      HttpRequest req = requestFactory.get().buildGetRequest(url);
      HttpResponse resp = req.execute();
      try (InputStream content = resp.getContent();
          Reader r = new InputStreamReader(content)) {
        return CharStreams.readLines(r).stream().findFirst();
      }
    } catch (HttpResponseException | UnknownHostException e) {
      return Optional.empty();
    }
  }


}
