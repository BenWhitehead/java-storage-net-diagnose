# java-storage-net-diagnose

## Prerequisites

### Java
1. jdk 11+
2. maven 3.x

### Cloud Permissions
1. `storage.buckets.get`
2. `storage.objects.get`
3. `compute.subnetworks.list`

## Build
```
mvn clean package
```

## Run
```bash
java -jar target/java-storage-net-diagnose-0.1.0-SNAPSHOT.jar gs://my-bucket/my-object
```

## Cross Run ipv{4,6}
```bash
printf '\-Djava.net.preferIPv4Stack=true\x00\-Djava.net.preferIPv6Stack=true' \
  | xargs -0 -I {} bash -c 'java {} -jar java-storage-net-diagnose-0.1.0-SNAPSHOT.jar gs://my-bucket/my-object ;'
```

## Report
Report information is written to stdout.

Some instance metadata can contain PII (Personally Identifiable Information) such as instance names, service account names, ssk-key names.

Do not share the contents of the report with untrusted parties.

## Disclaimer

This is not an official Google Product
