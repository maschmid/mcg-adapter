# MCG Adapter

A Kubernetes operator that receives S3 bucket notifications from NooBaa (Multicloud Object Gateway) and dispatches them as CloudEvents to configured trigger endpoints.

## Description

The MCG Adapter runs as an HTTP server inside a Kubernetes cluster, registered as a `bucketNotifications` connection in NooBaa. It watches `MCGOBCTrigger` custom resources to determine which S3 events from which buckets should be forwarded to which trigger URIs. When NooBaa delivers a notification, the adapter matches the event against all configured triggers and dispatches CloudEvents via HTTP to the matching endpoints.

## Custom Resource: MCGOBCTrigger

```yaml
apiVersion: internal.functions.dev/v1alpha1
kind: MCGOBCTrigger
metadata:
  namespace: foobar
  name: foo
spec:
  obc:
    name: foo-bucket          # ObjectBucketClaim in the same namespace
  events:
  - "s3:ObjectCreated:*"      # S3 event types to subscribe to
  triggers:
  - uri: http://foo.foobar.svc.cluster.local
  - uri: http://logger.foobar.svc.cluster.local
```

The controller manages three status conditions:

| Condition | Meaning |
|---|---|
| `OBCCredentialsAvailable` | The OBC’s ConfigMap and Secret exist with the expected keys |
| `BucketNotificationSet` | The S3 bucket notification was configured in NooBaa |
| `TestEventReceived` | The test event sent by NooBaa after notification setup was received |

Multiple `MCGOBCTrigger` resources referencing the same OBC are merged into a single bucket notification configuration (union of event types). Each trigger only receives events matching its own subscription.

## Getting Started

### Prerequisites

- Go 1.24+
- Docker 17.03+
- kubectl v1.11.3+
- Access to a Kubernetes cluster with NooBaa installed

### Configuration

The adapter is configured via environment variables:

| Variable | Default | Description |
|---|---|---|
| `ADAPTER_ID` | `mcg-adapter` | Identifier used in the S3 bucket notification configuration |
| `ADAPTER_TOPIC` | `mcg-adapter-connection` | Topic/connection name registered in NooBaa |
| `ADAPTER_PORT` | `8888` | Port the notification HTTP server listens on |

### NooBaa Connection Setup

Before the adapter can receive notifications, register it as an HTTP connection in NooBaa (one-time setup):

1. Create the connection secret:

```sh
oc create secret generic mcg-adapter-connection \
  --from-file=connect.json=/dev/stdin -n openshift-storage <<EOF
{
  "name": "mcg-adapter-connection",
  "notification_protocol": "http",
  "agent_request_object": {
    "host": "<adapter-service>.<adapter-namespace>.svc.cluster.local",
    "port": 8888
  }
}
EOF
```

2. Patch the NooBaa CR to register the connection:

```sh
existing_connections=$(oc get noobaa noobaa -n openshift-storage -o json \
  | jq -c ‘.spec.bucketNotifications.connections // []’)

updated_connections=$(echo "$existing_connections" | jq -c \
  --arg name "mcg-adapter-connection" \
  ‘[.[] | select(.name != $name)] + [{"name": $name, "namespace": "openshift-storage"}]’)

oc patch noobaa noobaa --type=’merge’ -n openshift-storage -p ‘{
  "spec": {
    "bucketNotifications": {
      "connections": ‘"${updated_connections}"’,
      "enabled": true
    }
  }
}’
```

### Deploy

```sh
make docker-build docker-push IMG=<some-registry>/mcg-adapter:tag
make install
make deploy IMG=<some-registry>/mcg-adapter:tag
```

### Run Locally (development)

```sh
make install          # Install CRDs
make run              # Run the operator locally
```

### Uninstall

```sh
kubectl delete -k config/samples/
make uninstall
make undeploy
```

## License

Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

