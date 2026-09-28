# Deployment boundary

This revision has no deployable service, listening socket, DDB adapter or
production configuration. `history-cache-tool` remains an offline fixture
command. `history-cache-staging` adds an explicitly gated S3 runner for synthetic
staging only; it defaults to planning without credentials or network access.

Do not wrap the fixture tool in a production timer. Publisher service templates,
agent containers, secret mounts and cloud UDS wiring are future deliverables
after P2/P3 acceptance and separate deployment approval.

Future publisher deployment is independent of `upcloud_gateway.service`. Cloud
and agent deployments use immutable artifacts, staging first, one cloud node at
a time. Nothing in the offline CI deploys or uploads data.
