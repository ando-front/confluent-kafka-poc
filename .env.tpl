# =============================================================================
# confluent-kafka-poc — environment variable template
# -----------------------------------------------------------------------------
# Copy this file to `.env` and fill in real values for your environment.
#   cp .env.tpl .env
# For local development you can instead rely on `.env.local` (checked in,
# secret-free defaults). NEVER put real secrets in .env.tpl or .env.local.
# =============================================================================

# --- Target environment: "local" (docker-compose) or "confluent" (Cloud) -----
KAFKA_ENV=local

# --- Broker connection -------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
SCHEMA_REGISTRY_URL=http://localhost:8081

# --- Client identity ---------------------------------------------------------
KAFKA_CLIENT_ID=confluent-kafka-poc
KAFKA_GROUP_ID=confluent-kafka-poc-group
KAFKA_AUTO_OFFSET_RESET=earliest

# --- Security (local = PLAINTEXT, Confluent Cloud = SASL_SSL) -----------------
# For Confluent Cloud set:
#   KAFKA_SECURITY_PROTOCOL=SASL_SSL
#   KAFKA_SASL_MECHANISM=PLAIN
#   KAFKA_SASL_USERNAME=<CLUSTER_API_KEY>
#   KAFKA_SASL_PASSWORD=<CLUSTER_API_SECRET>          # <-- put in .env, not here
#   SCHEMA_REGISTRY_URL=https://<sr-endpoint>
#   SCHEMA_REGISTRY_API_KEY=<SR_API_KEY>
#   SCHEMA_REGISTRY_API_SECRET=<SR_API_SECRET>        # <-- put in .env, not here
KAFKA_SECURITY_PROTOCOL=PLAINTEXT
KAFKA_SASL_MECHANISM=
KAFKA_SASL_USERNAME=
KAFKA_SASL_PASSWORD=
SCHEMA_REGISTRY_API_KEY=
SCHEMA_REGISTRY_API_SECRET=

# --- Defaults for topics created by the PoC ----------------------------------
DEFAULT_TOPIC_PARTITIONS=3
DEFAULT_TOPIC_REPLICATION=1

# --- Logging -----------------------------------------------------------------
LOG_LEVEL=INFO
