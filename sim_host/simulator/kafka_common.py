"""Shared Kafka + Schema Registry setup for the simulator and peek tool.
MSK IAM login from Python: OAUTHBEARER token signed with the EC2 role."""
import os
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

REGION = os.environ.get("AWS_REGION", "ap-southeast-2")
REGISTRY_URL = os.environ.get("REGISTRY_URL", "http://localhost:8081")
SCHEMA_DIR = os.environ.get("SCHEMA_DIR", "/opt/rs/sim_host/schemas")
CA_BUNDLE = "/etc/pki/tls/certs/ca-bundle.crt"


def _oauth_cb(_config):
    token, expiry_ms = MSKAuthTokenProvider.generate_auth_token(REGION)
    return token, expiry_ms / 1000


def kafka_conf(**extra):
    conf = {
        "bootstrap.servers": os.environ["BS"],
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "OAUTHBEARER",
        "oauth_cb": _oauth_cb,
    }
    if os.path.exists(CA_BUNDLE):
        conf["ssl.ca.location"] = CA_BUNDLE
    conf.update(extra)
    return conf


def schema_str(subject):
    with open(os.path.join(SCHEMA_DIR, subject + ".avsc")) as f:
        return f.read()
