"""Shared Kafka + Schema Registry setup for the simulator, lab scripts and peek tool.
KAFKA_MODE=msk   (default): MSK with IAM login (OAUTHBEARER token signed with the EC2 role)
KAFKA_MODE=local          : local Docker cluster, plain connection, no login"""
import os

HERE = os.path.dirname(os.path.abspath(__file__))
MODE = os.environ.get("KAFKA_MODE", "msk")
REGION = os.environ.get("AWS_REGION", "ap-southeast-2")
REGISTRY_URL = os.environ.get("REGISTRY_URL", "http://localhost:8081")
SCHEMA_DIR = os.environ.get("SCHEMA_DIR", os.path.join(HERE, "..", "schemas"))
CA_BUNDLE = "/etc/pki/tls/certs/ca-bundle.crt"
LOCAL_BS = "localhost:9092,localhost:9192,localhost:9292"


def _oauth_cb(_config):
    from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
    token, expiry_ms = MSKAuthTokenProvider.generate_auth_token(REGION)
    return token, expiry_ms / 1000


def kafka_conf(**extra):
    if MODE == "local":
        conf = {"bootstrap.servers": os.environ.get("BS", LOCAL_BS)}
    else:
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
