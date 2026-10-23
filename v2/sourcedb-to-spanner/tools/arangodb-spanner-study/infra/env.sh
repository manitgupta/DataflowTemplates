# Common settings for the arango-* environment (source me).
export CLOUDSDK_ACTIVE_CONFIG_NAME=omni
export PROJECT=terraform-demo-host
export REGION=us-central1
export ZONE=us-central1-a
export NET=arango-vpc
export SUBNET=arango-subnet
export SUBNET_RANGE=10.30.0.0/20
export BUCKET=gs://arango-${PROJECT}
export ARANGO_VERSION=3.11.14
export ARANGO_TARBALL_URL=https://download.arangodb.com/arangodb311/Community/Linux/arangodb3-linux-${ARANGO_VERSION}_x86_64.tar.gz
# Cluster: node i gets 10.30.1.(10+i)
export ARANGO_NODE_PREFIX=arango-db
export ARANGO_IP_BASE=10.30.1.
