# Project London
Project London is an experimental implementation of etcd api on top of Azure Storage Table. The implementation is scoped
to supporting kubernetes clusters. A full implementation of etcd api is not on the scope of this project. A full implementation 
of etcd api on top azure storage is also not impossible due to way azure table perform transactions.

> While the functional correctness has been validated, the project is still in alpha status and we are tracking a set of known issues.

# How To Use
```
# clone the repo
git clone https://github.com/khenidak/london.git

cd london
# build. drop will be ./_output
make binary-build 


# run
./_output/london run \
--use-tls \
--cert-file=<path> \
--key-file=<path> \
--trusted-ca-file=<path> \
--account-name=<storage account name> \
--table-name=<storage table name> \
--primary-account-key=<storage account key>

## kubernetes api-server can now connect to https://localhost:2379 or alternatively use --listen-address argument to change listening address
```

# Testing and Validation
the repo contains a set of tests as the following
1. Unit tests `make unit-tests` validates functionality of data access layer.
2. Integration tests `make integration-tests` validates the functionality using etcd client apis. 
3. E2E tests `make e2e-test` validates the functionality using an in-proc kubernets api-server backed by London.
