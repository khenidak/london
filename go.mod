module github.com/khenidak/london

// we use local copy of kubernetes (check the make file) and vendor it in our
// e2e tests. kubernetes (except klog) is not used in the binary itself.

go 1.15

require (
	github.com/Azure/azure-sdk-for-go v50.0.0+incompatible
	github.com/Azure/go-autorest/autorest v0.11.16 // indirect
	github.com/Azure/go-autorest/autorest/to v0.4.0 // indirect
	github.com/coreos/go-systemd v0.0.0-20191104093116-d3cd4ed1dbcf // indirect
	github.com/dnaeon/go-vcr v1.1.0 // indirect
	github.com/evanphx/json-patch v4.9.0+incompatible
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/google/uuid v1.1.4 // indirect
	github.com/urfave/cli v1.22.2
	go.etcd.io/etcd v0.5.0-alpha.5.0.20200910180754-dd1b699fc489
	go.uber.org/zap v1.16.0 // indirect
	google.golang.org/grpc v1.27.1
	k8s.io/api v0.0.0
	k8s.io/apimachinery v0.0.0
	k8s.io/client-go v0.0.0
	k8s.io/klog/v2 v2.4.0
	k8s.io/kubernetes v0.0.0
)

replace (
	go.etcd.io/etcd => go.etcd.io/etcd v0.5.0-alpha.5.0.20200910180754-dd1b699fc489

	k8s.io/api => ./kubernetes_src/kubernetes/staging/src/k8s.io/api
	k8s.io/apiextensions-apiserver => ./kubernetes_src/kubernetes/staging/src/k8s.io/apiextensions-apiserver
	k8s.io/apimachinery => ./kubernetes_src/kubernetes/staging/src/k8s.io/apimachinery
	k8s.io/apiserver => ./kubernetes_src/kubernetes/staging/src/k8s.io/apiserver
	k8s.io/cli-runtime => ./kubernetes_src/kubernetes/staging/src/k8s.io/cli-runtime
	k8s.io/client-go => ./kubernetes_src/kubernetes/staging/src/k8s.io/client-go
	k8s.io/cloud-provider => ./kubernetes_src/kubernetes/staging/src/k8s.io/cloud-provider
	k8s.io/cluster-bootstrap => ./kubernetes_src/kubernetes/staging/src/k8s.io/cluster-bootstrap
	k8s.io/code-generator => ./kubernetes_src/kubernetes/staging/src/k8s.io/code-generator
	k8s.io/component-base => ./kubernetes_src/kubernetes/staging/src/k8s.io/component-base
	k8s.io/component-helpers => ./kubernetes_src/kubernetes/staging/src/k8s.io/component-helpers
	k8s.io/controller-manager => ./kubernetes_src/kubernetes/staging/src/k8s.io/controller-manager
	k8s.io/cri-api => ./kubernetes_src/kubernetes/staging/src/k8s.io/cri-api
	k8s.io/csi-translation-lib => ./kubernetes_src/kubernetes/staging/src/k8s.io/csi-translation-lib
	k8s.io/kube-aggregator => ./kubernetes_src/kubernetes/staging/src/k8s.io/kube-aggregator
	k8s.io/kube-controller-manager => ./kubernetes_src/kubernetes/staging/src/k8s.io/kube-controller-manager
	k8s.io/kube-proxy => ./kubernetes_src/kubernetes/staging/src/k8s.io/kube-proxy
	k8s.io/kube-scheduler => ./kubernetes_src/kubernetes/staging/src/k8s.io/kube-scheduler
	k8s.io/kubectl => ./kubernetes_src/kubernetes/staging/src/k8s.io/kubectl
	k8s.io/kubelet => ./kubernetes_src/kubernetes/staging/src/k8s.io/kubelet
	k8s.io/kubernetes => ./kubernetes_src/kubernetes/
	k8s.io/legacy-cloud-providers => ./kubernetes_src/kubernetes/staging/src/k8s.io/legacy-cloud-providers
	k8s.io/metrics => ./kubernetes_src/kubernetes/staging/src/k8s.io/metrics
	k8s.io/mount-utils => ./kubernetes_src/kubernetes/staging/src/k8s.io/mount-utils
	k8s.io/sample-apiserver => ./kubernetes_src/kubernetes/staging/src/k8s.io/sample-apiserver
)
