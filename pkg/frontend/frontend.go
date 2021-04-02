package frontend

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"strings"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	grpchealth "google.golang.org/grpc/health"
	grpchealthpb "google.golang.org/grpc/health/grpc_health_v1"

	klogv2 "k8s.io/klog/v2"

	"go.etcd.io/etcd/etcdserver/etcdserverpb"

	"github.com/khenidak/london/pkg/backend"
	"github.com/khenidak/london/pkg/config"
	"github.com/khenidak/london/pkg/types"
)

const (
	clusterId = uint64(212010)
	memberId  = uint64(10)
	raftTerm  = uint64(9)
)

type Frontend interface {
	StartListening() error
}

type frontend struct {
	config *config.Config
	be     backend.Backend

	leaseLock  sync.Mutex
	watchCount int64
	leases     map[int64]*types.Lease
}

func NewFrontend(config *config.Config, be backend.Backend) (Frontend, error) {
	fe := &frontend{
		config: config,
		be:     be,
		leases: make(map[int64]*types.Lease),
	}
	// start lease mgmt loop
	go fe.leaseMangementLoop()

	return fe, nil

}

func (fe *frontend) StartListening() error {
	var grpcServer *grpc.Server
	if fe.config.UseTlS {
		// tls
		cert, err := tls.LoadX509KeyPair(fe.config.TLSConfig.CertFilePath, fe.config.TLSConfig.KeyFilePath)
		if err != nil {
			return err
		}

		certPool := x509.NewCertPool()
		bs, err := ioutil.ReadFile(fe.config.TLSConfig.TrustedCAFile)
		if err != nil {
			return err
		}

		ok := certPool.AppendCertsFromPEM(bs)
		if !ok {
			return fmt.Errorf("failed to append client certs")
		}

		tlsConfig := &tls.Config{
			ClientAuth:   tls.RequireAndVerifyClientCert,
			Certificates: []tls.Certificate{cert},
			ClientCAs:    certPool,
		}

		opts := grpc.Creds(credentials.NewTLS(tlsConfig))
		grpcServer = grpc.NewServer(opts)

	} else {
		grpcServer = grpc.NewServer()
	}
	// register servers
	etcdserverpb.RegisterLeaseServer(grpcServer, fe)
	etcdserverpb.RegisterWatchServer(grpcServer, fe)
	etcdserverpb.RegisterKVServer(grpcServer, fe)

	// register health server
	// we check health as we bootup, Might be a good idea to
	// to watch for connectivity errors and use an actual health
	// server implementation
	healthServer := grpchealth.NewServer()
	healthServer.SetServingStatus("Watch", grpchealthpb.HealthCheckResponse_SERVING)
	healthServer.SetServingStatus("Lease", grpchealthpb.HealthCheckResponse_SERVING)
	healthServer.SetServingStatus("KV", grpchealthpb.HealthCheckResponse_SERVING)
	healthServer.SetServingStatus("", grpchealthpb.HealthCheckResponse_SERVING)

	grpchealthpb.RegisterHealthServer(grpcServer, healthServer)

	listener, err := fe.createAndStartListener()
	if err != nil {
		return err
	}

	go func() {
		// stop
		doneCh := fe.config.Runtime.Context.Done()
		<-doneCh
		klogv2.Infof("etcd grpc server received a stop sginal.. stopping")
		grpcServer.Stop()
		_ = listener.Close()
		fe.config.Runtime.Done <- struct{}{}
	}()

	go func() {
		err := grpcServer.Serve(listener)
		if err != nil {
			klogv2.Errorf("etcd grpc server failed to serve with err:%v", err)
			_ = listener.Close()
			// signal done
			fe.config.Runtime.Done <- struct{}{}
		}
	}()

	return nil
}

func (fe *frontend) createAndStartListener() (net.Listener, error) {
	parts := strings.Split(fe.config.ListenAddress, "://")
	netType := parts[0]
	address := parts[1]
	if netType == "unix" {
		// remove socket
		err := os.Remove(address)
		if err != nil && !os.IsNotExist(err) {
			return nil, err
		}
	}
	klogv2.Infof("etcd grpc server is listening on %s://%s", netType, address)
	return net.Listen(netType, address)
}

func createResponseHeader(revision int64) *etcdserverpb.ResponseHeader {
	header := &etcdserverpb.ResponseHeader{}
	header.ClusterId = clusterId
	header.MemberId = memberId
	header.RaftTerm = raftTerm
	header.Revision = revision

	return header
}
func createUnsupportedError(s string) error {
	return fmt.Errorf("+UNSUPPORTED+ %s is not supported", s)
}

func wrapIntoEtcdError(e error) error {
	// TODO
	return e
}

func suffixedKey(key string) string {
	if !strings.HasSuffix(key, "/") {
		return fmt.Sprintf("%s/", key)
	}

	return key
}
