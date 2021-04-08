package utils

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"math/rand"
	"strings"
	"syscall"
	"testing"
	"time"

	"go.etcd.io/etcd/clientv3"

	"github.com/khenidak/london/pkg/backend"
	"github.com/khenidak/london/pkg/config"
	"github.com/khenidak/london/pkg/frontend"

	basictestutils "github.com/khenidak/london/test/utils/basic"
)

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyz0123456789")

func init() {
	rand.Seed(time.Now().UnixNano())
}

// creates a test etcd listener on top of azure storage table
func CreateTestApp(c *config.Config, t testing.TB) (stop func()) {
	t.Helper()
	be, err := backend.NewBackend(c)
	if err != nil {
		t.Fatalf("failed to create backend with err:%v", err)
	}

	fe, err := frontend.NewFrontend(c, be)
	if err != nil {
		t.Fatalf("failed to create frontend with err:%v", err)
	}

	err = fe.StartListening()
	if err != nil {
		t.Fatalf("faliled to start listening with err:%v", err)
	}

	return func() {
		//TODO. Clean exist for app
		c.Runtime.Stop <- syscall.SIGINT
		c.Runtime.Stop <- syscall.SIGTERM
		t.Logf("test app - stop signal sent")
		<-c.Runtime.Done
	}
}

func addressFromListenAddress(s string) string {
	parts := strings.Split(s, "://")
	return parts[1]
}

func MakeTestEtcdClient(c *config.Config, t testing.TB) *clientv3.Client {
	address := addressFromListenAddress(c.ListenAddress)
	clientConfig := clientv3.Config{
		Endpoints:   []string{address, address, address},
		DialTimeout: 10 * time.Second,
	}
	configVals := basictestutils.GetTestingVars(t)
	_, useTLS := configVals["USE_TLS"]
	if useTLS {
		cert, err := tls.LoadX509KeyPair(configVals["CLIENT_CERT_FILE_PATH"], configVals["CLIENT_KEY_FILE_PATH"])
		if err != nil {
			t.Fatalf("failed to read certs for client with err:%v", err)
		}

		certPool := x509.NewCertPool()
		bs, err := ioutil.ReadFile(configVals["SERVER_TRUSTED_CA_FILE_PATH"])
		if err != nil {
			t.Fatalf("failed to read server trusted CA file with error:%v", err)
		}

		ok := certPool.AppendCertsFromPEM(bs)
		if !ok {
			t.Fatalf("failed to append client certs")
		}

		tlsConfig := &tls.Config{

			ClientAuth:   tls.RequireAndVerifyClientCert,
			Certificates: []tls.Certificate{cert},
			ClientCAs:    certPool,
		}
		tlsConfig.InsecureSkipVerify = true
		clientConfig.TLS = tlsConfig
	}

	cli, err := clientv3.New(clientConfig)
	if err != nil {
		t.Fatalf("failed to create etcd client with err:%v", err)
	}

	return cli
}

func RandStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func RandObjectName(n int) string {
	return fmt.Sprintf("%s%s", "k", RandStringRunes(n))
}

// generates a key, makes sure that it has a / in the key name
func RandKey(n int) string {
	half := (n - 1) / 2

	return fmt.Sprintf("%s/%s", RandStringRunes(half), RandStringRunes(half))
}
