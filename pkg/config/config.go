package config

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/Azure/azure-sdk-for-go/storage"
)

const (
	AuthType_StorageKey = "storage-key"
)

type AuthStorageKey struct {
	//TODO account secondary key
	AccountPrimaryKey string
	ConnectionString  string
}

type TLSConfig struct {
	CertFilePath  string
	KeyFilePath   string
	TrustedCAFile string
}

type Runtime struct {
	Done          chan struct{}
	Stop          chan os.Signal
	Context       context.Context
	StorageClient storage.Client
	TableClient   storage.TableServiceClient
	StorageTable  *storage.Table
}

type Config struct {
	ListenAddress string
	UseTlS        bool
	AuthType      string

	AccountName string
	TableName   string

	StorageKey AuthStorageKey
	TLSConfig  TLSConfig

	// TODO MSI etc

	Runtime Runtime
}

func NewConfig() *Config {
	return &Config{}
}

func (c *Config) Validate() error {
	if c.AuthType != AuthType_StorageKey {
		return fmt.Errorf("storage auth types other than storage-key are not supported yet")
	}

	// storage configuration
	if len(c.AccountName) == 0 {
		return fmt.Errorf("storage account name is required")
	}

	// assuming that we are using keys. When we add more
	// change this validation
	if len(c.StorageKey.AccountPrimaryKey) == 0 {
		return fmt.Errorf("storage account key is required")
	}

	// we don't support connection strings yet
	if len(c.StorageKey.ConnectionString) != 0 {
		return fmt.Errorf("storage connection string is not supported yet")
	}

	if len(c.TableName) == 0 {
		return fmt.Errorf("storage account table name is required")
	}

	// listening endpoint config
	if c.UseTlS {
		if len(c.TLSConfig.CertFilePath) == 0 {
			return fmt.Errorf("cert file path is required when TLS is set to true")
		}

		if len(c.TLSConfig.KeyFilePath) == 0 {
			return fmt.Errorf("key file path is required when TLS is set to true")
		}

		if len(c.TLSConfig.TrustedCAFile) == 0 {
			return fmt.Errorf("trust client CA file is required when TLS is set to true")
		}
	}
	return nil
}

func (c *Config) InitRuntime() error {
	var err error
	c.Runtime.StorageClient, err = storage.NewBasicClient(c.AccountName, c.StorageKey.AccountPrimaryKey)

	if err != nil {
		return err
	}

	c.Runtime.TableClient = c.Runtime.StorageClient.GetTableService()
	c.Runtime.StorageTable = c.Runtime.TableClient.GetTableReference(c.TableName)

	// wire up runtime stop and context
	c.Runtime.Context = context.Background()
	c.Runtime.Stop = make(chan os.Signal, 1)
	c.Runtime.Done = make(chan struct{})
	var cancel func()
	c.Runtime.Context, cancel = context.WithCancel(c.Runtime.Context)

	signal.Notify(c.Runtime.Stop, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-c.Runtime.Stop
		cancel()
	}()
	// TODO would be great if we have a Done channel to graceful wait for things to declare done
	return nil
}
