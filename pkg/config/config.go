package config

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Azure/azure-sdk-for-go/storage"
)

const (
	AuthType_StorageKey            = "storage-key"
	connectionStringAccountName    = "accountname"
	connectionStringAccountKey     = "accountkey"
	connectionStringEndpointSuffix = "endpointsuffix"
	connectionStringTableEndpoint  = "connectionString"
)

type AuthStorageKey struct {
	//TODO account secondary key
	AccountPrimaryKey         string
	RevisionAccountPrimaryKey string
	ConnectionString          string
	RevisionConnectionString  string
	EndpointSuffix            string
	RevisionEndpointSuffix    string
}

type TLSConfig struct {
	CertFilePath  string
	KeyFilePath   string
	TrustedCAFile string
}

type Runtime struct {
	Done chan struct{}
	// Important: don't interact with this channel
	// except in tests to close to the entire runtime
	Stop                  chan os.Signal
	Context               context.Context
	StorageClient         storage.Client
	TableClient           storage.TableServiceClient
	StorageTable          *storage.Table
	RevisionStorageClient storage.Client
	RevisionTableClient   storage.TableServiceClient
	RevisionStorageTable  *storage.Table
}

type Config struct {
	ListenAddress string
	UseTlS        bool
	AuthType      string

	AccountName string
	TableName   string
	StorageKey  AuthStorageKey

	// RevisionAccountName is the account name for revision table
	RevisionAccountName string
	// RevisionTableName is the table name to use for revision
	// if not set, will default to the table name
	RevisionTableName string
	// RevisionStorageKey is the storage key required to access
	// revision table
	RevisionStorageKey AuthStorageKey
	UseRevisionTable   bool

	TLSConfig TLSConfig

	// TODO MSI etc

	// max in memory cached events
	MaxEventCount int

	Runtime Runtime
}

func NewConfig() *Config {
	return &Config{}
}

//Stole from here.
//https://github.com/Azure/azure-sdk-for-go/blob/1b5e008a20b6382007c576c991be7c4c95f496eb/storage/client.go#L242
func parseConnectionString(cstr string) map[string]string {
	parts := map[string]string{}
	for _, pair := range strings.Split(cstr, ";") {
		if pair == "" {
			continue
		}

		equalDex := strings.IndexByte(pair, '=')
		if equalDex <= 0 {
			continue
		}

		value := strings.TrimSpace(pair[equalDex+1:])
		key := strings.TrimSpace(strings.ToLower(pair[:equalDex]))
		parts[key] = value
	}
	return parts
}

func (c *Config) Validate() error {
	if len(c.StorageKey.ConnectionString) != 0 {
		//Cosmos db looks like
		//DefaultEndpointsProtocol=https;AccountName=londontest;AccountKey=<hidden>;TableEndpoint=https://londontest.table.cosmos.azure.com:443/;
		//table looks like
		//DefaultEndpointsProtocol=https;AccountName=oldlondon;AccountKey=<hidden>;EndpointSuffix=core.windows.net
		//notice endpointsufix vs tableendpoint
		parts := parseConnectionString(c.StorageKey.ConnectionString)

		c.AccountName = parts[connectionStringAccountName]
		c.StorageKey.AccountPrimaryKey = parts[connectionStringAccountKey]
		c.StorageKey.EndpointSuffix = parts[connectionStringEndpointSuffix]
		if _, ok := parts["tableendpoint"]; ok {
			//todo parse this out of TableEndpoint
			c.StorageKey.EndpointSuffix = "cosmos.azure.com"
		}
	}

	if len(c.StorageKey.RevisionConnectionString) != 0 {
		parts := parseConnectionString(c.StorageKey.RevisionConnectionString)

		c.RevisionAccountName = parts[connectionStringAccountName]
		c.StorageKey.RevisionAccountPrimaryKey = parts[connectionStringAccountKey]
		c.StorageKey.RevisionEndpointSuffix = parts[connectionStringEndpointSuffix]
		if _, ok := parts["tableendpoint"]; ok {
			//todo parse this out of TableEndpoint
			c.StorageKey.RevisionEndpointSuffix = "cosmos.azure.com"
		}
	}

	/*Do we need to be explicit or just default to connections string if present?
	if c.AuthType != AuthType_StorageKey {
		return fmt.Errorf("storage auth types other than storage-key are not supported yet")
	}*/

	// storage configuration
	if len(c.AccountName) == 0 {
		return fmt.Errorf("storage account name is required")
	}

	// assuming that we are using keys. When we add more
	// change this validation
	if len(c.StorageKey.AccountPrimaryKey) == 0 {
		return fmt.Errorf("storage account key is required")
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

	// validation when a separate table is used for revision
	if c.UseRevisionTable {
		// check if table name, account name and storage key are provided if
		// using a separate table for revision
		if len(c.RevisionTableName) == 0 {
			return fmt.Errorf("storage account table name is required for revision")
		}
		if len(c.RevisionAccountName) == 0 {
			return fmt.Errorf("storage account name is required for revision")
		}
		if len(c.StorageKey.RevisionAccountPrimaryKey) == 0 {
			return fmt.Errorf("storage account key is required for revision")
		}
	}

	return nil
}

const cosmosApiVersion = "2019-07-07"

var CosmosDbAdditionalHeaders = map[string]string{
	"MaxDataServiceVersion": "3.0;NetFx",
	"DataServiceVersion":    "3.0",
}

func (c *Config) InitRuntime() error {
	var err error
	if c.StorageKey.EndpointSuffix != "" {
		// if cosmos then use newcosmosclient? seems ot just use different validation on accountname
		if c.Runtime.StorageClient, err = storage.NewClient(c.AccountName, c.StorageKey.AccountPrimaryKey,
			c.StorageKey.EndpointSuffix, cosmosApiVersion, true); err != nil {
			return err
		}
	} else {
		if c.Runtime.StorageClient, err = storage.NewBasicClient(c.AccountName, c.StorageKey.AccountPrimaryKey); err != nil {
			return err
		}
	}

	// check if revision is using it's own table
	if c.UseRevisionTable {
		if c.StorageKey.RevisionEndpointSuffix != "" {
			if c.Runtime.RevisionStorageClient, err = storage.NewClient(c.RevisionAccountName, c.StorageKey.RevisionAccountPrimaryKey,
				c.StorageKey.RevisionEndpointSuffix, cosmosApiVersion, true); err != nil {
				return err
			}
		} else {
			if c.Runtime.RevisionStorageClient, err = storage.NewBasicClient(c.RevisionAccountName, c.StorageKey.RevisionAccountPrimaryKey); err != nil {
				return err
			}
		}
	} else {
		// use the same table and client for revision and store
		c.Runtime.RevisionStorageClient = c.Runtime.StorageClient
		c.RevisionTableName = c.TableName
	}

	//use explicit cosmos flag in the config?
	if strings.Contains(c.StorageKey.EndpointSuffix, "cosmos.") {
		//black magic to make cosmos db work with old go client
		c.Runtime.StorageClient.AddAdditionalHeaders(CosmosDbAdditionalHeaders)
	}

	c.Runtime.TableClient = c.Runtime.StorageClient.GetTableService()
	c.Runtime.StorageTable = c.Runtime.TableClient.GetTableReference(c.TableName)
	c.Runtime.RevisionTableClient = c.Runtime.RevisionStorageClient.GetTableService()
	c.Runtime.RevisionStorageTable = c.Runtime.RevisionTableClient.GetTableReference(c.RevisionTableName)

	// optimize http Client
	defaultRoundTripper := http.DefaultTransport
	transport := defaultRoundTripper.(*http.Transport)
	// we favor memory consumption over cycling
	// connections. roughly the same # of connections
	// created by all api-server goroutine.
	transport.MaxIdleConns = 35000
	transport.MaxIdleConnsPerHost = 35000
	transport.IdleConnTimeout = time.Second * 0
	transport.ForceAttemptHTTP2 = false

	c.Runtime.StorageClient.HTTPClient.Transport = transport
	c.Runtime.RevisionStorageClient.HTTPClient.Transport = transport

	// wire up runtime stop and context
	c.Runtime.Context = context.Background()
	c.Runtime.Stop = make(chan os.Signal, 1)
	c.Runtime.Done = make(chan struct{})
	var cancel func()
	c.Runtime.Context, cancel = context.WithCancel(c.Runtime.Context)

	signal.Notify(c.Runtime.Stop, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-c.Runtime.Stop
		close(c.Runtime.Stop)
		cancel()
	}()
	return nil
}
