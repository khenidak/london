package config

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

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
	AccountPrimaryKey string
	ConnectionString  string
	EndpointSuffix    string
	IsCosmos          bool
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
	// we don't support connection strings yet

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
			c.StorageKey.IsCosmos = true
			//todo parse this out of TableEndpoint
			c.StorageKey.EndpointSuffix = "cosmos.azure.com"
		}
		//return fmt.Errorf("storage connection string is not supported yet")
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
	return nil
}

const cosmosApiVersion = "2019-07-07"

var CosmosDbAdditionalHeaders = map[string]string{
	"MaxDataServiceVersion": "3.0;NetFx",
	"DataServiceVersion":    "3.0",
}

func (c *Config) InitRuntime() error {
	var err error
	fmt.Printf("Endpoint suffix was %s\n", c.StorageKey.EndpointSuffix)
	if c.StorageKey.EndpointSuffix != "" {
		//if cosmos then use newcosmosclient? seems ot just use different validation on accountname
		c.Runtime.StorageClient, err = storage.NewClient(c.AccountName, c.StorageKey.AccountPrimaryKey,
			c.StorageKey.EndpointSuffix, cosmosApiVersion, true)
	} else {
		c.Runtime.StorageClient, err = storage.NewBasicClient(c.AccountName, c.StorageKey.AccountPrimaryKey)
	}
	if err != nil {
		return err
	}

	//use explicit cosmos flag in the config?
	if strings.Contains(c.StorageKey.EndpointSuffix, "cosmos.") {
		//black magic to make cosmos db work with old go client
		fmt.Printf("using cosmos since tableendpoint was %s\n", c.StorageKey.EndpointSuffix)
		c.Runtime.StorageClient.AddAdditionalHeaders(CosmosDbAdditionalHeaders)
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
