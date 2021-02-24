package basic

import (
	"bufio"
	"os"
	"strings"

	"testing"

	"github.com/Azure/azure-sdk-for-go/storage"

	"github.com/khenidak/london/pkg/config"
)

// parses testing vars file (expected to be as ENV VAR) and convert it
// to a map
func GetTestingVars(t *testing.T) map[string]string {
	m := make(map[string]string)
	testingVarsPath := os.Getenv("LONDON_TESTING_VARS")
	if len(testingVarsPath) == 0 {
		t.Fatalf("no testing vars file defined (LONDON_TESTING_VARS) check hack/testing_vars_doc file")
	}

	file, err := os.Open(testingVarsPath)
	if err != nil {
		t.Fatalf("failed to open testing vars %v err:%v", testingVarsPath, err)
	}

	fscanner := bufio.NewScanner(file)
	for fscanner.Scan() {
		txt := fscanner.Text()
		parts := strings.Split(txt, " ")
		if len(parts) == 2 {
			m[parts[0]] = parts[1]
		} else {
			m[txt] = ""
		}
	}

	return m
}

// creates test config based on testing var file.
func MakeTestConfig(t *testing.T) *config.Config {
	configVals := GetTestingVars(t)

	_, useTLS := configVals["USE_TLS"]

	c := &config.Config{}
	c.AccountName = configVals["ACCOUNT_NAME"]
	c.StorageKey.AccountPrimaryKey = configVals["ACCOUNT_KEY"]
	c.TableName = configVals["TABLE_NAME"]
	// listening
	c.ListenAddress = "tcp://0.0.0.0:2379"
	if useTLS {
		c.UseTlS = useTLS
		c.TLSConfig.CertFilePath = configVals["SERVER_CERT_FILE_PATH"]
		c.TLSConfig.KeyFilePath = configVals["SERVER_KEY_FILE_PATH"]
		c.TLSConfig.TrustedCAFile = configVals["CLIENT_TRUSTED_CA_FILE_PATH"]
	}

	if err := c.InitRuntime(); err != nil {
		t.Fatalf("failed to init runtime with err:%v", err)
	}

	_, dontRecreate := configVals["DO_NOT_RECREATE_TABLE"]
	if dontRecreate {
		return c
	}
	t.Logf("** CLEARING TABLE, will take a bit")
	// TODO: Logic to drop and create table
	ClearTable(t, c)

	return c
}

func ClearTable(t *testing.T, c *config.Config) {
	all := make([]*storage.Entity, 0)

	o := &storage.QueryOptions{
		Filter: "",
	}
	res, err := c.Runtime.StorageTable.QueryEntities(1, storage.MinimalMetadata, o)
	if err != nil {
		t.Fatalf("CLEAR-TABLE-FAILED: error query table :%v", err)
	}
	all = append(all, res.Entities...)
	for {
		if res.NextLink == nil {
			break
		}

		res, err = res.NextResults(nil) // TODO: <-- is this correct??
		if err != nil {
			t.Fatalf("CLEAR-TABLE-FAILED: error query tablei - next :%v", err)
		}
		all = append(all, res.Entities...)
	}

	for _, e := range all {
		batch := c.Runtime.StorageTable.NewBatch()
		batch.Table = c.Runtime.StorageTable

		batch.DeleteEntity(e, true)
		err := batch.ExecuteBatch()
		if err != nil {
			t.Fatalf("failed to execute batch to clear table:%v", err)
		}

	}
}
