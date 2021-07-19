package basic

import (
	"bufio"
	"errors"
	"net/http"
	"os"
	"strings"

	"testing"

	"github.com/Azure/azure-sdk-for-go/storage"

	"github.com/khenidak/london/pkg/config"
)

// parses testing vars file (expected to be as ENV VAR) and convert it
// to a map
func GetTestingVars(t testing.TB) map[string]string {
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
func MakeTestConfig(t testing.TB, clearTable bool) *config.Config {
	configVals := GetTestingVars(t)

	_, useTLS := configVals["USE_TLS"]
	_, useRevisionTable := configVals["USE_REVISION_TABLE"]

	c := &config.Config{}
	c.AccountName = configVals["ACCOUNT_NAME"]
	c.StorageKey.AccountPrimaryKey = configVals["ACCOUNT_KEY"]
	c.StorageKey.ConnectionString = configVals["CONNECTION_STRING"]
	c.TableName = configVals["TABLE_NAME"]
	c.RevisionAccountName = configVals["REVISION_ACCOUNT_NAME"]
	c.StorageKey.RevisionAccountPrimaryKey = configVals["REVISION_ACCOUNT_KEY"]
	c.RevisionTableName = configVals["REVISION_TABLE_NAME"]
	c.UseRevisionTable = useRevisionTable

	// listening
	c.ListenAddress = "tcp://0.0.0.0:2379"
	if useTLS {
		c.UseTlS = useTLS
		c.TLSConfig.CertFilePath = configVals["SERVER_CERT_FILE_PATH"]
		c.TLSConfig.KeyFilePath = configVals["SERVER_KEY_FILE_PATH"]
		c.TLSConfig.TrustedCAFile = configVals["CLIENT_TRUSTED_CA_FILE_PATH"]
	}

	if err := c.Validate(); err != nil {
		t.Fatalf("failed to validate config:%v", err)
	}

	if err := c.InitRuntime(); err != nil {
		t.Fatalf("failed to init runtime with err:%v", err)
	}

	_, dontRecreate := configVals["DO_NOT_RECREATE_TABLE"]
	if dontRecreate {
		return c
	}
	if clearTable {
		t.Logf("** CLEARING TABLE, will take a bit")
		ClearTable(t, c)
	}
	return c
}

func ClearTable(t testing.TB, c *config.Config) {
	var status storage.AzureStorageServiceError
	// cosmos db is cool with this but old storage takes a long time to delete
	err := c.Runtime.StorageTable.Delete(100, &storage.TableOptions{})
	if err != nil {
		if !errors.As(err, &status) {
			t.Fatalf("unknown err:%v", err)
		}

		if status.StatusCode != http.StatusNotFound {
			t.Fatalf("got status code %d:  %v", status.StatusCode, err)
		}
	}

	tableCreated := false
	attempts := 0
	for attempts < 10 || !tableCreated {
		// add the table back for other tests to successfully use it
		err = c.Runtime.StorageTable.Create(100, storage.EmptyPayload, &storage.TableOptions{})
		if err == nil {
			tableCreated = true
			continue
		}
		if !errors.As(err, &status) {
			t.Fatalf("unknown err:%v", err)
		}
		// table deletion in the previous can take a few seconds to succeed before we can recreate again
		if status.StatusCode != http.StatusConflict {
			t.Fatalf("got status code %d:  %v", status.StatusCode, err)
		}
		attempts++
	}
}
