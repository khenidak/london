package main

import (
	"fmt"
	"os"

	klogv2 "k8s.io/klog/v2"

	"github.com/urfave/cli"

	"github.com/khenidak/london/pkg/backend"
	"github.com/khenidak/london/pkg/config"
	"github.com/khenidak/london/pkg/frontend"
)

// version mgmt, set in makefile
var Version string
var Buildtime string

func main() {
	config := config.NewConfig()

	app := cli.NewApp()
	app.Name = "london"
	app.Description = "TBD"
	app.Usage = "TBD"
	app.Commands = []cli.Command{
		{
			Name:  "run",
			Usage: "runs the listener",
			Flags: getRunFlags(config),
			Action: func(c *cli.Context) error {
				return startApp(config, c)
			},
		},
		{
			Name:  "version",
			Usage: "prints version and build time of this binary",
			Action: func(c *cli.Context) error {
				fmt.Printf("Version: %s\n", Version)
				fmt.Printf("BuildTime: %s\n", Buildtime)
				return nil
			},
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		klogv2.Infof("failed to run app with err:%v", err)
	}
}

func startApp(c *config.Config, cliContext *cli.Context) error {
	// validate config
	if err := c.Validate(); err != nil {
		return err
	}

	if err := c.InitRuntime(); err != nil {
		return err
	}

	be, err := backend.NewBackend(c)
	if err != nil {
		return err
	}

	fe, err := frontend.NewFrontend(c, be)
	if err != nil {
		return err
	}

	err = fe.StartListening()
	if err != nil {
		return err
	}

	<-c.Runtime.Done
	return nil
}

func getRunFlags(config *config.Config) []cli.Flag {
	return []cli.Flag{
		cli.StringFlag{
			Name:        "listen-address",
			Value:       "tcp://0.0.0.0:2379",
			Destination: &config.ListenAddress,
		},
		cli.IntFlag{
			Name: "max-event-count",
			Value: 10000, // Should be increased for large clusters
			Destination: &config.MaxEventCount,
		}

		cli.BoolFlag{
			Name:        "use-tls",
			Destination: &config.UseTlS,
		},

		cli.BoolFlag{
			Name:        "use-revision-table",
			Destination: &config.UseRevisionTable,
		},

		cli.StringFlag{
			Name:        "cert-file",
			Usage:       "path to the server TLS cert file",
			Destination: &config.TLSConfig.CertFilePath,
		},

		cli.StringFlag{
			Name:        "key-file",
			Usage:       "path to the server TLS key file",
			Destination: &config.TLSConfig.KeyFilePath,
		},

		cli.StringFlag{
			Name:        "trusted-ca-file",
			Usage:       "path to the client ca for client auth",
			Destination: &config.TLSConfig.TrustedCAFile,
		},

		cli.StringFlag{
			Name:        "account-name",
			Usage:       "azure storage account name",
			Destination: &config.AccountName,
		},
		cli.StringFlag{
			Name:        "table-name",
			Usage:       "azure storage table name",
			Destination: &config.TableName,
		},

		cli.StringFlag{
			Name:        "revision-account-name",
			Usage:       "azure storage account name for revision",
			Destination: &config.RevisionAccountName,
		},

		cli.StringFlag{
			Name:        "revision-table-name",
			Usage:       "azure storage table name for revision",
			Destination: &config.RevisionTableName,
		},

		cli.StringFlag{
			Name:        "azure-auth-type",
			Usage:       "supported values storage-key:either key or connection string",
			Value:       "storage-key",
			Destination: &config.AuthType,
		},

		cli.StringFlag{
			Name:        "primary-account-key",
			Usage:       "azure storage account key",
			Destination: &config.StorageKey.AccountPrimaryKey,
		},

		cli.StringFlag{
			Name:        "revision-primary-account-key",
			Usage:       "azure storage account key for revision",
			Destination: &config.StorageKey.RevisionAccountPrimaryKey,
		},

		cli.StringFlag{
			Name:        "connection-string",
			Usage:       "storage connection string. mutually exclusive with account name and key",
			Destination: &config.StorageKey.ConnectionString,
		},
	}
}
