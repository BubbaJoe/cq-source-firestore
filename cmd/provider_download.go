package cmd

import (
	"context"

	"github.com/cloudquery/cloudquery/internal/logging"
	"github.com/cloudquery/cloudquery/internal/signalcontext"
	"github.com/cloudquery/cloudquery/pkg/ui/console"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var providerDownloadHelpMsg = "Downloads all providers specified in config.hcl."

var providerDownloadCmd = &cobra.Command{
	Use:   "download",
	Short: providerDownloadHelpMsg,
	Long:  providerDownloadHelpMsg,
	Example: `
  # Downloads all providers specified in config.hcl:
  ./cloudquery provider download
`,
	RunE: func(cmd *cobra.Command, args []string) error {
		configPath := viper.GetString("configPath")
		ctx, _ := signalcontext.WithInterrupt(context.Background(), logging.NewZHcLog(&log.Logger, ""))
		c, err := console.CreateClient(ctx, configPath)
		if err != nil {
			return err
		}
		defer c.Client().Close()
		return c.DownloadProviders(ctx)
	},
}

func init() {
	providerCmd.AddCommand(providerDownloadCmd)
}
