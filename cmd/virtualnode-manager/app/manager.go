package app

import (
	"context"
	"fmt"
	"github.com/clusterrouter-io/clusterrouter/cmd/virtualnode-manager/app/config"
	"github.com/clusterrouter-io/clusterrouter/cmd/virtualnode-manager/app/options"
	"github.com/clusterrouter-io/clusterrouter/pkg/utils/log"
	"github.com/clusterrouter-io/clusterrouter/pkg/utils/log/klogv2"
	"github.com/clusterrouter-io/clusterrouter/pkg/utils/trace"
	"github.com/clusterrouter-io/clusterrouter/pkg/utils/trace/opencensus"
	"github.com/clusterrouter-io/clusterrouter/pkg/virtualnodemanager"
	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	cliflag "k8s.io/component-base/cli/flag"
	"k8s.io/component-base/term"
	"k8s.io/klog/v2"
	"os"
)

func NewVirtualNodeManagerCommand(ctx context.Context) *cobra.Command {
	opts, _ := options.NewVirtualNodeOptions()
	cmd := &cobra.Command{
		Use: "virtualnode-manager",
		RunE: func(cmd *cobra.Command, args []string) error {
			config, err := opts.Config()
			if err != nil {
				return err
			}

			log.L = klogv2.New(log.Fields{
				"log": "vkLog",
			})
			trace.T = opencensus.Adapter{}

			if err := Run(ctx, config); err != nil {
				return err
			}
			return nil
		},
	}

	/*	logger := logrus.StandardLogger()

		log.L = logrus2.FromLogrus(logrus.NewEntry(logger))
		logConfig := &Config{LogLevel: "info"}*/

	namedFlagSets := opts.Flags()

	fs := cmd.Flags()
	for _, f := range namedFlagSets.FlagSets {
		fs.AddFlagSet(f)
	}

	cols, _, _ := term.TerminalSize(cmd.OutOrStdout())
	cliflag.SetUsageAndHelpFunc(cmd, namedFlagSets, cols)

	return cmd
}

func Run(ctx context.Context, c *config.Config) error {
	vnManager := virtualnodemanager.NewManager(c)
	if !c.LeaderElection.LeaderElect {
		vnManager.Run(c.WorkerNumber, ctx.Done())
		return nil
	}

	id, err := os.Hostname()
	if err != nil {
		return err
	}
	id += "_" + string(uuid.NewUUID())

	rl, err := resourcelock.NewFromKubeconfig(
		c.LeaderElection.ResourceLock,
		c.LeaderElection.ResourceNamespace,
		c.LeaderElection.ResourceName,
		resourcelock.ResourceLockConfig{
			Identity: id,
		},
		c.KubeConfig,
		c.LeaderElection.RenewDeadline.Duration,
	)
	if err != nil {
		return fmt.Errorf("failed to create resource lock: %w", err)
	}

	var done chan struct{}
	leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
		Name: c.LeaderElection.ResourceName,

		Lock:          rl,
		LeaseDuration: c.LeaderElection.LeaseDuration.Duration,
		RenewDeadline: c.LeaderElection.RenewDeadline.Duration,
		RetryPeriod:   c.LeaderElection.RetryPeriod.Duration,

		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				done = make(chan struct{})
				defer close(done)

				stopCh := ctx.Done()
				vnManager.Run(c.WorkerNumber, stopCh)
			},
			OnStoppedLeading: func() {
				klog.Info("leaderelection lost")
				if done != nil {
					<-done
				}
			},
		},
	})
	return nil
}
