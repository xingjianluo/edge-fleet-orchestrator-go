package edge

import (
	"time"

	"github.com/xingjianluo/edge-fleet-orchestrator-go/pkg/cloud"
	"github.com/xingjianluo/edge-fleet-orchestrator-go/pkg/deploy"
)

func deployMgrForTest(cp *cloud.ControlPlane) *deploy.Manager {
	return deploy.NewManager(cp, cp, time.Now)
}
