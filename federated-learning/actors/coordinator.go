package actors

import (
	"agentskiSistemi/actor-framework"
	"agentskiSistemi/actor-framework/crdt"
	"agentskiSistemi/actor-framework/remote"
	"agentskiSistemi/federated-learning/model"
	"fmt"
	"time"
)

func init() {
	remote.DefaultRegistry.Register(ClientJoin{})
	remote.DefaultRegistry.Register(StartRound{})
	remote.DefaultRegistry.Register(LocalUpdate{})
	remote.DefaultRegistry.Register(ClusterAssign{})
}

type CoordinatorConfig struct {
	TotalRounds  int
	LearningRate float64
	Epochs       int
	DoneCh       chan struct{}
}

type CoordinatorActor struct {
	cfg            CoordinatorConfig
	globalModel    *model.LinearModel
	clients        []actor.ActorRef
	aggregator     actor.ActorRef
	monitor        actor.ActorRef
	clusterManager actor.ActorRef
	remoteClient   *remote.RemoteClient
	currentRound   int
	roundStart     time.Time
	roundCounter   *crdt.GCounter
	clientSet      *crdt.ORSet[string]
}

func NewCoordinatorActor(cfg CoordinatorConfig, numFeatures int, remoteClient *remote.RemoteClient) *CoordinatorActor {
	return &CoordinatorActor{
		cfg:          cfg,
		globalModel:  model.New(numFeatures),
		roundCounter: crdt.NewGCounter("coordinator"),
		clientSet:    crdt.NewORSet[string](),
		remoteClient: remoteClient,
	}
}

func (c *CoordinatorActor) OnStart(ctx actor.ActorContext) {}
func (c *CoordinatorActor) OnStop()                        {}

func (c *CoordinatorActor) Receive(ctx actor.ActorContext, msg actor.Message) {
	switch m := msg.(type) {
	case StartFederatedLearning:
		c.clients = m.Clients
		c.aggregator = m.Aggregator
		c.monitor = m.Monitor
		for _, ref := range m.Clients {
			c.clientSet.Add(string(ref.ID()))
		}
		c.startRound(ctx)
	case AggregationResult:
		c.globalModel.Weights = m.GlobalWeights
		c.globalModel.Bias = m.GlobalBias
		ctx.Send(c.monitor, SetGlobalModel{Weights: m.GlobalWeights, Bias: m.GlobalBias})
		ctx.Send(c.monitor, RoundComplete{
			RoundID:     m.RoundID,
			GlobalLoss:  m.AggregatedLoss,
			NumClients:  len(c.clients),
			ElapsedTime: time.Since(c.roundStart).Milliseconds(),
		})
	case MetricsReport:
		fmt.Printf("[Coordinator] runda %d — RMSE=%.4f MAE=%.4f R²=%.4f\n",
			m.RoundID, m.RMSE, m.MAE, m.R2Score)
		if m.ConvergenceFlag {
			fmt.Println("[Coordinator] model konvergirao")
			if c.cfg.DoneCh != nil {
				close(c.cfg.DoneCh)
			}
			return
		}
		c.startRound(ctx)
	case actor.ActorFailed:
		fmt.Printf("[Coordinator] aktor %s pao: %s\n", m.ActorID, m.ErrMsg)
	case SetPeers:
		c.aggregator = m.Aggregator
		c.monitor = m.Monitor
		c.clusterManager = m.ClusterManager
	case ClientJoin:
		ref := remote.NewRemoteActorRef(actor.ActorID(m.ClientID), m.Address, c.remoteClient)
		c.clients = append(c.clients, ref)
		c.clientSet.Add(m.ClientID)
		fmt.Printf("[Coordinator] klijent %s se registrovao (%s)\n", m.ClientID, m.Address)
		if c.clusterManager != nil {
			ctx.Send(c.clusterManager, RegisterClient{Ref: ref, FeatureMean: m.FeatureMean, ExpectedTotal: m.ExpectedTotal})
		}
		if len(c.clients) >= m.ExpectedTotal {
			c.startRound(ctx)
		}
	}
}

func (c *CoordinatorActor) startRound(ctx actor.ActorContext) {
	if c.currentRound >= c.cfg.TotalRounds {
		fmt.Println("[Coordinator] sve runde završene")
		if c.cfg.DoneCh != nil {
			close(c.cfg.DoneCh)
		}
		return
	}
	c.currentRound++
	c.roundStart = time.Now()
	c.roundCounter.Increment()

	fmt.Printf("[Coordinator] runda %d/%d\n", c.currentRound, c.cfg.TotalRounds)

	for _, client := range c.clients {
		ctx.Send(client, StartRound{
			RoundID:      c.currentRound,
			ModelWeights: c.globalModel.Weights,
			ModelBias:    c.globalModel.Bias,
			LearningRate: c.cfg.LearningRate,
			Epochs:       c.cfg.Epochs,
		})
	}
}

func (c *CoordinatorActor) GetGlobalModel() *model.LinearModel {
	return c.globalModel
}
