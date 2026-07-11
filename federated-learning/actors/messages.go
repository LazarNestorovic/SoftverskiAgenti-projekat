package actors

import "agentskiSistemi/actor-framework"

type StartRound struct {
	RoundID      int
	ModelWeights []float64
	ModelBias    float64
	LearningRate float64
	Epochs       int
}

type LocalUpdate struct {
	ClientID   string
	RoundID    int
	Weights    []float64
	Bias       float64
	NumSamples int
	Loss       float64
}

type AggregationResult struct {
	RoundID        int
	GlobalWeights  []float64
	GlobalBias     float64
	AggregatedLoss float64
}

type RoundComplete struct {
	RoundID     int
	GlobalLoss  float64
	NumClients  int
	ElapsedTime int64
}

type SetGlobalModel struct {
	Weights []float64
	Bias    float64
}

type MetricsReport struct {
	RoundID         int
	RMSE            float64
	MAE             float64
	R2Score         float64
	ConvergenceFlag bool
}

type ClusterAssign struct {
	ClusterID            int
	ClusterMembers       []string
	ClusterCoordinatorID string
}

type SetCoordinator struct{ Ref actor.ActorRef }

type RegisterClient struct {
	Ref           actor.ActorRef
	FeatureMean   []float64
	ExpectedTotal int
}

type StartFederatedLearning struct {
	Clients    []actor.ActorRef
	Aggregator actor.ActorRef
	Monitor    actor.ActorRef
}

// ClientJoin je poruka koju client proces šalje koordinatoru preko gRPC-a
// da bi se registrovao za distribuirani trening (mora biti bez ActorRef polja
// da bi bila JSON-serijalizabilna preko remote sloja).
type ClientJoin struct {
	ClientID      string
	Address       string
	FeatureMean   []float64
	ExpectedTotal int
}

// SetPeers povezuje CoordinatorActor sa aggregator/monitor/cluster-manager
// akterima u distribuiranom modu, bez pokretanja treninga (ostaje in-process).
type SetPeers struct {
	Aggregator     actor.ActorRef
	Monitor        actor.ActorRef
	ClusterManager actor.ActorRef
}
