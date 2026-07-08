package actors

import (
	"agentskiSistemi/actor-framework"
	"agentskiSistemi/actor-framework/crdt"
	"agentskiSistemi/federated-learning/model"
	"fmt"
)

type AggregatorActor struct {
	coordinator    actor.ActorRef
	numClients     int
	updates        []model.ClientUpdate
	currentRoundID int
	modelRegister  *crdt.LWWRegister[[]float64]
}

func NewAggregatorActor(coordinator actor.ActorRef, numClients int) *AggregatorActor {
	return &AggregatorActor{
		coordinator:    coordinator,
		numClients:     numClients,
		updates:        make([]model.ClientUpdate, 0, numClients),
		currentRoundID: 0,
		modelRegister:  crdt.NewLWWRegister[[]float64](),
	}
}

func (a *AggregatorActor) OnStart(ctx actor.ActorContext) {}
func (a *AggregatorActor) OnStop()                        {}

func (a *AggregatorActor) Receive(ctx actor.ActorContext, msg actor.Message) {
	switch m := msg.(type) {
	case LocalUpdate:
		if m.RoundID != a.currentRoundID {
			a.updates = make([]model.ClientUpdate, 0, a.numClients)
			a.currentRoundID = m.RoundID
		}
		a.updates = append(a.updates, model.ClientUpdate{
			ClientID:   m.ClientID,
			Weights:    m.Weights,
			Bias:       m.Bias,
			NumSamples: m.NumSamples,
			Loss:       m.Loss,
		})
		if len(a.updates) >= a.numClients {
			a.aggregate(ctx)
		}
	}
}

func (a *AggregatorActor) aggregate(ctx actor.ActorContext) {
	globalModel := model.FedAvg(a.updates)
	a.modelRegister.Set(globalModel.Weights)
	var totalLoss float64
	for _, u := range a.updates {
		totalLoss += u.Loss
	}
	avgLoss := totalLoss / float64(len(a.updates))

	fmt.Printf("[Aggregator] runda %d agregirana, avg_loss=%.4f\n", a.currentRoundID, avgLoss)

	ctx.Send(a.coordinator, AggregationResult{
		RoundID:        a.currentRoundID,
		GlobalWeights:  globalModel.Weights,
		GlobalBias:     globalModel.Bias,
		AggregatedLoss: avgLoss,
	})
	a.updates = nil
}
