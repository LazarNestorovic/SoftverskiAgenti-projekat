package actors

import (
	"agentskiSistemi/actor-framework"
	"agentskiSistemi/federated-learning/model"
	"fmt"
)

type ClientActor struct {
	clientID   string
	X          [][]float64
	y          []float64
	aggregator actor.ActorRef
}

func NewClientActor(clientID string, X [][]float64, y []float64, aggregator actor.ActorRef) *ClientActor {
	return &ClientActor{
		clientID:   clientID,
		X:          X,
		y:          y,
		aggregator: aggregator,
	}
}

func (c *ClientActor) OnStart(ctx actor.ActorContext) {}
func (c *ClientActor) OnStop()                        {}

func (c *ClientActor) Receive(ctx actor.ActorContext, msg actor.Message) {
	switch m := msg.(type) {
	case StartRound:
		local := model.New(len(m.ModelWeights))
		copy(local.Weights, m.ModelWeights)
		local.Bias = m.ModelBias

		loss := local.Train(c.X, c.y, m.Epochs, m.LearningRate)
		fmt.Printf("[Client %s] runda %d, loss=%.4f\n", c.clientID, m.RoundID, loss)

		ctx.Send(c.aggregator, LocalUpdate{
			c.clientID,
			m.RoundID,
			local.Weights,
			local.Bias,
			len(c.X),
			loss,
		})
	case ClusterAssign:
		fmt.Printf("[Client %s] klaster %d\n", c.clientID, m.ClusterID)
	}
}
