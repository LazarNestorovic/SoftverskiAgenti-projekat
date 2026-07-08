package actors

import (
	"agentskiSistemi/actor-framework"
	"agentskiSistemi/actor-framework/crdt"
	"agentskiSistemi/federated-learning/evaluation"
	"agentskiSistemi/federated-learning/model"
	"fmt"
)

type MonitorActor struct {
	coordinator actor.ActorRef
	globalModel *model.LinearModel
	valX        [][]float64
	valY        []float64
	metricsLog  *crdt.GSet[evaluation.RoundMetric]
	threshold   float64
}

func NewMonitorActor(coordinator actor.ActorRef, valX [][]float64, valY []float64, threshold float64) *MonitorActor {
	return &MonitorActor{
		coordinator: coordinator,
		valX:        valX,
		valY:        valY,
		metricsLog:  crdt.NewGSet[evaluation.RoundMetric](),
		threshold:   threshold,
	}
}

func (m *MonitorActor) OnStart(ctx actor.ActorContext) {}
func (m *MonitorActor) OnStop()                        {}

func (m *MonitorActor) Receive(ctx actor.ActorContext, msg actor.Message) {
	switch msg := msg.(type) {
	case SetGlobalModel:
		if m.globalModel == nil {
			m.globalModel = model.New(len(msg.Weights))
		}
		copy(m.globalModel.Weights, msg.Weights)
		m.globalModel.Bias = msg.Bias
	case RoundComplete:
		if m.globalModel == nil || len(m.valX) == 0 {
			return
		}
		pred := make([]float64, len(m.valX))
		for i, x := range m.valX {
			pred[i] = m.globalModel.Predict(x)
		}

		rmse := evaluation.RMSE(pred, m.valY)
		mae := evaluation.MAE(pred, m.valY)
		r2 := evaluation.R2(pred, m.valY)
		converged := rmse < m.threshold

		m.metricsLog.Add(evaluation.RoundMetric{
			RoundID: msg.RoundID, RMSE: rmse, MAE: mae, R2: r2, Loss: msg.GlobalLoss,
		})

		fmt.Printf("[Monitor] runda %d — RMSE=%.4f MAE=%.4f R²=%.4f (%dms)\n",
			msg.RoundID, rmse, mae, r2, msg.ElapsedTime)

		ctx.Send(m.coordinator, MetricsReport{
			RoundID: msg.RoundID, RMSE: rmse, MAE: mae,
			R2Score: r2, ConvergenceFlag: converged,
		})
	}
}
