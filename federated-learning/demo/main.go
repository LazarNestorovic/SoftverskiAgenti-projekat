package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	actor "agentskiSistemi/actor-framework"
	flactors "agentskiSistemi/federated-learning/actors"
	"agentskiSistemi/federated-learning/data"
)

func main() {
	cfgPath := flag.String("config", "demo/config.yaml", "putanja do config.yaml")
	flag.Parse()

	cfg, err := loadConfig(*cfgPath)
	if err != nil {
		log.Fatalf("greška učitavanja konfiguracije: %v", err)
	}

	runLocal(cfg)
}

func runLocal(cfg *Config) {
	// ── 1. Učitaj dataset ──────────────────────────────────────────────────────
	samples, err := data.Load(cfg.Dataset.Path)
	if err != nil {
		log.Fatalf("greška učitavanja dataseta: %v", err)
	}
	fmt.Printf("Učitano %d uzoraka iz %s\n", len(samples), cfg.Dataset.Path)

	// ── 2. Normalizacija i podela train/validation ─────────────────────────────
	data.Normalize(samples)

	splitIdx := int(float64(len(samples)) * (1 - cfg.Dataset.ValidationSplit))
	trainSamples := samples[:splitIdx]
	valSamples := samples[splitIdx:]
	valX, valY := data.ToMatrices(valSamples)

	fmt.Printf("Train: %d | Validation: %d\n", len(trainSamples), len(valSamples))

	// ── 3. Particija trening podataka po klijentima ────────────────────────────
	mode := data.IID
	if cfg.FL.PartitionMode == "non_iid" {
		mode = data.NonIID
	}
	partitions := data.Partition(trainSamples, cfg.FL.NumClients, data.PartitionMode(mode))

	// ── 4. Kreiraj ActorSystem ─────────────────────────────────────────────────
	sys := actor.NewActorSystem("federated-learning")
	doneCh := make(chan struct{})

	// ── 5. Spawn koordinator ───────────────────────────────────────────────────
	coordCfg := flactors.CoordinatorConfig{
		TotalRounds:  cfg.FL.NumRounds,
		LearningRate: cfg.FL.LearningRate,
		Epochs:       cfg.FL.Epochs,
		DoneCh:       doneCh,
	}
	coordinatorRef := sys.Spawn(
		flactors.NewCoordinatorActor(coordCfg, data.NumFeatures),
		"coordinator",
	)

	// ── 6. Spawn aggregator i monitor ──────────────────────────────────────────
	aggregatorRef := sys.Spawn(
		flactors.NewAggregatorActor(coordinatorRef, cfg.FL.NumClients),
		"aggregator",
	)
	monitorRef := sys.Spawn(
		flactors.NewMonitorActor(coordinatorRef, valX, valY, cfg.FL.ConvergenceThreshold),
		"monitor",
	)

	// ── 7. Spawn klijenata ─────────────────────────────────────────────────────
	clientRefs := make([]actor.ActorRef, cfg.FL.NumClients)
	for i, partition := range partitions {
		X, y := data.ToMatrices(partition)
		id := fmt.Sprintf("client-%d", i+1)
		clientRefs[i] = sys.Spawn(
			flactors.NewClientActor(id, X, y, aggregatorRef),
			id,
		)
		fmt.Printf("  [%s] spawned — %d uzoraka\n", id, len(X))
	}

	// ── 8. Spawn ClusterManagerActor ──────────────────────────────────────────
	clusterMgr := sys.Spawn(
		flactors.NewClusterManagerActor(cfg.FL.NumClusters),
		"cluster-manager",
	)

	// Registruj klijente u ClusterManager (feature mean = prosek prvog feature-a)
	for i, ref := range clientRefs {
		X, _ := data.ToMatrices(partitions[i])
		mean := featureMean(X)
		clusterMgr.Tell(flactors.RegisterClient{
			Ref:           ref,
			FeatureMean:   mean,
			ExpectedTotal: cfg.FL.NumClients,
		})
	}

	// ── 9. Pokreni FL ─────────────────────────────────────────────────────────
	time.Sleep(100 * time.Millisecond) // daj aktorima da završe OnStart
	fmt.Println("\nPokrećem Federated Learning...")
	fmt.Printf("Konfiguracija: %d klijenata | %d rundi | lr=%.4f | %d epoha\n\n",
		cfg.FL.NumClients, cfg.FL.NumRounds, cfg.FL.LearningRate, cfg.FL.Epochs)

	coordinatorRef.Tell(flactors.StartFederatedLearning{
		Clients:    clientRefs,
		Aggregator: aggregatorRef,
		Monitor:    monitorRef,
	})

	// ── 10. Čekaj kraj ili SIGINT ──────────────────────────────────────────────
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-doneCh:
		fmt.Println("\nFL trening završen.")
	case <-sig:
		fmt.Println("\nPrimljen signal za zaustavljanje.")
	}

	sys.Shutdown()
	fmt.Println("Sistem zaustavljen.")
}

// featureMean računa prosek po svakom feature-u (za K-Means).
func featureMean(X [][]float64) []float64 {
	if len(X) == 0 {
		return nil
	}
	mean := make([]float64, len(X[0]))
	for _, x := range X {
		for j, f := range x {
			mean[j] += f
		}
	}
	for j := range mean {
		mean[j] /= float64(len(X))
	}
	return mean
}
