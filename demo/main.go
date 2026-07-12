package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"syscall"
	"time"

	actor "agentskiSistemi/actor-framework"
	"agentskiSistemi/actor-framework/cluster"
	"agentskiSistemi/actor-framework/remote"
	"agentskiSistemi/actor-framework/supervision"
	flactors "agentskiSistemi/federated-learning/actors"
	"agentskiSistemi/federated-learning/data"
	"agentskiSistemi/federated-learning/model"
)

func main() {
	cfgPath := flag.String("config", "demo/config.yaml", "putanja do config.yaml")
	mode := flag.String("mode", "local", "režim rada: local | coordinator | client")
	id := flag.String("id", "", "ID klijenta (mode=client)")
	coordinatorAddr := flag.String("coordinator", "", "gRPC adresa koordinatora, npr. coordinator:50051 (mode=client)")
	listenAddr := flag.String("listen", ":50051", "gRPC adresa na kojoj koordinator sluša (mode=coordinator)")
	clientPort := flag.String("client-port", ":50060", "gRPC port na kome klijent sluša (mode=client)")
	flag.Parse()

	cfg, err := loadConfig(*cfgPath)
	if err != nil {
		log.Fatalf("greška učitavanja konfiguracije: %v", err)
	}

	switch *mode {
	case "coordinator":
		runCoordinator(cfg, *listenAddr)
	case "client":
		runClient(cfg, *id, *coordinatorAddr, *clientPort)
	default:
		runLocal(cfg)
	}
}

func runLocal(cfg *Config) {
	// ── 1. Učitaj dataset ──────────────────────────────────────────────────────
	samples, err := data.Load(cfg.Dataset.Path)
	if err != nil {
		log.Fatalf("greška učitavanja dataseta: %v", err)
	}
	fmt.Printf("Učitano %d uzoraka iz %s\n", len(samples), cfg.Dataset.Path)

	// ── 2. Normalizacija i podela train/validation ─────────────────────────────
	mins, maxs, minTarget, maxTarget := data.Normalize(samples)

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
	coordActor := flactors.NewCoordinatorActor(coordCfg, data.NumFeatures, nil, nil)
	coordinatorRef := sys.Spawn(
		coordActor,
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
	clientStrategy := supervision.NewSupervisor(supervision.NewOneForOne(3, 30*time.Second, nil))
	for i, partition := range partitions {
		X, y := data.ToMatrices(partition)
		id := fmt.Sprintf("client-%d", i+1)
		clientRefs[i] = sys.SpawnWithSupervisor(
			flactors.NewClientActor(id, X, y, aggregatorRef),
			id,
			clientStrategy,
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
		runPredictionDemo(coordActor.GetGlobalModel(), mins, maxs, minTarget, maxTarget)
	case <-sig:
		fmt.Println("\nPrimljen signal za zaustavljanje.")
	}

	sys.Shutdown()
	fmt.Println("Sistem zaustavljen.")
}

// runCoordinator pokreće koordinatorski proces distribuiranog FL treninga:
// hostuje CoordinatorActor/AggregatorActor/MonitorActor/ClusterManagerActor
// i čeka da se klijenti (u odvojenim procesima) registruju preko gRPC-a.
func runCoordinator(cfg *Config, listenAddr string) {
	samples, err := data.Load(cfg.Dataset.Path)
	if err != nil {
		log.Fatalf("greška učitavanja dataseta: %v", err)
	}
	fmt.Printf("Učitano %d uzoraka iz %s\n", len(samples), cfg.Dataset.Path)

	mins, maxs, minTarget, maxTarget := data.Normalize(samples)

	splitIdx := int(float64(len(samples)) * (1 - cfg.Dataset.ValidationSplit))
	valSamples := samples[splitIdx:]
	valX, valY := data.ToMatrices(valSamples)

	sys := actor.NewActorSystem("coordinator")
	doneCh := make(chan struct{})
	remoteClient := remote.NewRemoteClient(nil)

	server := remote.NewRemoteServer(sys)
	if err := server.Start(listenAddr); err != nil {
		log.Fatalf("greška pokretanja gRPC servera: %v", err)
	}

	// Gossip-based membership: coordinator prati klijente preko istog gRPC
	// kanala i otkriva kad neki od njih padne (NodeDead), bez čega bi runda
	// zauvek čekala LocalUpdate od mrtvog klijenta.
	transport := remote.NewGrpcGossipTransport(remoteClient, server)
	advertiseAddr := "coordinator" + listenAddr
	membership := cluster.NewCluster(3, cluster.NodeID("coordinator"), advertiseAddr, transport)

	coordCfg := flactors.CoordinatorConfig{
		TotalRounds:  cfg.FL.NumRounds,
		LearningRate: cfg.FL.LearningRate,
		Epochs:       cfg.FL.Epochs,
		DoneCh:       doneCh,
	}
	coordActor := flactors.NewCoordinatorActor(coordCfg, data.NumFeatures, remoteClient, membership)
	coordinatorRef := sys.Spawn(coordActor, "coordinator")

	membership.Watch(func(n *cluster.NodeInfo) {
		if n.Status == cluster.NodeDead {
			fmt.Printf("[Coordinator] klijent %s izgleda mrtav (gossip)\n", n.ID)
			coordinatorRef.Tell(flactors.ClientDown{ClientID: string(n.ID)})
		}
	})
	membership.Start()

	aggregatorRef := sys.Spawn(flactors.NewAggregatorActor(coordinatorRef, cfg.FL.NumClients), "aggregator")
	monitorRef := sys.Spawn(flactors.NewMonitorActor(coordinatorRef, valX, valY, cfg.FL.ConvergenceThreshold), "monitor")
	clusterMgrRef := sys.Spawn(flactors.NewClusterManagerActor(cfg.FL.NumClusters), "cluster-manager")

	time.Sleep(100 * time.Millisecond) // daj aktorima da završe OnStart
	coordinatorRef.Tell(flactors.SetPeers{Aggregator: aggregatorRef, Monitor: monitorRef, ClusterManager: clusterMgrRef})

	fmt.Printf("[Coordinator] gRPC server sluša na %s, čekam %d klijenata...\n", listenAddr, cfg.FL.NumClients)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-doneCh:
		fmt.Println("\nFL trening završen.")
		runPredictionDemo(coordActor.GetGlobalModel(), mins, maxs, minTarget, maxTarget)
	case <-sig:
		fmt.Println("\nPrimljen signal za zaustavljanje.")
	}

	membership.Stop()
	server.Stop()
	sys.Shutdown()
	fmt.Println("Sistem zaustavljen.")
}

var clientIndexRe = regexp.MustCompile(`(\d+)$`)

// runClient pokreće klijentski proces distribuiranog FL treninga: učitava
// istu particiju podataka koju bi runLocal dodelio ovom klijentu, i
// registruje se kod koordinatora preko gRPC-a.
func runClient(cfg *Config, id string, coordinatorAddr string, listenPort string) {
	if id == "" {
		log.Fatal("--id je obavezan u mode=client")
	}
	if coordinatorAddr == "" {
		log.Fatal("--coordinator je obavezan u mode=client")
	}

	match := clientIndexRe.FindStringSubmatch(id)
	if match == nil {
		log.Fatalf("ne mogu da izvučem indeks klijenta iz --id=%s (očekivan format client-N)", id)
	}
	num, _ := strconv.Atoi(match[1])
	clientIndex := num - 1

	samples, err := data.Load(cfg.Dataset.Path)
	if err != nil {
		log.Fatalf("greška učitavanja dataseta: %v", err)
	}
	data.Normalize(samples)

	splitIdx := int(float64(len(samples)) * (1 - cfg.Dataset.ValidationSplit))
	trainSamples := samples[:splitIdx]

	mode := data.IID
	if cfg.FL.PartitionMode == "non_iid" {
		mode = data.NonIID
	}
	partitions := data.Partition(trainSamples, cfg.FL.NumClients, data.PartitionMode(mode))
	if clientIndex < 0 || clientIndex >= len(partitions) {
		log.Fatalf("indeks klijenta %d van opsega (num_clients=%d)", clientIndex, len(partitions))
	}
	X, y := data.ToMatrices(partitions[clientIndex])
	fmt.Printf("[%s] particija: %d uzoraka\n", id, len(X))

	sys := actor.NewActorSystem(id)
	remoteClient := remote.NewRemoteClient(nil)
	aggregatorRef := remote.NewRemoteActorRef(actor.ActorID("aggregator"), coordinatorAddr, remoteClient)
	clientStrategy := supervision.NewSupervisor(supervision.NewOneForOne(3, 30*time.Second, nil))
	sys.SpawnWithSupervisor(flactors.NewClientActor(id, X, y, aggregatorRef), id, clientStrategy)

	server := remote.NewRemoteServer(sys)
	if err := server.Start(listenPort); err != nil {
		log.Fatalf("greška pokretanja gRPC servera: %v", err)
	}
	advertiseAddr := id + listenPort

	// Gossip-based membership: klijent seed-uje coordinator kao poznati peer
	// (gossip inače nema kome da se javi prvi put) da bi coordinator mogao da
	// detektuje pad ovog klijenta preko istog gRPC kanala.
	transport := remote.NewGrpcGossipTransport(remoteClient, server)
	membership := cluster.NewCluster(3, cluster.NodeID(id), advertiseAddr, transport)
	membership.Seed(cluster.NodeInfo{ID: "coordinator", Address: coordinatorAddr, Status: cluster.NodeAlive})
	membership.Start()

	time.Sleep(100 * time.Millisecond) // daj ClientActor-u da završi OnStart

	fmt.Printf("[%s] registrujem se kod koordinatora %s (adresa: %s)\n", id, coordinatorAddr, advertiseAddr)
	joinMsg := flactors.ClientJoin{
		ClientID:      id,
		Address:       advertiseAddr,
		FeatureMean:   featureMean(X),
		ExpectedTotal: cfg.FL.NumClients,
	}
	const maxAttempts = 30
	var joinErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		joinErr = remoteClient.Tell(coordinatorAddr, actor.ActorID("coordinator"), joinMsg)
		if joinErr == nil {
			break
		}
		time.Sleep(1 * time.Second)
	}
	if joinErr != nil {
		log.Fatalf("[%s] neuspešna registracija kod koordinatora nakon %d pokušaja: %v", id, maxAttempts, joinErr)
	}
	fmt.Printf("[%s] registrovan, čekam instrukcije koordinatora...\n", id)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	fmt.Printf("\n[%s] primljen signal za zaustavljanje.\n", id)

	membership.Stop()
	server.Stop()
	sys.Shutdown()
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

func runPredictionDemo(globalModel *model.LinearModel, mins, maxs []float64, minTarget, maxTarget float64) {
	fmt.Println("\n═══════════════════════════════════════")
	fmt.Println("        DEMO: Predikcija cene")
	fmt.Println("═══════════════════════════════════════")
	// CSV kolone: [longitude, latitude, housing_median_age,
	//              total_rooms, total_bedrooms, population, households, median_income]

	examples := []struct {
		name     string
		features []float64 // sirove vrednosti, isti redosled kao CSV
		actual   float64   // poznata cena u USD
	}{
		{
			// Tačno red 1 iz housing.csv
			name:     "San Francisco, Near Bay",
			features: []float64{-122.23, 37.88, 41.0, 880.0, 129.0, 322.0, 126.0, 8.3252},
			actual:   452600,
		},
		{
			// Tipična kuća u Los Angelesu
			name:     "Los Angeles, prosečna zona",
			features: []float64{-118.25, 34.05, 25.0, 3200.0, 620.0, 1500.0, 570.0, 3.10},
			actual:   198000,
		},
		{
			// Ruralna oblast, San Joaquin Valley
			name:     "San Joaquin Valley, ruralna oblast",
			features: []float64{-119.50, 36.50, 20.0, 1200.0, 250.0, 620.0, 200.0, 1.50},
			actual:   87500,
		},
	}

	for _, ex := range examples {
		// Normalizuj sirove feature vrednosti istim min/max kao trening podaci
		normalized := make([]float64, len(ex.features))
		for j, f := range ex.features {
			r := maxs[j] - mins[j]
			if r > 0 {
				normalized[j] = (f - mins[j]) / r
			}
		}

		// Model predviđa normalizovani target ∈ [0,1]
		// Denormalizacija: pred × (maxTarget - minTarget) + minTarget
		predNorm := globalModel.Predict(normalized)
		predicted := predNorm*(maxTarget-minTarget) + minTarget
		errPct := (predicted - ex.actual) / ex.actual * 100

		fmt.Printf("\n%s\n", ex.name)
		fmt.Printf("   Predikcija : $%.0f\n", predicted)
		fmt.Printf("   Stvarna    : $%.0f\n", ex.actual)
		fmt.Printf("   Greška     : %+.1f%%\n", errPct)
	}
}
