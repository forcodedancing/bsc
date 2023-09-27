// Copyright 2014 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

// Package miner implements Ethereum block creation and mining.
package miner

import (
	"context"
	"crypto/tls"
	"fmt"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/blxr/version"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/eth/downloader"
	"github.com/ethereum/go-ethereum/event"
	pb "github.com/ethereum/go-ethereum/grpc/protobuf"
	"github.com/ethereum/go-ethereum/internal/ethapi"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// Backend wraps all methods required for mining. Only full node is capable
// to offer all the functions here.
type Backend interface {
	BlockChain() *core.BlockChain
	TxPool() *core.TxPool
}

type ClientMap map[string]*rpc.Client
type ClientGrpcMap map[string]pb.ProposerClient

type ClientMapping struct {
	mx            *sync.RWMutex
	clientMap     ClientMap
	clientGrpcMap ClientGrpcMap
}

func NewClientMap(relays, relaysGRPC []string) *ClientMapping {
	c := &ClientMapping{
		mx:            new(sync.RWMutex),
		clientMap:     make(ClientMap),
		clientGrpcMap: make(ClientGrpcMap),
	}

	for _, endpoint := range relaysGRPC {
		tlsCfg := &tls.Config{InsecureSkipVerify: true}
		tlsCred := credentials.NewTLS(tlsCfg)
		conn, err := grpc.Dial(endpoint, grpc.WithTransportCredentials(tlsCred))
		if err != nil {
			log.Warn("Failed to dial MEV GRPC relay", "dest", endpoint, "err", err)
			continue
		}

		c.clientGrpcMap[endpoint] = pb.NewProposerClient(conn)
	}

	for _, relay := range relays {
		client, err := rpc.Dial(relay)
		if err != nil {
			log.Warn("Failed to dial MEV relay", "dest", relay, "err", err)
			continue
		}

		c.clientMap[relay] = client
	}

	return c
}

func (c *ClientMapping) Len() int {
	c.mx.RLock()
	defer c.mx.RUnlock()
	return len(c.clientMap)
}

func (c *ClientMapping) Mapping() ClientMap {
	clientMap := make(ClientMap, len(c.clientMap))

	c.mx.RLock()
	for k, v := range c.clientMap {
		clientMap[k] = v
	}
	c.mx.RUnlock()

	return clientMap
}

func (c *ClientMapping) Get(relay string) (*rpc.Client, bool) {
	c.mx.RLock()
	client, ok := c.clientMap[relay]
	c.mx.RUnlock()

	return client, ok
}

func (c *ClientMapping) Add(relay string) (*rpc.Client, error) {
	c.mx.Lock()
	defer c.mx.Unlock()

	client, err := rpc.Dial(relay)
	if err != nil {
		return nil, err
	}

	c.clientMap[relay] = client

	return client, nil
}

func (c *ClientMapping) Remove(relay string) error {
	c.mx.Lock()
	defer c.mx.Unlock()

	if _, ok := c.clientMap[relay]; !ok {
		return fmt.Errorf("relay %s not found", relay)
	}

	delete(c.clientMap, relay)

	return nil
}

func (c *ClientMapping) MappingGRPC() ClientGrpcMap {
	clientGrpcMap := make(ClientGrpcMap, len(c.clientGrpcMap))

	c.mx.RLock()
	for k, v := range c.clientGrpcMap {
		clientGrpcMap[k] = v
	}
	c.mx.RUnlock()

	return clientGrpcMap
}

func (c *ClientMapping) LenGRPC() int {
	c.mx.RLock()
	defer c.mx.RUnlock()
	return len(c.clientGrpcMap)
}

func (c *ClientMapping) AddGrpc(relay string) (pb.ProposerClient, error) {
	c.mx.Lock()
	defer c.mx.Unlock()

	tlsCfg := &tls.Config{InsecureSkipVerify: true}
	tlsCred := credentials.NewTLS(tlsCfg)
	conn, err := grpc.Dial(relay, grpc.WithTransportCredentials(tlsCred))
	if err != nil {
		log.Warn("Failed to dial MEV GRPC relay", "dest", relay, "err", err)
		return nil, err
	}
	client := pb.NewProposerClient(conn)
	c.clientGrpcMap[relay] = pb.NewProposerClient(conn)

	return client, nil
}

func (c *ClientMapping) GetGRPC(relay string) (pb.ProposerClient, bool) {
	c.mx.RLock()
	client, ok := c.clientGrpcMap[relay]
	c.mx.RUnlock()

	return client, ok
}

func (c *ClientMapping) RemoveGrpc(relay string) error {
	c.mx.Lock()
	defer c.mx.Unlock()

	if _, ok := c.clientGrpcMap[relay]; !ok {
		return fmt.Errorf("relay grpc %s not found", relay)
	}

	delete(c.clientGrpcMap, relay)

	return nil
}

// Config is the configuration parameters of mining.
type Config struct {
	Etherbase              common.Address `toml:",omitempty"` // Public address for block mining rewards (default = first account)
	Notify                 []string       `toml:",omitempty"` // HTTP URL list to be notified of new work packages (only useful in ethash).
	NotifyFull             bool           `toml:",omitempty"` // Notify with pending block headers instead of work packages
	ExtraData              hexutil.Bytes  `toml:",omitempty"` // Block extra data set by the miner
	DelayLeftOver          time.Duration  // Time reserved to finalize a block(calculate root, distribute income...)
	GasFloor               uint64         // Target gas floor for mined blocks.
	GasCeil                uint64         // Target gas ceiling for mined blocks.
	GasPrice               *big.Int       // Minimum gas price for mining a transaction
	Recommit               time.Duration  // The time interval for miner to re-create mining work.
	Noverify               bool           // Disable remote mining solution verification(only useful in ethash).
	VoteEnable             bool           // Whether to vote when mining
	DisableVoteAttestation bool           // Whether to skip assembling vote attestation

	MEVRelays                   []string `toml:",omitempty"` // RPC clients to register validator each epoch
	ProposedBlockUri            string   `toml:",omitempty"` // received proposedBlocks on that uri
	MevRelaysGRPC               []string `toml:",omitempty"` // gRPC clients to register validator each epoch
	ProposedBlockGrpcUri        string   `toml:",omitempty"` // received proposedBlocks on that grpc uri
	ProposedBlockNamespace      string   `toml:",omitempty"` // define the namespace of proposedBlock
	RegisterValidatorSignedHash []byte   `toml:"-"`          // signed value of crypto.Keccak256([]byte(ProposedBlockUri))
}

// Miner creates blocks and searches for proof-of-work values.
type Miner struct {
	mux      *event.TypeMux
	worker   *worker
	coinbase common.Address
	eth      Backend
	engine   consensus.Engine
	exitCh   chan struct{}
	startCh  chan common.Address
	stopCh   chan struct{}

	wg sync.WaitGroup

	mevRelays              *ClientMapping
	proposedBlockUri       string
	proposedBlockGrpcUri   string
	proposedBlockNamespace string
	signedProposedBlockUri []byte
}

func New(eth Backend, config *Config, chainConfig *params.ChainConfig, mux *event.TypeMux, engine consensus.Engine, isLocalBlock func(header *types.Header) bool) *Miner {
	miner := &Miner{
		eth:     eth,
		mux:     mux,
		engine:  engine,
		exitCh:  make(chan struct{}),
		startCh: make(chan common.Address),
		stopCh:  make(chan struct{}),
		worker:  newWorker(config, chainConfig, engine, eth, mux, isLocalBlock, false),

		mevRelays:              NewClientMap(config.MEVRelays, config.MevRelaysGRPC),
		proposedBlockUri:       config.ProposedBlockUri,
		proposedBlockGrpcUri:   config.ProposedBlockGrpcUri,
		proposedBlockNamespace: config.ProposedBlockNamespace,
		signedProposedBlockUri: config.RegisterValidatorSignedHash,
	}
	miner.wg.Add(1)
	go miner.update()
	return miner
}

// update keeps track of the downloader events. Please be aware that this is a one shot type of update loop.
// It's entered once and as soon as `Done` or `Failed` has been broadcasted the events are unregistered and
// the loop is exited. This to prevent a major security vuln where external parties can DOS you with blocks
// and halt your mining operation for as long as the DOS continues.
func (miner *Miner) update() {
	defer miner.wg.Done()

	events := miner.mux.Subscribe(downloader.StartEvent{}, downloader.DoneEvent{}, downloader.FailedEvent{})
	defer func() {
		if !events.Closed() {
			events.Unsubscribe()
		}
	}()

	chainBlockCh := make(chan core.ChainHeadEvent, chainHeadChanSize)

	chainBlockSub := miner.eth.BlockChain().SubscribeChainBlockEvent(chainBlockCh)
	defer chainBlockSub.Unsubscribe()

	shouldStart := false
	canStart := true
	dlEventCh := events.Chan()

	// miner started at the middle of an epoch, we want to register it
	miner.registerValidator()

	for {
		select {
		case ev := <-dlEventCh:
			if ev == nil {
				// Unsubscription done, stop listening
				dlEventCh = nil
				continue
			}
			switch ev.Data.(type) {
			case downloader.StartEvent:
				wasMining := miner.Mining()
				miner.worker.stop()
				canStart = false
				if wasMining {
					// Resume mining after sync was finished
					shouldStart = true
					log.Info("Mining aborted due to sync")
				}
			case downloader.FailedEvent:
				canStart = true
				if shouldStart {
					miner.SetEtherbase(miner.coinbase)
					miner.worker.start()
				}
			case downloader.DoneEvent:
				canStart = true
				if shouldStart {
					miner.SetEtherbase(miner.coinbase)
					miner.worker.start()
				}
				// Stop reacting to downloader events
				events.Unsubscribe()
			}
		case addr := <-miner.startCh:
			miner.SetEtherbase(addr)
			if canStart {
				miner.worker.start()
			}
			shouldStart = true

		case block := <-chainBlockCh:
			// ToDo check if epoch, if so send eth_registerValidator to list of Relays
			if block.Block.NumberU64()%params.BSCChainConfig.Parlia.Epoch == 0 {
				miner.registerValidator()
			}
		case <-miner.stopCh:
			shouldStart = false
			miner.worker.stop()
		case <-miner.exitCh:
			miner.worker.close()
			return
		case <-chainBlockSub.Err():
			return
		}
	}
}

func (miner *Miner) Start(coinbase common.Address) {
	miner.startCh <- coinbase
}

func (miner *Miner) Stop() {
	miner.stopCh <- struct{}{}
}

func (miner *Miner) Close() {
	close(miner.exitCh)
	miner.wg.Wait()
}

func (miner *Miner) Mining() bool {
	return miner.worker.isRunning()
}

func (miner *Miner) Hashrate() uint64 {
	if pow, ok := miner.engine.(consensus.PoW); ok {
		return uint64(pow.Hashrate())
	}
	return 0
}

func (miner *Miner) SetExtra(extra []byte) error {
	if uint64(len(extra)) > params.MaximumExtraDataSize {
		return fmt.Errorf("extra exceeds max length. %d > %v", len(extra), params.MaximumExtraDataSize)
	}
	miner.worker.setExtra(extra)
	return nil
}

// SetRecommitInterval sets the interval for sealing work resubmitting.
func (miner *Miner) SetRecommitInterval(interval time.Duration) {
	miner.worker.setRecommitInterval(interval)
}

// Pending returns the currently pending block and associated state.
func (miner *Miner) Pending() (*types.Block, *state.StateDB) {
	if miner.worker.isRunning() {
		pendingBlock, pendingState := miner.worker.pending()
		if pendingState != nil && pendingBlock != nil {
			return pendingBlock, pendingState
		}
	}
	// fallback to latest block
	block := miner.worker.chain.CurrentBlock()
	if block == nil {
		return nil, nil
	}
	stateDb, err := miner.worker.chain.StateAt(block.Root())
	if err != nil {
		return nil, nil
	}
	return block, stateDb
}

// PendingBlock returns the currently pending block.
//
// Note, to access both the pending block and the pending state
// simultaneously, please use Pending(), as the pending state can
// change between multiple method calls
func (miner *Miner) PendingBlock() *types.Block {
	if miner.worker.isRunning() {
		pendingBlock := miner.worker.pendingBlock()
		if pendingBlock != nil {
			return pendingBlock
		}
	}
	// fallback to latest block
	return miner.worker.chain.CurrentBlock()
}

// PendingBlockAndReceipts returns the currently pending block and corresponding receipts.
func (miner *Miner) PendingBlockAndReceipts() (*types.Block, types.Receipts) {
	return miner.worker.pendingBlockAndReceipts()
}

func (miner *Miner) SetEtherbase(addr common.Address) {
	miner.coinbase = addr
	miner.worker.setEtherbase(addr)
}

// SetGasCeil sets the gaslimit to strive for when mining blocks post 1559.
// For pre-1559 blocks, it sets the ceiling.
func (miner *Miner) SetGasCeil(ceil uint64) {
	miner.worker.setGasCeil(ceil)
}

// GetSealingBlock retrieves a sealing block based on the given parameters.
// The returned block is not sealed but all other fields should be filled.
func (miner *Miner) GetSealingBlock(parent common.Hash, timestamp uint64, coinbase common.Address, random common.Hash) (*types.Block, error) {
	return miner.worker.getSealingBlock(parent, timestamp, coinbase, random)
}

// SubscribePendingLogs starts delivering logs from pending transactions
// to the given channel.
func (miner *Miner) SubscribePendingLogs(ch chan<- []*types.Log) event.Subscription {
	return miner.worker.pendingLogsFeed.Subscribe(ch)
}

// ProposedBlock add the block to the list of works
func (miner *Miner) ProposedBlock(ctx context.Context, mevRelay string, blockNumber *big.Int, prevBlockHash common.Hash, reward *big.Int, gasLimit uint64, gasUsed uint64, txs types.Transactions, unReverted map[common.Hash]struct{}) (simDuration time.Duration, err error) {
	var (
		isBlockSkipped bool
		simWork        *bestProposedWork
	)

	endOfProposingWindow := time.Unix(int64(miner.eth.BlockChain().CurrentBlock().Time()+miner.worker.chainConfig.Parlia.Period), 0).Add(-miner.worker.config.DelayLeftOver)

	timeout := time.Until(endOfProposingWindow)
	if timeout <= 0 {
		err = fmt.Errorf("proposed block is too late, end of proposing window %s, appeared %s later", endOfProposingWindow, common.PrettyDuration(timeout))
		return
	}

	proposingCtx, proposingCancel := context.WithTimeout(ctx, timeout)
	defer proposingCancel()

	currentGasLimit := atomic.LoadUint64(miner.worker.currentGasLimit)
	previousBlockGasLimit := atomic.LoadUint64(miner.worker.prevBlockGasLimit)
	defer func() {
		logCtx := []any{
			"blockNumber", blockNumber,
			"mevRelay", mevRelay,
			"prevBlockHash", prevBlockHash.Hex(),
			"proposedReward", reward,
			"gasLimit", gasLimit,
			"gasUsed", gasUsed,
			"txCount", len(txs),
			"unRevertedCount", len(unReverted),
			"isBlockSkipped", isBlockSkipped,
			"currentGasLimit", currentGasLimit,
			"timestamp", time.Now().UTC().Format(timestampFormat),
			"simDuration", simDuration,
		}

		if err != nil {
			logCtx = append(logCtx, "err", err)
		}

		log.Debug("Received proposedBlock", logCtx...)
	}()
	isBlockSkipped = gasUsed > currentGasLimit
	if isBlockSkipped {
		err = fmt.Errorf("proposed block gasUsed %v exceeds the current block gas limit %v", gasUsed, currentGasLimit)
		return
	}
	desiredGasLimit := core.CalcGasLimit(previousBlockGasLimit, miner.worker.config.GasCeil)
	if desiredGasLimit != gasLimit {
		log.Warn("proposedBlock has wrong gasLimit", "MEVRelay", mevRelay, "blockNumber", blockNumber, "validatorGasLimit", desiredGasLimit, "proposedBlockGasLimit", gasLimit)
		err = fmt.Errorf("proposed block gasLimit %v is different than the validator gasLimit %v", gasLimit, desiredGasLimit)
		return
	}
	args := &ProposedBlockArgs{
		mevRelay:      mevRelay,
		blockNumber:   blockNumber,
		prevBlockHash: prevBlockHash,
		blockReward:   reward,
		gasLimit:      gasLimit,
		gasUsed:       gasUsed,
		txs:           txs,
		unReverted:    unReverted,
	}
	simWork, simDuration, err = miner.worker.simulateProposedBlock(proposingCtx, args)
	if err != nil {
		err = fmt.Errorf("processing and simulating proposedBlock failed, %v", err)
		return
	}
	if simWork == nil {
		//  do not return error, when the block is skipped
		return
	}

	select {
	case <-proposingCtx.Done():
		err = errors.WithMessage(proposingCtx.Err(), "failed to propose block due to context timeout")
		return
	case miner.worker.proposedCh <- &ProposedBlock{args: args, simulatedWork: simWork, simDuration: simDuration}:
		return
	}
}

func (miner *Miner) registerValidator() {

	if miner.proposedBlockGrpcUri != "" && miner.mevRelays.LenGRPC() != 0 {
		miner.registerValidatorViaGRPC()
		return // do not proceed if grpc enabled
	}

	log.Info("register validator via RPC to MEV relays")
	registerValidatorArgs := &ethapi.RegisterValidatorArgs{
		Data:       []byte(miner.proposedBlockUri),
		Signature:  miner.signedProposedBlockUri,
		Namespace:  miner.proposedBlockNamespace,
		CommitHash: version.CommitHash(),
		GasCeil:    miner.worker.config.GasCeil,
	}
	for dest, destClient := range miner.mevRelays.Mapping() {
		go func(dest string, destinationClient *rpc.Client, registerValidatorArgs *ethapi.RegisterValidatorArgs) {
			var result any

			if err := destinationClient.Call(
				&result, "eth_registerValidator", registerValidatorArgs,
			); err != nil {
				log.Warn("Failed to register validator to MEV relay", "dest", dest, "err", err)
				return
			}

			log.Debug("register validator to MEV relay", "dest", dest, "result", result)
		}(dest, destClient, registerValidatorArgs)
	}
}

func (miner *Miner) registerValidatorViaGRPC() {
	log.Info("register validator via gRPC to MEV relays")
	registerValidatorArgs := &pb.RegisterValidatorRequest{
		Data:       []byte(miner.proposedBlockGrpcUri),
		Signature:  miner.signedProposedBlockUri,
		Namespace:  miner.proposedBlockNamespace,
		CommitHash: version.CommitHash(),
	}
	for dest, destClient := range miner.mevRelays.MappingGRPC() {
		go func(dest string, destClient pb.ProposerClient, request *pb.RegisterValidatorRequest) {

			_, err := destClient.RegisterValidator(context.Background(), request)
			if err != nil {
				log.Warn("Failed to register validator to MEV relay", "dest", dest, "err", err)
				return
			}
		}(dest, destClient, registerValidatorArgs)
	}
}

func (miner *Miner) AddRelay(relay string) error {
	client, err := miner.mevRelays.Add(relay)
	if err != nil {
		return err
	}

	log.Info("register validator to MEV relay", "dest", relay)
	registerValidatorArgs := &ethapi.RegisterValidatorArgs{
		Data:       []byte(miner.proposedBlockUri),
		Signature:  miner.signedProposedBlockUri,
		Namespace:  miner.proposedBlockNamespace,
		CommitHash: version.CommitHash(),
		GasCeil:    miner.worker.config.GasCeil,
	}

	var result any

	if err = client.Call(
		&result, "eth_registerValidator", registerValidatorArgs,
	); err != nil {
		log.Warn("Failed to register validator to MEV relay", "dest", relay, "err", err)
		return err
	}

	log.Debug("register validator to MEV relay", "dest", relay, "result", result)

	return nil
}

func (miner *Miner) RemoveRelay(relay string) error {
	return miner.mevRelays.Remove(relay)
}
