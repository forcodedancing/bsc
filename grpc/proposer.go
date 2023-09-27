package grpc

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	pb "github.com/ethereum/go-ethereum/grpc/protobuf"
	"github.com/ethereum/go-ethereum/internal/ethapi"
	"github.com/ethereum/go-ethereum/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	// timestamp format
	timestampFormat = "2006-01-02 15:04:05.000000"
)

var _ pb.ProposerServer = (*Proposer)(nil)

type Proposer struct {
	backend ethapi.Backend
	pb.UnimplementedProposerServer
}

func NewProposer(backend ethapi.Backend) *Proposer {
	return &Proposer{backend: backend}
}

func (p *Proposer) ProposeBlock(ctx context.Context, in *pb.ProposeBlockRequest) (*pb.ProposeBlockResponse, error) {

	var (
		receivedAt = time.Now()
		txs        types.Transactions
	)
	if len(in.Payload) == 0 {
		return nil, errors.New("proposed block missing txs")
	}
	if in.BlockNumber == 0 {
		return nil, errors.New("proposed block missing blockNumber")
	}

	blockOnChain := p.backend.CurrentBlock()
	proposedBlockNumber := new(big.Int).SetUint64(in.GetBlockNumber())

	if proposedBlockNumber.Cmp(blockOnChain.Number()) < 1 {
		log.Info("Validating ProposedBlock failed", "blockNumber", in.GetBlockNumber(), "onChainBlockNumber", blockOnChain.Number(), "onChainBlockHash", blockOnChain.Hash(), "prevBlockHash", in.GetPrevBlockHash(), "mevRelay", in.GetMevRelay())
		return nil, fmt.Errorf("proposed block contains incorrect blockNumber is incorrect. proposedBlockNumber: %v onChainBlockNumber: %v onChainBlockHash %v", in.GetBlockNumber(), blockOnChain.Number(), blockOnChain.Hash().String())
	}
	for _, encodedTx := range in.Payload {
		tx := new(types.Transaction)
		if err := tx.UnmarshalBinary(encodedTx); err != nil {
			return nil, err
		}
		txs = append(txs, tx)
	}

	var unRevertedHashes = make(map[common.Hash]struct{})

	simDuration, err := p.backend.ProposedBlock(ctx, in.MevRelay, new(big.Int).SetUint64(in.GetBlockNumber()), common.HexToHash(in.GetPrevBlockHash()), new(big.Int).SetUint64(in.GetBlockReward()), in.GetGasLimit(), in.GetGasUsed(), txs, unRevertedHashes)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pb.ProposeBlockResponse{
		ReceivedAt:        receivedAt.UTC().Format(timestampFormat),
		SimulatedDuration: durationpb.New(simDuration),
		ResponseSentAt:    time.Now().UTC().Format(timestampFormat),
	}, nil
}
