package ethapi

import (
	"context"
	"errors"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rlp"
	"math/big"
)

// PublicBuilderAPI provides an API for PBS.
// It offers methods for the interaction between builders and validators.
type PublicBuilderAPI struct {
	b Backend
}

// NewPublicBuilderAPI creates a new Builder API.
func NewPublicBuilderAPI(b Backend) *PublicBuilderAPI {
	return &PublicBuilderAPI{b}
}

type BidMessage struct {
	// block height
	Block int64 `json:"block"`
	// parent block hash
	ParentHash string `json:"parent_hash"`
	// unix timestamp in seconds
	Timestamp int64 `json:"timestamp"`
	// address of builder
	BuilderAddress string `json:"builder_address"`
	// gas limit of the block to be proposed in BNB wei
	GasLimit int64 `json:"gas_value"`
	// gas value of the block to be proposed in BNB wei
	GasValue int64 `json:"gas_value"`
	// the fee that builder would like to get
	BuilderFeeValue int64 `json:"builder_fee_value"`
	// ordered raw transactions, optional
	Txs []hexutil.Bytes `json:"txs,omitempty"`
}

type BidArgs struct {
	// bid message
	Message *BidMessage `json:"message"`
	// signed signature of the message
	Signature string `json:"signature"`
}

func checkBasic(args BidArgs) error {
	if args.Message.Block == 0 {
		return errors.New("missing block number")
	}

	if args.Message.ParentHash == "" {
		return errors.New("missing parent hash")
	}

	if args.Message.GasLimit <= 0 {
		return errors.New("missing gas limit")
	}

	if args.Message.GasValue <= 0 {
		return errors.New("missing gas value")
	}

	if args.Message.BuilderFeeValue < 0 {
		return errors.New("invalid builder fee")
	}

	if args.Message.GasValue <= args.Message.BuilderFeeValue {
		return errors.New("gas value is lower than builder fee")
	}

	if args.Message.BuilderAddress == "" {
		return errors.New("missing builder address")
	}

	if !common.IsHexAddress(args.Message.BuilderAddress) {
		return errors.New("wrong builder address format")
	}

	return nil
}

func checkBlock(args BidArgs, currentBlock *types.Block) (types.Transactions, error) {
	var txs types.Transactions

	bidBlockHeight := big.NewInt(args.Message.Block)
	if bidBlockHeight.Cmp(big.NewInt(0).Add(currentBlock.Number(), big.NewInt(1))) != 0 {
		return nil, fmt.Errorf("invalid block height, bid block: %v current block: %v", args.Message.Block, currentBlock.Number())
	}

	if args.Message.ParentHash != currentBlock.Hash().Hex() {
		return nil, fmt.Errorf("invalid parent hash, bid block: %v current block: %v", args.Message.ParentHash, currentBlock.Hash().Hex())
	}

	for _, encodedTx := range args.Message.Txs {
		tx := new(types.Transaction)
		if err := tx.UnmarshalBinary(encodedTx); err != nil {
			return nil, errors.New("invalid txs: " + err.Error())
		}
		txs = append(txs, tx)
	}

	return txs, nil
}

func checkSignature(args BidArgs) error {
	hash, err := rlp.EncodeToBytes(args.Message)
	if err != nil {
		return errors.New("fail to verify signature, err: " + err.Error())
	}

	sig := hexutil.MustDecode(args.Signature)
	sigPublicKey, err := crypto.Ecrecover(hash, sig)
	if err != nil {
		return errors.New("fail to verify signature, err: " + err.Error())
	}

	pk, err := crypto.UnmarshalPubkey(sigPublicKey)
	if err != nil {
		return errors.New("fail to verify signature, err: " + err.Error())
	}

	expected := crypto.PubkeyToAddress(*pk)
	actual := common.HexToAddress(args.Message.BuilderAddress)
	if expected != actual {
		return fmt.Errorf("invalid signature: signature comes from %v not %v", expected, actual)
	}

	return nil
}

func (s *PublicBuilderAPI) Bid(ctx context.Context, args BidArgs) error {
	enabled := s.b.BuilderEnabled()
	if !enabled {
		return errors.New("builder is not enabled")
	}

	err := checkBasic(args)
	if err != nil {
		return err
	}

	currentBlock := s.b.CurrentBlock()
	txs, err := checkBlock(args, currentBlock)
	if err != nil {
		return err
	}

	err = checkSignature(args)
	if err != nil {
		return err
	}

	return s.b.Bid(ctx, common.HexToAddress(args.Message.BuilderAddress), args.Message.Block, txs,
		args.Message.GasValue, args.Message.BuilderFeeValue, args.Message.GasLimit)
}

type TxsMessage struct {
	// block height
	Block int32 `json:"block"`
	// parent block hash
	ParentHash string `json:"parent_hash"`
	// unix timestamp in seconds
	Timestamp int32 `json:"timestamp"`
	// address of builder
	BuilderAddress string `json:"builder_address"`
	// gas value of the block to be proposed in BNB wei
	GasValue int32 `json:"gas_value"`
	// the fee that builder would like to get
	BuilderFeeValue int32 `json:"builder_fee_value"`
	// consensus address of the validator
	ConsensusAddress string `json:"consensus_address"`
}

type TxsArgs struct {
	// txs message
	Message *TxsMessage `json:"message"`
	// signed signature of the message
	Signature string `json:"signature"`
	// whether we need the builder return transactions or not
	ReturnTxs bool `json:"return_txs,omitempty"`
}

type TxsRes struct {
	// ordered raw transactions
	Txs []hexutil.Bytes `json:"txs"`
}

func (s *PublicBuilderAPI) Txs(ctx context.Context, args TxsArgs) (*TxsRes, error) {
	// implement on Builder side
	return nil, nil
}

type IssueMessage struct {
	// code of the issue
	IssueCode int32 `json:"issue_code"`
	// unix timestamp in seconds
	Timestamp int32 `json:"timestamp"`
	// address of builder
	ConsensusAddress string `json:"consensus_address"`
}

type IssueArgs struct {
	// issue message
	Message *IssueMessage `json:"message"`
	// signed signature of the message
	Signature string `json:"signature"`
}

func (s *PublicBuilderAPI) Issue(ctx context.Context, args IssueArgs) error {
	// implement on Builder side
	return nil
}
