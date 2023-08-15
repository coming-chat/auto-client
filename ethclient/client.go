package ethclient

import (
	"context"
	"errors"
	"math/big"
	"math/rand"
	"runtime"
	"sync"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
)

type Client struct {
	clients []*ethclient.Client
}

// Dial connect all rawurls
// if all rpc error, an error will return
func Dial(rawurls []string) (*Client, error) {
	return DialContext(context.Background(), rawurls)
}

// DialContext connect all rawurls
// if all rpc error, an error will return
func DialContext(ctx context.Context, rawurls []string) (*Client, error) {
	return DialOptions(ctx, rawurls)
}

func DialOptions(ctx context.Context, rawurls []string, options ...rpc.ClientOption) (*Client, error) {
	if len(rawurls) == 0 {
		return nil, errors.New("empty rawurls")
	}

	innerClients := make([]*ethclient.Client, 0, len(rawurls))
	var lastErr error
	for _, rawurl := range rawurls {
		rpcClient, err := rpc.DialOptions(ctx, rawurl, options...)
		if err != nil {
			lastErr = err
			continue
		}
		innerClients = append(innerClients, ethclient.NewClient(rpcClient))
	}
	if len(innerClients) == 0 {
		return nil, lastErr
	}
	return &Client{clients: innerClients}, nil
}

func (ec *Client) Close() {
	for _, client := range ec.clients {
		client.Close()
	}
}

func (ec *Client) Clients() []*ethclient.Client {
	return ec.clients
}

func (ec *Client) RandClient() *ethclient.Client {
	return ec.clients[rand.Intn(len(ec.clients))]
}

func (ec *Client) pickTwo() [2]*ethclient.Client {
	l := len(ec.clients)
	if l == 1 {
		return [2]*ethclient.Client{ec.clients[0], ec.clients[0]}
	}
	c0 := rand.Intn(l)
	c1 := c0
	for c1 == c0 {
		c1 = rand.Intn(l)
	}
	return [2]*ethclient.Client{ec.clients[c0], ec.clients[c1]}
}

func (ec *Client) call(ctx context.Context, f func(context.Context, *ethclient.Client) error) error {
	return f(ctx, ec.RandClient())
}

// callWithTwoClient pick two client call function f
// return success when one success, fail when all fail
func (ec *Client) callWithTwoClient(ctx context.Context, f func(context.Context, *ethclient.Client) error) error {
	innerCtx, cancel := context.WithCancel(ctx)
	var (
		lastErr error
		success bool
	)
	clients := ec.pickTwo()
	wg := sync.WaitGroup{}
	wg.Add(2)
	for _, client := range clients {
		go func(innerCtx context.Context, client *ethclient.Client) {
			defer wg.Done()
			err := f(innerCtx, client)
			if err != nil {
				lastErr = err
				return
			}
			success = true
			cancel()
		}(innerCtx, client)
	}
	runtime.Gosched()
	wg.Wait()
	cancel() // cancel must be called to release resource
	if success {
		return nil
	}
	return lastErr
}

// ethclient.Client apis

// ChainID retrieves the current chain ID for transaction replay protection.
func (ec *Client) ChainID(ctx context.Context) (chainId *big.Int, err error) {
	err = ec.callWithTwoClient(ctx, func(ctx context.Context, client *ethclient.Client) error {
		res, err := client.ChainID(ctx)
		if err != nil {
			return err
		}
		chainId = res
		return nil
	})
	return chainId, err
}

// BlockByHash returns the given full block.
//
// Note that loading full blocks requires two requests. Use HeaderByHash
// if you don't need all transactions or uncle headers.
func (ec *Client) BlockByHash(ctx context.Context, hash common.Hash) (block *types.Block, err error) {
	err = ec.callWithTwoClient(ctx, func(ctx context.Context, c *ethclient.Client) error {
		res, err := c.BlockByHash(ctx, hash)
		if err != nil {
			return err
		}
		block = res
		return nil
	})
	return
}

// BlockByNumber returns a block from the current canonical chain. If number is nil, the
// latest known block is returned.
//
// Note that loading full blocks requires two requests. Use HeaderByNumber
// if you don't need all transactions or uncle headers.
func (ec *Client) BlockByNumber(ctx context.Context, number *big.Int) (block *types.Block, err error) {
	err = ec.callWithTwoClient(ctx, func(ctx context.Context, c *ethclient.Client) error {
		res, err := c.BlockByNumber(ctx, number)
		if err != nil {
			return err
		}
		block = res
		return nil
	})
	return
}

// BlockNumber returns the most recent block number
// the block number may **not increase**, because of multi rpc
func (ec *Client) BlockNumber(ctx context.Context) (blockNumber uint64, err error) {
	err = ec.callWithTwoClient(ctx, func(ctx context.Context, c *ethclient.Client) error {
		res, err := c.BlockNumber(ctx)
		if err != nil {
			return err
		}
		blockNumber = res
		return nil
	})
	return
}

func (ec *Client) PeerCount(ctx context.Context) (count uint64, err error) {
	err = ec.callWithTwoClient(ctx, func(ctx context.Context, c *ethclient.Client) error {
		res, err := c.PeerCount(ctx)
		if err != nil {
			return err
		}
		count = res
		return nil
	})
	return
}

func (ec *Client) HeaderByHash(ctx context.Context, hash common.Hash) (header *types.Header, err error) {
	err = ec.callWithTwoClient(ctx, func(ctx context.Context, c *ethclient.Client) error {
		res, err := c.HeaderByHash(ctx, hash)
		if err != nil {
			return err
		}
		header = res
		return nil
	})
	return
}

func (ec *Client) HeaderByNumber(ctx context.Context, number *big.Int) (header *types.Header, err error) {
	err = ec.callWithTwoClient(ctx, func(ctx context.Context, c *ethclient.Client) error {
		res, err := c.HeaderByNumber(ctx, number)
		if err != nil {
			return err
		}
		header = res
		return nil
	})
	return
}

func (ec *Client) TransactionByHash(ctx context.Context, hash common.Hash) (tx *types.Transaction, isPending bool, err error) {
	err = ec.callWithTwoClient(ctx, func(ctx context.Context, c *ethclient.Client) error {
		res0, res1, err := c.TransactionByHash(ctx, hash)
		if err != nil {
			return err
		}
		tx = res0
		isPending = res1
		return nil
	})
	return
}

func (ec *Client) TransactionSender(ctx context.Context, tx *types.Transaction, block common.Hash, index uint) (sender common.Address, err error) {
	err = ec.callWithTwoClient(ctx, func(ctx context.Context, c *ethclient.Client) error {
		res, err := c.TransactionSender(ctx, tx, block, index)
		if err != nil {
			return err
		}
		sender = res
		return nil
	})
	return
}

func (ec *Client) TransactionCount(ctx context.Context, blockHash common.Hash) (count uint, err error) {
	err = ec.callWithTwoClient(ctx, func(ctx context.Context, c *ethclient.Client) error {
		res, err := c.TransactionCount(ctx, blockHash)
		if err != nil {
			return err
		}
		count = res
		return nil
	})
	return
}

func (ec *Client) TransactionInBlock(ctx context.Context, blockHash common.Hash, index uint) (tx *types.Transaction, err error) {
	err = ec.callWithTwoClient(ctx, func(ctx context.Context, c *ethclient.Client) error {
		res, err := c.TransactionInBlock(ctx, blockHash, index)
		if err != nil {
			return err
		}
		tx = res
		return nil
	})
	return
}

func (ec *Client) TransactionReceipt(ctx context.Context, txHash common.Hash) (receipt *types.Receipt, err error) {
	err = ec.callWithTwoClient(ctx, func(ctx context.Context, c *ethclient.Client) error {
		res, err := c.TransactionReceipt(ctx, txHash)
		if err != nil {
			return err
		}
		receipt = res
		return nil
	})
	return
}

func (ec *Client) SyncProgress(ctx context.Context) (progress *ethereum.SyncProgress, err error) {
	err = ec.callWithTwoClient(ctx, func(ctx context.Context, c *ethclient.Client) error {
		res, err := c.SyncProgress(ctx)
		if err != nil {
			return err
		}
		progress = res
		return nil
	})
	return
}

// State Access

// NetworkID returns the network ID for this client.
func (ec *Client) NetworkID(ctx context.Context) (ver *big.Int, err error) {
	err = ec.callWithTwoClient(ctx, func(ctx context.Context, c *ethclient.Client) error {
		res, err := c.NetworkID(ctx)
		if err != nil {
			return err
		}
		ver = res
		return nil
	})
	return
}

// BalanceAt returns the wei balance of the given account.
// The block number can be nil, in which case the balance is taken from the latest known block.
func (ec *Client) BalanceAt(ctx context.Context, account common.Address, blockNumber *big.Int) (balance *big.Int, err error) {
	err = ec.callWithTwoClient(ctx, func(ctx context.Context, c *ethclient.Client) error {
		res, err := c.BalanceAt(ctx, account, blockNumber)
		if err != nil {
			return err
		}
		balance = res
		return nil
	})
	return
}

// StorageAt returns the value of key in the contract storage of the given account.
// The block number can be nil, in which case the value is taken from the latest known block.
func (ec *Client) StorageAt(ctx context.Context, account common.Address, key common.Hash, blockNumber *big.Int) (data []byte, err error) {
	err = ec.callWithTwoClient(ctx, func(ctx context.Context, c *ethclient.Client) error {
		res, err := c.StorageAt(ctx, account, key, blockNumber)
		if err != nil {
			return err
		}
		data = res
		return nil
	})
	return
}

// CodeAt returns the contract code of the given account.
// The block number can be nil, in which case the code is taken from the latest known block.
func (ec *Client) CodeAt(ctx context.Context, account common.Address, blockNumber *big.Int) (data []byte, err error) {
	err = ec.callWithTwoClient(ctx, func(ctx context.Context, c *ethclient.Client) error {
		res, err := c.CodeAt(ctx, account, blockNumber)
		if err != nil {
			return err
		}
		data = res
		return nil
	})
	return
}

// NonceAt returns the account nonce of the given account.
// The block number can be nil, in which case the nonce is taken from the latest known block.
func (ec *Client) NonceAt(ctx context.Context, account common.Address, blockNumber *big.Int) (nonce uint64, err error) {
	err = ec.callWithTwoClient(ctx, func(ctx context.Context, c *ethclient.Client) error {
		res, err := c.NonceAt(ctx, account, blockNumber)
		if err != nil {
			return err
		}
		nonce = res
		return nil
	})
	return
}

// Filters

// FilterLogs executes a filter query.
func (ec *Client) FilterLogs(ctx context.Context, q ethereum.FilterQuery) (logs []types.Log, err error) {
	err = ec.callWithTwoClient(ctx, func(ctx context.Context, c *ethclient.Client) error {
		res, err := c.FilterLogs(ctx, q)
		if err != nil {
			return err
		}
		logs = res
		return nil
	})
	return
}

// Pending State

// PendingBalanceAt returns the wei balance of the given account in the pending state.
func (ec *Client) PendingBalanceAt(ctx context.Context, account common.Address) (balance *big.Int, err error) {
	err = ec.callWithTwoClient(ctx, func(ctx context.Context, c *ethclient.Client) error {
		res, err := c.PendingBalanceAt(ctx, account)
		if err != nil {
			return err
		}
		balance = res
		return nil
	})
	return
}

// PendingStorageAt returns the value of key in the contract storage of the given account in the pending state.
func (ec *Client) PendingStorageAt(ctx context.Context, account common.Address, key common.Hash) (data []byte, err error) {
	err = ec.callWithTwoClient(ctx, func(ctx context.Context, c *ethclient.Client) error {
		res, err := c.PendingStorageAt(ctx, account, key)
		if err != nil {
			return err
		}
		data = res
		return nil
	})
	return
}

// PendingCodeAt returns the contract code of the given account in the pending state.
func (ec *Client) PendingCodeAt(ctx context.Context, account common.Address) (data []byte, err error) {
	err = ec.callWithTwoClient(ctx, func(ctx context.Context, c *ethclient.Client) error {
		res, err := c.PendingCodeAt(ctx, account)
		if err != nil {
			return err
		}
		data = res
		return nil
	})
	return
}

// PendingNonceAt returns the account nonce of the given account in the pending state.
// This is the nonce that should be used for the next transaction.
func (ec *Client) PendingNonceAt(ctx context.Context, account common.Address) (nonce uint64, err error) {
	err = ec.callWithTwoClient(ctx, func(ctx context.Context, c *ethclient.Client) error {
		res, err := c.PendingNonceAt(ctx, account)
		if err != nil {
			return err
		}
		nonce = res
		return nil
	})
	return
}

// PendingTransactionCount returns the total number of transactions in the pending state.
func (ec *Client) PendingTransactionCount(ctx context.Context) (count uint, err error) {
	err = ec.callWithTwoClient(ctx, func(ctx context.Context, c *ethclient.Client) error {
		res, err := c.PendingTransactionCount(ctx)
		if err != nil {
			return err
		}
		count = res
		return nil
	})
	return
}

// Contract Calling

// CallContract executes a message call transaction, which is directly executed in the VM
// of the node, but never mined into the blockchain.
//
// blockNumber selects the block height at which the call runs. It can be nil, in which
// case the code is taken from the latest known block. Note that state from very old
// blocks might not be available.
func (ec *Client) CallContract(ctx context.Context, msg ethereum.CallMsg, blockNumber *big.Int) (data []byte, err error) {
	err = ec.callWithTwoClient(ctx, func(ctx context.Context, c *ethclient.Client) error {
		res, err := c.CallContract(ctx, msg, blockNumber)
		if err != nil {
			return err
		}
		data = res
		return nil
	})
	return
}

// CallContractAtHash is almost the same as CallContract except that it selects
// the block by block hash instead of block height.
func (ec *Client) CallContractAtHash(ctx context.Context, msg ethereum.CallMsg, blockHash common.Hash) (data []byte, err error) {
	err = ec.callWithTwoClient(ctx, func(ctx context.Context, c *ethclient.Client) error {
		res, err := c.CallContractAtHash(ctx, msg, blockHash)
		if err != nil {
			return err
		}
		data = res
		return nil
	})
	return
}

// PendingCallContract executes a message call transaction using the EVM.
// The state seen by the contract call is the pending state.
func (ec *Client) PendingCallContract(ctx context.Context, msg ethereum.CallMsg) (data []byte, err error) {
	err = ec.callWithTwoClient(ctx, func(ctx context.Context, c *ethclient.Client) error {
		res, err := c.PendingCallContract(ctx, msg)
		if err != nil {
			return err
		}
		data = res
		return nil
	})
	return
}

// SuggestGasPrice retrieves the currently suggested gas price to allow a timely
// execution of a transaction.
func (ec *Client) SuggestGasPrice(ctx context.Context) (gasPrice *big.Int, err error) {
	err = ec.callWithTwoClient(ctx, func(ctx context.Context, c *ethclient.Client) error {
		res, err := c.SuggestGasPrice(ctx)
		if err != nil {
			return err
		}
		gasPrice = res
		return nil
	})
	return
}

// SuggestGasTipCap retrieves the currently suggested gas tip cap after 1559 to
// allow a timely execution of a transaction.
func (ec *Client) SuggestGasTipCap(ctx context.Context) (gasTipCap *big.Int, err error) {
	err = ec.callWithTwoClient(ctx, func(ctx context.Context, c *ethclient.Client) error {
		res, err := c.SuggestGasTipCap(ctx)
		if err != nil {
			return err
		}
		gasTipCap = res
		return nil
	})
	return
}

// FeeHistory retrieves the fee market history.
func (ec *Client) FeeHistory(ctx context.Context, blockCount uint64, lastBlock *big.Int, rewardPercentiles []float64) (history *ethereum.FeeHistory, err error) {
	err = ec.callWithTwoClient(ctx, func(ctx context.Context, c *ethclient.Client) error {
		res, err := c.FeeHistory(ctx, blockCount, lastBlock, rewardPercentiles)
		if err != nil {
			return err
		}
		history = res
		return nil
	})
	return
}

// EstimateGas tries to estimate the gas needed to execute a specific transaction based on
// the current pending state of the backend blockchain. There is no guarantee that this is
// the true gas limit requirement as other transactions may be added or removed by miners,
// but it should provide a basis for setting a reasonable default.
func (ec *Client) EstimateGas(ctx context.Context, msg ethereum.CallMsg) (gas uint64, err error) {
	err = ec.callWithTwoClient(ctx, func(ctx context.Context, c *ethclient.Client) error {
		res, err := c.EstimateGas(ctx, msg)
		if err != nil {
			return err
		}
		gas = res
		return nil
	})
	return
}

// SendTransaction injects a signed transaction into the pending pool for execution.
//
// If the transaction was a contract creation use the TransactionReceipt method to get the
// contract address after the transaction has been mined.
func (ec *Client) SendTransaction(ctx context.Context, tx *types.Transaction) (err error) {
	err = ec.call(ctx, func(ctx context.Context, c *ethclient.Client) error {
		return c.SendTransaction(ctx, tx)
	})
	return
}
