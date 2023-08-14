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
	if len(rawurls) == 0 {
		return nil, errors.New("empty rawurls")
	}

	innerClients := make([]*ethclient.Client, 0, len(rawurls))
	var lastErr error
	for _, rawurl := range rawurls {
		rpcClient, err := rpc.DialContext(ctx, rawurl)
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
