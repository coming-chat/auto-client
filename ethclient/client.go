package ethclient

import (
	"context"
	"errors"
	"math/big"
	"math/rand"
	"sync"

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

func (c *Client) Close() {
	for _, client := range c.clients {
		client.Close()
	}
}

func (c *Client) Clients() []*ethclient.Client {
	return c.clients
}

func (c *Client) pickTwo() [2]*ethclient.Client {
	l := len(c.clients)
	if l == 1 {
		return [2]*ethclient.Client{c.clients[0], c.clients[0]}
	}
	c0 := rand.Intn(l)
	c1 := c0
	for c1 == c0 {
		c1 = rand.Intn(l)
	}
	return [2]*ethclient.Client{c.clients[c0], c.clients[c1]}
}

// callWithTwoClient pick two client call function f
// return success when one success, fail when all fail
func (c *Client) callWithTwoClient(ctx context.Context, f func(context.Context, *ethclient.Client) error) error {
	innerCtx, cancel := context.WithCancel(ctx)
	var (
		lastErr error
		success bool
	)
	clients := c.pickTwo()
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
	wg.Wait()
	cancel() // cancel must be called to release resource
	if success {
		return nil
	}
	return lastErr
}

func (c *Client) ChainID(ctx context.Context) (*big.Int, error) {
	var (
		chainId *big.Int
		err     error
	)
	err = c.callWithTwoClient(ctx, func(ctx context.Context, client *ethclient.Client) error {
		res, err := client.ChainID(ctx)
		if err != nil {
			return err
		}
		chainId = res
		return nil
	})
	return chainId, err
}
