package redisclient

import (
	"context"

	"github.com/redis/go-redis/v9"
)

func New(ctx context.Context, addr string) (*redis.Client, error) {
	c := redis.NewClient(&redis.Options{Addr: addr})
	if err := c.Ping(ctx).Err(); err != nil {
		return nil, err
	}
	return c, nil
}
