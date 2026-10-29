package handler

import (
	"context"
	"log"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
)

type Limiter interface {
	Allow(ctx context.Context, id string, limit int, window time.Duration) (allowed bool, retryAfter time.Duration, err error)
}

// RateLimit allows `limit` requests per `window` per client IP.
// If the limiter backend fails, requests are allowed (fail open).
func RateLimit(l Limiter, limit int, window time.Duration) gin.HandlerFunc {
	return func(c *gin.Context) {
		ok, retry, err := l.Allow(c.Request.Context(), c.ClientIP(), limit, window)
		if err != nil {
			log.Printf("rate limiter unavailable, allowing request: %v", err)
			c.Next()
			return
		}
		if !ok {
			c.Header("Retry-After", strconv.Itoa(int(math.Ceil(retry.Seconds()))))
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{"error": "rate limit exceeded"})
			return
		}
		c.Next()
	}
}

// WithLimiter installs a middleware on the /api/v1 routes.
func (h *Handler) WithLimiter(mw gin.HandlerFunc) *Handler {
	h.limiter = mw
	return h
}
