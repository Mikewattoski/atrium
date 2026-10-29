package handler

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/Mikewattoski/atrium/internal/model"
	"github.com/Mikewattoski/atrium/internal/service"
)

type Handler struct {
	svc     *service.Service
	ping    func(ctx context.Context) error
	limiter gin.HandlerFunc
}

func New(svc *service.Service, ping func(ctx context.Context) error) *Handler {
	return &Handler{svc: svc, ping: ping}
}

func (h *Handler) Register(r *gin.Engine) {
	r.GET("/health", h.health)

	v1 := r.Group("/api/v1")
	if h.limiter != nil {
		v1.Use(h.limiter)
	}
	v1.GET("/devices", h.listDevices)
	v1.GET("/devices/:id/readings", h.listReadings)
	v1.GET("/devices/:id/latest", h.latest)
	v1.GET("/alerts", h.listAlerts)
}

func (h *Handler) health(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()
	if err := h.ping(ctx); err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "unhealthy"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

func (h *Handler) listDevices(c *gin.Context) {
	devices, err := h.svc.ListDevices(c.Request.Context())
	if err != nil {
		respondError(c, err)
		return
	}
	c.JSON(http.StatusOK, devices)
}

func (h *Handler) listReadings(c *gin.Context) {
	q := service.ReadingsQuery{Metric: c.Query("metric")}
	var err error

	if v := c.Query("from"); v != "" {
		if q.From, err = parseTime("from", v); err != nil {
			respondError(c, err)
			return
		}
	}
	if v := c.Query("to"); v != "" {
		if q.To, err = parseTime("to", v); err != nil {
			respondError(c, err)
			return
		}
	}
	if v := c.Query("cursor"); v != "" {
		t, err := parseTime("cursor", v)
		if err != nil {
			respondError(c, err)
			return
		}
		q.Cursor = &t
	}
	if v := c.Query("limit"); v != "" {
		if q.Limit, err = strconv.Atoi(v); err != nil {
			respondError(c, fmt.Errorf("%w: limit must be an integer", model.ErrInvalid))
			return
		}
	}

	page, err := h.svc.GetReadings(c.Request.Context(), c.Param("id"), q)
	if err != nil {
		respondError(c, err)
		return
	}
	c.JSON(http.StatusOK, page)
}

func (h *Handler) latest(c *gin.Context) {
	r, hit, err := h.svc.GetLatest(c.Request.Context(), c.Param("id"), c.Query("metric"))
	if err != nil {
		respondError(c, err)
		return
	}
	if hit {
		c.Header("X-Cache", "HIT")
	} else {
		c.Header("X-Cache", "MISS")
	}
	c.JSON(http.StatusOK, r)
}

func (h *Handler) listAlerts(c *gin.Context) {
	limit := 0
	if v := c.Query("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			respondError(c, fmt.Errorf("%w: limit must be an integer", model.ErrInvalid))
			return
		}
		limit = n
	}
	alerts, err := h.svc.ListAlerts(c.Request.Context(), c.Query("status"), limit)
	if err != nil {
		respondError(c, err)
		return
	}
	c.JSON(http.StatusOK, alerts)
}

func parseTime(name, v string) (time.Time, error) {
	t, err := time.Parse(time.RFC3339Nano, v)
	if err != nil {
		return time.Time{}, fmt.Errorf("%w: %s must be RFC3339, e.g. 2026-10-22T10:00:00Z", model.ErrInvalid, name)
	}
	return t.UTC(), nil
}

func respondError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, model.ErrNotFound):
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
	case errors.Is(err, model.ErrInvalid):
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
	default:
		log.Printf("internal error: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
	}
}
