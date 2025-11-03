package handler

import (
	"net/http"
	"time"

	"feature-api/internal/models"
	"feature-api/internal/service"

	"github.com/gin-gonic/gin"
)

type FeatureHandler struct {
	service *service.FeatureService
}

func NewFeatureHandler(service *service.FeatureService) *FeatureHandler {
	return &FeatureHandler{service: service}
}

func (h *FeatureHandler) GetFeatures(c *gin.Context) {
	var req models.FeatureRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	response, err := h.service.GetFeatures(req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, response)
}

func (h *FeatureHandler) GetMultipleFeatures(c *gin.Context) {
	start := time.Now()

	var req models.BatchFeatureRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	responses, err := h.service.GetMultipleFeatures(req.Requests)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	latency := time.Since(start).Milliseconds()

	c.JSON(http.StatusOK, models.BatchFeatureResponse{
		Responses: responses,
		LatencyMs: latency,
	})
}

func (h *FeatureHandler) GetStats(c *gin.Context) {
	stats := h.service.GetStats()
	c.JSON(http.StatusOK, stats)
}
