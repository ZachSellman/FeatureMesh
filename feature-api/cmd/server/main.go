package main

import (
	"log"

	"feature-api/internal/handler"
	"feature-api/internal/service"
	"feature-api/internal/storage"

	"github.com/gin-gonic/gin"
)

func main() {
	// Initialize Redis
	redis, err := storage.NewRedisClient("localhost:6379")
	if err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}
	defer redis.Close()

	// Initialize PostgreSQL
	postgres, err := storage.NewPostgresClient(
		"localhost", 5432, "featurestore", "featurestore", "featurestore",
	)
	if err != nil {
		log.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}
	defer postgres.Close()

	// Initialize service and handler
	featureService := service.NewFeatureService(redis, postgres)
	featureHandler := handler.NewFeatureHandler(featureService)

	// Setup Gin router with recovery middleware
	router := gin.Default()
	router.Use(gin.Recovery()) // This catches panics

	// Health check
	router.GET("/health", func(c *gin.Context) {
		redisHealth := redis.Health() == nil
		postgresHealth := postgres.Health() == nil

		status := "healthy"
		statusCode := 200
		if !redisHealth || !postgresHealth {
			status = "degraded"
			statusCode = 503
		}

		c.JSON(statusCode, gin.H{
			"status":   status,
			"redis":    redisHealth,
			"postgres": postgresHealth,
		})
	})

	// Stats endpoint
	router.GET("/stats", func(c *gin.Context) {
		log.Println("Stats endpoint hit")
		defer func() {
			if r := recover(); r != nil {
				log.Printf("Panic in stats endpoint: %v", r)
				c.JSON(500, gin.H{"error": "Internal server error"})
			}
		}()

		featureHandler.GetStats(c)
	})

	// Feature endpoints
	v1 := router.Group("/api/v1")
	{
		v1.POST("/features", featureHandler.GetFeatures)
		v1.POST("/features/batch", featureHandler.GetMultipleFeatures)
	}

	log.Println("Starting Feature API on :8000")
	log.Println("Multi-tier caching: L1 (local) -> L2 (Redis) -> PostgreSQL fallback")
	log.Println("Endpoints:")
	log.Println("  GET  /health")
	log.Println("  GET  /stats")
	log.Println("  POST /api/v1/features")
	log.Println("  POST /api/v1/features/batch")

	if err := router.Run(":8000"); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
