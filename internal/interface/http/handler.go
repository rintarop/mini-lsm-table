package http

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/Bloom0716/mini-bigtable/internal/usecase"
)

// Handler represents the HTTP handler for LSM-tree operations
type Handler struct {
	service *usecase.LSMTableService
}

// Request/Response structures
type PutRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type GetResponse struct {
	Key   string `json:"key"`
	Value string `json:"value"`
	Found bool   `json:"found"`
}

type DeleteRequest struct {
	Key string `json:"key"`
}

type StatusResponse struct {
	ActiveMemTableSize int         `json:"active_memtable_size"`
	ImmutableCount     int         `json:"immutable_count"`
	SSTableStats       map[int]int `json:"sstable_stats"`
	Message            string      `json:"message"`
}

type ErrorResponse struct {
	Error string `json:"error"`
}

// NewHandler creates a new HTTP handler
func NewHandler(service *usecase.LSMTableService) *Handler {
	return &Handler{
		service: service,
	}
}

// PUT /api/put - Store a key-value pair
func (h *Handler) HandlePut(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut && r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req PutRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeErrorResponse(w, http.StatusBadRequest, "Invalid JSON format")
		return
	}

	if req.Key == "" {
		h.writeErrorResponse(w, http.StatusBadRequest, "Key cannot be empty")
		return
	}

	if err := h.service.Put([]byte(req.Key), []byte(req.Value)); err != nil {
		h.writeErrorResponse(w, http.StatusInternalServerError, fmt.Sprintf("Failed to put: %v", err))
		return
	}

	h.writeSuccessResponse(w, map[string]string{
		"status":  "success",
		"message": fmt.Sprintf("Key '%s' stored successfully", req.Key),
	})
}

// GET /api/get/{key} - Retrieve a value by key
func (h *Handler) HandleGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	key := r.URL.Path[len("/api/get/"):]
	if key == "" {
		h.writeErrorResponse(w, http.StatusBadRequest, "Key cannot be empty")
		return
	}

	value, err := h.service.Get([]byte(key))
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(GetResponse{
			Key:   key,
			Value: "",
			Found: false,
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(GetResponse{
		Key:   key,
		Value: string(value),
		Found: true,
	})
}

// DELETE /api/delete - Delete a key
func (h *Handler) HandleDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete && r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req DeleteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeErrorResponse(w, http.StatusBadRequest, "Invalid JSON format")
		return
	}

	if req.Key == "" {
		h.writeErrorResponse(w, http.StatusBadRequest, "Key cannot be empty")
		return
	}

	if err := h.service.Delete([]byte(req.Key)); err != nil {
		h.writeErrorResponse(w, http.StatusInternalServerError, fmt.Sprintf("Failed to delete: %v", err))
		return
	}

	h.writeSuccessResponse(w, map[string]string{
		"status":  "success",
		"message": fmt.Sprintf("Key '%s' deleted successfully", req.Key),
	})
}

// GET /api/status - Get system status
func (h *Handler) HandleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	activeSize, immutableCount := h.service.GetMemTableStats()
	sstableStats := h.service.GetSSTableStats()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(StatusResponse{
		ActiveMemTableSize: activeSize,
		ImmutableCount:     immutableCount,
		SSTableStats:       sstableStats,
		Message:            "LSM-Tree service is running",
	})
}

// POST /api/recovery - Trigger recovery
func (h *Handler) HandleRecovery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if err := h.service.Recovery(); err != nil {
		h.writeErrorResponse(w, http.StatusInternalServerError, fmt.Sprintf("Recovery failed: %v", err))
		return
	}

	h.writeSuccessResponse(w, map[string]string{
		"status":  "success",
		"message": "Recovery completed successfully",
	})
}

// GET /health - Health check endpoint
func (h *Handler) HandleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"status":  "healthy",
		"service": "mini-lsm-table",
	})
}

// GET / - API documentation endpoint
func (h *Handler) HandleAPIDoc(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	apiDoc := map[string]interface{}{
		"service": "Mini LSM-Tree Table API",
		"version": "1.0.0",
		"endpoints": map[string]interface{}{
			"PUT /api/put": map[string]string{
				"description": "Store a key-value pair",
				"body":        `{"key": "string", "value": "string"}`,
			},
			"GET /api/get/{key}": map[string]string{
				"description": "Retrieve a value by key",
			},
			"DELETE /api/delete": map[string]string{
				"description": "Delete a key",
				"body":        `{"key": "string"}`,
			},
			"GET /api/status": map[string]string{
				"description": "Get system status and statistics",
			},
			"POST /api/recovery": map[string]string{
				"description": "Trigger recovery process",
			},
			"GET /health": map[string]string{
				"description": "Health check endpoint",
			},
		},
		"examples": map[string]interface{}{
			"store_data":  "curl -X PUT http://localhost:8080/api/put -H 'Content-Type: application/json' -d '{\"key\":\"user:1\",\"value\":\"Alice\"}'",
			"get_data":    "curl http://localhost:8080/api/get/user:1",
			"delete_data": "curl -X DELETE http://localhost:8080/api/delete -H 'Content-Type: application/json' -d '{\"key\":\"user:1\"}'",
			"status":      "curl http://localhost:8080/api/status",
		},
	}
	json.NewEncoder(w).Encode(apiDoc)
}

// Helper methods for response handling
func (h *Handler) writeErrorResponse(w http.ResponseWriter, statusCode int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(ErrorResponse{Error: message})
}

func (h *Handler) writeSuccessResponse(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(data)
}
