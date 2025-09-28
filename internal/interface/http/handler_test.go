package http

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/Bloom0716/mini-bigtable/internal/service"
)

func setupTestHandler(t *testing.T) (*Handler, func()) {
	// Create temporary directory for test
	tmpDir := filepath.Join(os.TempDir(), "test_lsm_http")

	// Create LSM service
	service, err := service.NewLSMTableService(tmpDir, 10)
	if err != nil {
		t.Fatalf("Failed to create LSM service: %v", err)
	}

	handler := NewHandler(service)

	// Return cleanup function
	cleanup := func() {
		service.Close()
		os.RemoveAll(tmpDir)
	}

	return handler, cleanup
}

func TestHandler_HandlePut(t *testing.T) {
	handler, cleanup := setupTestHandler(t)
	defer cleanup()

	tests := []struct {
		name           string
		requestBody    PutRequest
		expectedStatus int
	}{
		{
			name:           "Valid PUT request",
			requestBody:    PutRequest{Key: "test:key", Value: "test value"},
			expectedStatus: http.StatusOK,
		},
		{
			name:           "Empty key",
			requestBody:    PutRequest{Key: "", Value: "test value"},
			expectedStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create request body
			body, err := json.Marshal(tt.requestBody)
			if err != nil {
				t.Fatalf("Failed to marshal request body: %v", err)
			}

			// Create request
			req := httptest.NewRequest(http.MethodPut, "/api/put", bytes.NewBuffer(body))
			req.Header.Set("Content-Type", "application/json")

			// Create response recorder
			rr := httptest.NewRecorder()

			// Call handler
			handler.HandlePut(rr, req)

			// Check status code
			if rr.Code != tt.expectedStatus {
				t.Errorf("Expected status %d, got %d", tt.expectedStatus, rr.Code)
			}
		})
	}
}

func TestHandler_HandleGet(t *testing.T) {
	handler, cleanup := setupTestHandler(t)
	defer cleanup()

	// First, put a value
	putReq := PutRequest{Key: "test:get", Value: "get value"}
	body, _ := json.Marshal(putReq)
	req := httptest.NewRequest(http.MethodPut, "/api/put", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	handler.HandlePut(rr, req)

	tests := []struct {
		name           string
		key            string
		expectedStatus int
		expectedFound  bool
	}{
		{
			name:           "Existing key",
			key:            "test:get",
			expectedStatus: http.StatusOK,
			expectedFound:  true,
		},
		{
			name:           "Non-existing key",
			key:            "test:nonexistent",
			expectedStatus: http.StatusNotFound,
			expectedFound:  false,
		},
		{
			name:           "Empty key",
			key:            "",
			expectedStatus: http.StatusBadRequest,
			expectedFound:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create request
			req := httptest.NewRequest(http.MethodGet, "/api/get/"+tt.key, nil)

			// Create response recorder
			rr := httptest.NewRecorder()

			// Call handler
			handler.HandleGet(rr, req)

			// Check status code
			if rr.Code != tt.expectedStatus {
				t.Errorf("Expected status %d, got %d", tt.expectedStatus, rr.Code)
			}

			// Check response body for successful requests
			if tt.expectedStatus == http.StatusOK || tt.expectedStatus == http.StatusNotFound {
				var response GetResponse
				if err := json.Unmarshal(rr.Body.Bytes(), &response); err != nil {
					t.Fatalf("Failed to unmarshal response: %v", err)
				}

				if response.Found != tt.expectedFound {
					t.Errorf("Expected found=%t, got found=%t", tt.expectedFound, response.Found)
				}

				if tt.expectedFound && response.Key != tt.key {
					t.Errorf("Expected key=%s, got key=%s", tt.key, response.Key)
				}
			}
		})
	}
}

func TestHandler_HandleDelete(t *testing.T) {
	handler, cleanup := setupTestHandler(t)
	defer cleanup()

	// First, put a value
	putReq := PutRequest{Key: "test:delete", Value: "delete value"}
	body, _ := json.Marshal(putReq)
	req := httptest.NewRequest(http.MethodPut, "/api/put", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	handler.HandlePut(rr, req)

	tests := []struct {
		name           string
		requestBody    DeleteRequest
		expectedStatus int
	}{
		{
			name:           "Valid DELETE request",
			requestBody:    DeleteRequest{Key: "test:delete"},
			expectedStatus: http.StatusOK,
		},
		{
			name:           "Empty key",
			requestBody:    DeleteRequest{Key: ""},
			expectedStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create request body
			body, err := json.Marshal(tt.requestBody)
			if err != nil {
				t.Fatalf("Failed to marshal request body: %v", err)
			}

			// Create request
			req := httptest.NewRequest(http.MethodDelete, "/api/delete", bytes.NewBuffer(body))
			req.Header.Set("Content-Type", "application/json")

			// Create response recorder
			rr := httptest.NewRecorder()

			// Call handler
			handler.HandleDelete(rr, req)

			// Check status code
			if rr.Code != tt.expectedStatus {
				t.Errorf("Expected status %d, got %d", tt.expectedStatus, rr.Code)
			}
		})
	}
}

func TestHandler_HandleStatus(t *testing.T) {
	handler, cleanup := setupTestHandler(t)
	defer cleanup()

	// Create request
	req := httptest.NewRequest(http.MethodGet, "/api/status", nil)

	// Create response recorder
	rr := httptest.NewRecorder()

	// Call handler
	handler.HandleStatus(rr, req)

	// Check status code
	if rr.Code != http.StatusOK {
		t.Errorf("Expected status %d, got %d", http.StatusOK, rr.Code)
	}

	// Check response body
	var response StatusResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &response); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	if response.Message == "" {
		t.Error("Expected message to be non-empty")
	}
}

func TestHandler_HandleHealth(t *testing.T) {
	handler, cleanup := setupTestHandler(t)
	defer cleanup()

	// Create request
	req := httptest.NewRequest(http.MethodGet, "/health", nil)

	// Create response recorder
	rr := httptest.NewRecorder()

	// Call handler
	handler.HandleHealth(rr, req)

	// Check status code
	if rr.Code != http.StatusOK {
		t.Errorf("Expected status %d, got %d", http.StatusOK, rr.Code)
	}

	// Check response contains expected fields
	var response map[string]string
	if err := json.Unmarshal(rr.Body.Bytes(), &response); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	if response["status"] != "healthy" {
		t.Errorf("Expected status=healthy, got status=%s", response["status"])
	}

	if response["service"] != "mini-lsm-table" {
		t.Errorf("Expected service=mini-lsm-table, got service=%s", response["service"])
	}
}
