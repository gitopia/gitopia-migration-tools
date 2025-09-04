package shared

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/pkg/errors"
)

// PinataClient handles interactions with Pinata API
type PinataClient struct {
	jwtToken string
	client   *http.Client
}

// PinataResponse represents the response from Pinata API
type PinataResponse struct {
	Data struct {
		ID        string    `json:"id"`
		Name      string    `json:"name"`
		CID       string    `json:"cid"`
		Size      int64     `json:"size"`
		CreatedAt time.Time `json:"created_at"`
	} `json:"data"`
}

// PinataFileInfo represents file information stored for unpinning
type PinataFileInfo struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	CID  string `json:"cid"`
}

// NewPinataClient creates a new Pinata client
func NewPinataClient(jwtToken string) *PinataClient {
	return &PinataClient{
		jwtToken: jwtToken,
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// PinFile uploads a file to Pinata
func (p *PinataClient) PinFile(ctx context.Context, filePath, name string) (*PinataResponse, error) {
	// Open the file
	file, err := os.Open(filePath)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to open file %s", filePath)
	}
	defer file.Close()

	// Create a buffer to hold the multipart form data
	var requestBody bytes.Buffer
	writer := multipart.NewWriter(&requestBody)

	// Create the file field
	fileWriter, err := writer.CreateFormFile("file", filepath.Base(filePath))
	if err != nil {
		return nil, errors.Wrap(err, "failed to create form file field")
	}

	// Copy file content to the form
	_, err = io.Copy(fileWriter, file)
	if err != nil {
		return nil, errors.Wrap(err, "failed to copy file content")
	}

	// Add metadata
	metadata := map[string]interface{}{
		"name": name,
	}
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal metadata")
	}

	err = writer.WriteField("pinataMetadata", string(metadataJSON))
	if err != nil {
		return nil, errors.Wrap(err, "failed to write metadata field")
	}

	// Close the writer to finalize the multipart form
	err = writer.Close()
	if err != nil {
		return nil, errors.Wrap(err, "failed to close multipart writer")
	}

	// Create the HTTP request
	req, err := http.NewRequestWithContext(ctx, "POST", "https://api.pinata.cloud/pinning/pinFileToIPFS", &requestBody)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create HTTP request")
	}

	// Set headers
	req.Header.Set("Content-Type", writer.FormDataContentType())
	req.Header.Set("Authorization", "Bearer "+p.jwtToken)

	// Send the request
	resp, err := p.client.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "failed to send HTTP request")
	}
	defer resp.Body.Close()

	// Read the response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read response body")
	}

	// Check for HTTP errors
	if resp.StatusCode != http.StatusOK {
		return nil, errors.Errorf("Pinata API returned status %d: %s", resp.StatusCode, string(body))
	}

	// Parse the response
	var pinataResp PinataResponse
	err = json.Unmarshal(body, &pinataResp)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to parse response: %s", string(body))
	}

	return &pinataResp, nil
}

// UnpinFile removes a file from Pinata by file name
func (p *PinataClient) UnpinFile(ctx context.Context, fileName string) error {
	// First, get the file ID by listing files and finding the one with matching name
	fileID, err := p.getFileIDByName(ctx, fileName)
	if err != nil {
		return errors.Wrapf(err, "failed to get file ID for %s", fileName)
	}

	if fileID == "" {
		// File not found, consider it already unpinned
		return nil
	}

	// Create the HTTP request to unpin
	req, err := http.NewRequestWithContext(ctx, "DELETE", fmt.Sprintf("https://api.pinata.cloud/pinning/unpin/%s", fileID), nil)
	if err != nil {
		return errors.Wrap(err, "failed to create unpin HTTP request")
	}

	// Set headers
	req.Header.Set("Authorization", "Bearer "+p.jwtToken)

	// Send the request
	resp, err := p.client.Do(req)
	if err != nil {
		return errors.Wrap(err, "failed to send unpin HTTP request")
	}
	defer resp.Body.Close()

	// Check for HTTP errors
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return errors.Errorf("Pinata unpin API returned status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// getFileIDByName retrieves the file ID for a given file name
func (p *PinataClient) getFileIDByName(ctx context.Context, fileName string) (string, error) {
	// Create the HTTP request to list files
	req, err := http.NewRequestWithContext(ctx, "GET", "https://api.pinata.cloud/data/pinList", nil)
	if err != nil {
		return "", errors.Wrap(err, "failed to create list HTTP request")
	}

	// Set headers
	req.Header.Set("Authorization", "Bearer "+p.jwtToken)

	// Add query parameters to filter by name
	q := req.URL.Query()
	q.Add("metadata[name]", fileName)
	q.Add("status", "pinned")
	req.URL.RawQuery = q.Encode()

	// Send the request
	resp, err := p.client.Do(req)
	if err != nil {
		return "", errors.Wrap(err, "failed to send list HTTP request")
	}
	defer resp.Body.Close()

	// Read the response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", errors.Wrap(err, "failed to read list response body")
	}

	// Check for HTTP errors
	if resp.StatusCode != http.StatusOK {
		return "", errors.Errorf("Pinata list API returned status %d: %s", resp.StatusCode, string(body))
	}

	// Parse the response
	var listResp struct {
		Rows []struct {
			ID           string `json:"id"`
			IPFSPinHash  string `json:"ipfs_pin_hash"`
			Size         int64  `json:"size"`
			UserID       string `json:"user_id"`
			DatePinned   string `json:"date_pinned"`
			DateUnpinned string `json:"date_unpinned"`
			Metadata     struct {
				Name string `json:"name"`
			} `json:"metadata"`
		} `json:"rows"`
	}

	err = json.Unmarshal(body, &listResp)
	if err != nil {
		return "", errors.Wrapf(err, "failed to parse list response: %s", string(body))
	}

	// Find the file with matching name
	for _, row := range listResp.Rows {
		if row.Metadata.Name == fileName {
			return row.ID, nil
		}
	}

	// File not found
	return "", nil
}
