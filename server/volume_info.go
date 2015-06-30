package server

type volumeInfo struct {
	ID   uint32 `json:"id,omitempty"`
	Size int64  `json:"size,omitempty"`
}
