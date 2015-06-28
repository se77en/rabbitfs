package DirectoryServer

// type logicVolume struct {
// 	logicVolID   uint32
// 	physicVolMap map[uint32]string
// }

type volumeIDIP struct {
	ID uint32   `json:"id,omitempty"`
	IP []string `json:"ip,omitempty"`
}
