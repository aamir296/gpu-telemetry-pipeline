package protocol

type ErrorResponse struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type MembershipRequest struct {
	Group    string `json:"group"`
	MemberID string `json:"member_id"`
}

type MembershipResponse struct {
	IncarnationID string   `json:"incarnation_id"`
	Generation    uint64   `json:"generation"`
	Members       []string `json:"members"`
	Partitions    []uint32 `json:"partitions,omitempty"`
}

type CycleRequest struct {
	DatasetID string `json:"dataset_id"`
	MemberID  string `json:"member_id"`
	Complete  bool   `json:"complete,omitempty"`
}

type CycleResponse struct {
	CycleID       uint64   `json:"cycle_id"`
	Generation    uint64   `json:"generation"`
	IncarnationID string   `json:"incarnation_id"`
	Members       []string `json:"members"`
	Ready         bool     `json:"ready"`
	WaitMillis    int64    `json:"wait_millis,omitempty"`
}

type PublishResponse struct {
	Accepted   int `json:"accepted"`
	Duplicates int `json:"duplicates"`
}

type FetchRequest struct {
	Group         string `json:"group"`
	MemberID      string `json:"member_id"`
	IncarnationID string `json:"incarnation_id"`
	Generation    uint64 `json:"generation"`
	MaxRecords    int    `json:"max_records"`
}

type CommitRequest struct {
	Group         string            `json:"group"`
	MemberID      string            `json:"member_id"`
	IncarnationID string            `json:"incarnation_id"`
	Generation    uint64            `json:"generation"`
	Offsets       map[uint32]uint64 `json:"offsets"`
}
