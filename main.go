package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"html/template"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// #region agent log
func debugLog(hypothesisID, location, message string, data map[string]interface{}) {
	payload := map[string]interface{}{
		"sessionId":    "837454",
		"runId":        "pre-fix",
		"hypothesisId": hypothesisID,
		"location":     location,
		"message":      message,
		"data":         data,
		"timestamp":    time.Now().UnixMilli(),
	}
	line, err := json.Marshal(payload)
	if err != nil {
		return
	}
	f, err := os.OpenFile("/Users/aniljoshi/.cursor/debug-logs/debug-837454.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return
	}
	defer f.Close()
	_, _ = f.Write(append(line, '\n'))
}

// #endregion

// SummaryInfo holds basic server metadata, including mysqladmin status metrics
type SummaryInfo struct {
	Hostname           string
	CollectedAt        string
	ServerVersion      string
	Uptime             string
	UptimeSec          string
	TxnIsolation       string
	ReadOnly           string
	ConnDetails        string
	Host               string
	Port               string
	User               string
	Threads            string
	Questions          string
	SlowQueries        string
	Opens              string
	FlushTables        string
	OpenTables         string
	QueriesPerSec      string
	ClusterFlowControl string // Dynamic flow control indicator in top summary
}

// KeyVal represents basic two-column status or config metrics
type KeyVal struct {
	Key   string
	Value string
}

// ProcessInfo holds details of active threads
type ProcessInfo struct {
	ID      string
	User    string
	Host    string
	DB      string
	Command string
	Time    string
	State   string
	Info    string
}

type BinlogEvent struct {
	LogName   string
	Pos       string
	EventType string
	ServerID  string
	EndLogPos string
	Info      string
}

// GroupMember holds details from performance_schema.replication_group_members
type GroupMember struct {
	ChannelName string
	MemberID    string
	MemberHost  string
	MemberPort  string
	MemberState string
	MemberRole  string
	Version     string
}

// ReplicationStatus holds replica status info
type ReplicationStatus struct {
	ChannelName              string
	ReplicaIORunning         string
	ReplicaSQLRunning        string
	SourceHost               string
	SourceUser               string
	SourcePort               string
	MasterLogFile            string
	ReadMasterLogPos         string
	RelayLogFile             string
	RelayLogPos              string
	RelayMasterLogFile       string
	ExecMasterLogPos         string
	SecondsBehind            string
	ReplicateDoDB            string
	ReplicateIgnoreDB        string
	ReplicateDoTable         string
	ReplicateIgnoreTable     string
	ReplicateWildDoTable     string
	ReplicateWildIgnoreTable string
	SQLDelay                 string
	ReplicateRewriteDB       string
	RetrievedGtidSet         string
	ExecutedGtidSet          string
	LastIOError              string
	LastSQLError             string
	AutoPosition             string
}

// ReplicaWorkerStatus holds per-worker applier error details
// from performance_schema.replication_applier_status_by_worker
type ReplicaWorkerStatus struct {
	ChannelName        string
	WorkerID           string
	LastErrorNumber    string
	LastErrorMessage   string
	LastErrorTimestamp string
}

// GTIDInfo holds the gtid_executed and gtid_purged values
type GTIDInfo struct {
	GTIDExecuted string
	GTIDPurged   string
}

// GRMemberStats holds Group Replication queue lengths for Flow Control checks
type GRMemberStats struct {
	MemberID     string
	CertQueue    int
	ApplierQueue int
}

// GaleraQueueStats holds PXC / Galera Queue telemetry metrics
type GaleraQueueStats struct {
	RecvQueue         string
	SendQueue         string
	FlowControlPaused string
}

// WsrepProviderOption is a single parsed key=value from wsrep_provider_options
type WsrepProviderOption struct {
	Key   string
	Value string
}

// WsrepQueueMax holds wsrep_local_recv_queue and wsrep_local_recv_queue_max
// from performance_schema.global_status, used for fc_limit comparison
type WsrepQueueMax struct {
	RecvQueue       string
	RecvQueueMax    string
	FCLimit         string // extracted gcs.fc_limit from wsrep_provider_options
	FCMasterSlave   string // extracted gcs.fc_master_slave
	FCSinglePrimary string // extracted gcs.fc_single_primary
	Alert           bool   // true when RecvQueueMax >= FCLimit
}

// GRFlowConfig holds the Group Replication flow control + consistency variables
type GRFlowConfig struct {
	CommStack        string
	Consistency      string
	ApplierThreshold string
	CertThreshold    string
	FlowControlMode  string
	BootstrapGroup   string
}

// GaleraSummary holds the key PXC cluster identity and status fields
type GaleraSummary struct {
	ClusterName       string
	ClusterSize       string
	ClusterStatus     string
	IncomingAddresses string
}

// RouterDetails represents the structured router records parsed from the custom metadata join query
type RouterDetails struct {
	RouterID        string
	RouterName      string
	RouterLabel     string
	Address         string
	Version         string
	LastCheckIn     string
	RWPort          string
	ROPort          string
	RWXPort         string
	ROXPort         string
	RWSplitPort     string
	MetadataUser    string
	ReadOnlyTargets string
}

// MemoryEvent represents sys.memory_global_by_current_bytes
type MemoryEvent struct {
	EventName    string
	CurrentAlloc string
}

// WaitEvent represents performance_schema.events_waits_summary_global_by_event_name
type WaitEvent struct {
	EventName    string
	CountStar    string
	TotalWaitSec string
	AvgWaitMs    string
}

// FileIOEvent represents performance_schema.file_summary_by_event_name
type FileIOEvent struct {
	EventName    string
	CountRead    string
	CountWrite   string
	MBRead       string
	MBWritten    string
	ReadLatency  string
	WriteLatency string
}

// DigestStat represents the detailed statement summary from performance_schema
type DigestStat struct {
	Digest               string
	SchemaName           string
	QuerySample          string
	ExecCount            string
	TotalExecSec         string
	AvgExecMs            string
	TotalLockSec         string
	RowsExamined         string
	RowsSent             string
	CreatedTmpTables     string
	CreatedTmpDiskTables string
	SortRows             string
	NoIndexUsed          string
	NoGoodIndexUsed      string
}

// LockWait represents information from sys.innodb_lock_waits
type LockWait struct {
	WaitStarted               string
	WaitAgeSecs               string
	LockedTable               string
	LockedIndex               string
	LockedType                string
	WaitingPid                string
	WaitingTrxId              string
	WaitingLockMode           string
	WaitingQuery              string
	BlockingPid               string
	BlockingTrxId             string
	BlockingLockMode          string
	BlockingQuery             string
	SQLKillBlockingConnection string
}

// SchemaTableLockWait holds raw metadata lock wait coordinates from sys.schema_table_lock_waits
type SchemaTableLockWait struct {
	ObjectSchema              string
	ObjectName                string
	WaitingPid                string
	WaitingLockType           string
	WaitingQuery              string
	WaitingQuerySecs          string
	BlockingPid               string
	BlockingLockType          string
	SQLKillBlockingConnection string
}

// DDLLock holds dynamic metadata lock waits from sys.schema_table_lock_waits JOIN events_statements_history
type DDLLock struct {
	BlockingPid      string
	ObjectSchema     string
	ObjectName       string
	BlockingThreadID string
	SQLQuery         string
}

// InnodbTrx holds active transaction details from information_schema.innodb_trx
type InnodbTrx struct {
	TrxID             string
	TrxState          string
	TrxStarted        string
	TrxWaitStarted    string
	TrxWeight         string
	TrxMysqlThreadID  string
	TrxQuery          string
	TrxOperation      string
	TrxTablesInUse    string
	TrxTablesLocked   string
	TrxLockStructs    string
	TrxRowsLocked     string
	TrxRowsModified   string
	TrxIsolationLevel string
}

// PfsThread holds background and foreground thread details from performance_schema.threads
type PfsThread struct {
	ThreadID           string
	Name               string
	Type               string
	ProcesslistID      string
	ProcesslistUser    string
	ProcesslistHost    string
	ProcesslistDB      string
	ProcesslistCommand string
	ProcesslistTime    string
	ProcesslistState   string
	ProcesslistInfo    string
}

// UserDetail holds MySQL account details from mysql.user
type UserDetail struct {
	User            string
	Host            string
	Plugin          string
	PasswordExpired string
}

// Recommendation holds an automated advice item based on current configurations or statuses
type Recommendation struct {
	Type        string // CRITICAL, WARNING, INFO
	Parameter   string
	Description string
}

// SemaphoreEntry is a single ranked hotspot line parsed from the SEMAPHORES block
type SemaphoreEntry struct {
	Count    int
	Location string
}

// SemaphoreCounters holds aggregate spin/wait counters from the SEMAPHORES block
type SemaphoreCounters struct {
	SpinWaits  string
	SpinRounds string
	OSWaits    string
	SpinRatio  string // SpinRounds / OSWaits efficiency indicator
}

// InnodbMutex holds one row from SHOW ENGINE INNODB MUTEX
type InnodbMutex struct {
	Type   string
	Name   string
	Status string
}

// SemaphoreAnalysis is the fully parsed SEMAPHORES section
type SemaphoreAnalysis struct {
	WaitingAt       []SemaphoreEntry // "has waited at" file:line hotspots
	Holders         []SemaphoreEntry // "has reserved it in mode" current holders
	CreatedAt       []SemaphoreEntry // "created in file" mutex creation sites
	LastWriteLocked []SemaphoreEntry // "Last time write locked" recent write-lock locations
	Counters        SemaphoreCounters
	RawBlock        string
	MutexRows       []InnodbMutex // SHOW ENGINE INNODB MUTEX rows
}

// ── InnoDB ClusterSet topology structs ──────────────────────────────────────

// CSTopologyNode is one member inside a cluster's topology map
type CSTopologyNode struct {
	Address        string
	MemberRole     string // PRIMARY / SECONDARY
	Mode           string // R/W or R/O
	Status         string // ONLINE / OFFLINE / ...
	Version        string
	ReplicationLag string // replicationLagFromImmediateSource
}

// CSCluster represents one cluster inside the ClusterSet
type CSCluster struct {
	Name                string
	ClusterRole         string // PRIMARY / REPLICA
	GlobalStatus        string
	Status              string
	StatusText          string
	Primary             string // primary member address (primary cluster only)
	TransactionSet      string
	TxConsistencyStatus string
	TxErrantGTID        string
	TxMissingGTID       string
	// ClusterSet replication channel (replica clusters only)
	CSReplSource              string
	CSReplReceiver            string
	CSReplReceiverStatus      string
	CSReplApplierStatus       string
	CSReplApplierThreads      int
	CSReplReceiverThreadState string
	CSReplApplierThreadState  string
	CSReplSSLMode             string
	Nodes                     []CSTopologyNode
}

// ClusterSetTopology is the top-level parsed result
type ClusterSetTopology struct {
	DomainName            string
	GlobalPrimaryInstance string
	PrimaryCluster        string
	Status                string
	StatusText            string
	MetadataServer        string
	PrimaryClusterData    *CSCluster
	ReplicaClusters       []CSCluster
}

// ── Raw JSON structs for unmarshalling mysqlsh output ───────────────────────

// CSRouter is one entry from myclusterset.listRouters()
type CSRouter struct {
	RouterKey     string
	Hostname      string
	LastCheckIn   string
	ROPort        string
	ROXPort       string
	RWPort        string
	RWXPort       string
	RWSplitPort   string
	TargetCluster string
	Version       string
}

// CSRoutingOptions holds global + per-router routing policy from routingOptions()
type CSRoutingOptions struct {
	DomainName              string
	GlobalInvalidatedPolicy string
	GlobalStatsFrequency    string
	GlobalTargetCluster     string
	RouterOverrides         map[string]string
}

type csRawRouterEntry struct {
	Hostname      string `json:"hostname"`
	LastCheckIn   string `json:"lastCheckIn"`
	RoPort        string `json:"roPort"`
	RoXPort       string `json:"roXPort"`
	RwPort        string `json:"rwPort"`
	RwXPort       string `json:"rwXPort"`
	TargetCluster string `json:"targetCluster"`
	Version       string `json:"version"`
}
type csRawRouterList struct {
	DomainName string                      `json:"domainName"`
	Routers    map[string]csRawRouterEntry `json:"routers"`
}
type csRawRoutingGlobal struct {
	InvalidatedClusterPolicy string `json:"invalidated_cluster_policy"`
	StatsUpdatesFrequency    int    `json:"stats_updates_frequency"`
	TargetCluster            string `json:"target_cluster"`
}
type csRawRoutingRouter struct {
	TargetCluster string `json:"target_cluster"`
}
type csRawRoutingOptions struct {
	DomainName string                        `json:"domainName"`
	Global     csRawRoutingGlobal            `json:"global"`
	Routers    map[string]csRawRoutingRouter `json:"routers"`
}

func parseRouterList(raw []byte) ([]CSRouter, error) {
	var rl csRawRouterList
	if err := json.Unmarshal(raw, &rl); err != nil {
		return nil, err
	}
	var out []CSRouter
	for key, r := range rl.Routers {
		out = append(out, CSRouter{RouterKey: key, Hostname: r.Hostname,
			LastCheckIn: r.LastCheckIn, ROPort: r.RoPort, ROXPort: r.RoXPort,
			RWPort: r.RwPort, RWXPort: r.RwXPort, TargetCluster: r.TargetCluster, Version: r.Version})
	}
	return out, nil
}

func parseRoutingOptions(raw []byte) (*CSRoutingOptions, error) {
	var ro csRawRoutingOptions
	if err := json.Unmarshal(raw, &ro); err != nil {
		return nil, err
	}
	overrides := map[string]string{}
	for k, v := range ro.Routers {
		if v.TargetCluster != "" {
			overrides[k] = v.TargetCluster
		}
	}
	return &CSRoutingOptions{
		DomainName: ro.DomainName, GlobalInvalidatedPolicy: ro.Global.InvalidatedClusterPolicy,
		GlobalStatsFrequency: fmt.Sprintf("%d", ro.Global.StatsUpdatesFrequency),
		GlobalTargetCluster:  ro.Global.TargetCluster, RouterOverrides: overrides,
	}, nil
}

// prettyJSONRaw indents JSON for HTML display (e.g. mysqlsh status output).
func prettyJSONRaw(raw json.RawMessage) string {
	if len(raw) <= 2 {
		return ""
	}
	var buf bytes.Buffer
	if err := json.Indent(&buf, raw, "", "  "); err != nil {
		return string(raw)
	}
	return buf.String()
}

// csRunMysqlsh runs a JS snippet via mysqlsh and returns stdout trimmed to first '{'
func csRunMysqlsh(bin, uri, password, jsCode string) ([]byte, error) {
	cmd := exec.Command(bin, "--uri="+uri, "--password="+password, "--js", "--no-wizard", "-e", jsCode)
	var out, errBuf bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errBuf
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("%v — %s", err, strings.TrimSpace(errBuf.String()))
	}
	raw := bytes.TrimSpace(out.Bytes())
	if idx := bytes.IndexByte(raw, '{'); idx >= 0 {
		raw = raw[idx:]
	}
	return raw, nil
}

type csRawNode struct {
	Address        string `json:"address"`
	MemberRole     string `json:"memberRole"`
	Mode           string `json:"mode"`
	Role           string `json:"role"`
	Status         string `json:"status"`
	Version        string `json:"version"`
	ReplicationLag string `json:"replicationLagFromImmediateSource"`
}

type csRawReplication struct {
	ApplierStatus        string `json:"applierStatus"`
	ApplierThreadState   string `json:"applierThreadState"`
	ApplierWorkerThreads int    `json:"applierWorkerThreads"`
	Receiver             string `json:"receiver"`
	ReceiverStatus       string `json:"receiverStatus"`
	ReceiverThreadState  string `json:"receiverThreadState"`
	ReplicationSslMode   string `json:"replicationSslMode"`
	Source               string `json:"source"`
}

type csRawCluster struct {
	ClusterRole           string               `json:"clusterRole"`
	GlobalStatus          string               `json:"globalStatus"`
	Status                string               `json:"status"`
	StatusText            string               `json:"statusText"`
	Primary               string               `json:"primary"`
	TransactionSet        string               `json:"transactionSet"`
	TxConsistencyStatus   string               `json:"transactionSetConsistencyStatus"`
	TxErrantGTID          string               `json:"transactionSetErrantGtidSet"`
	TxMissingGTID         string               `json:"transactionSetMissingGtidSet"`
	ClusterSetReplication *csRawReplication    `json:"clusterSetReplication"`
	Topology              map[string]csRawNode `json:"topology"`
}

type csRawRoot struct {
	DomainName            string                  `json:"domainName"`
	GlobalPrimaryInstance string                  `json:"globalPrimaryInstance"`
	PrimaryCluster        string                  `json:"primaryCluster"`
	Status                string                  `json:"status"`
	StatusText            string                  `json:"statusText"`
	MetadataServer        string                  `json:"metadataServer"`
	Clusters              map[string]csRawCluster `json:"clusters"`
}

// parseClusterSetJSON converts the raw mysqlsh JSON into ClusterSetTopology
func parseClusterSetJSON(raw []byte) (*ClusterSetTopology, error) {
	var root csRawRoot
	if err := json.Unmarshal(raw, &root); err != nil {
		return nil, err
	}
	topo := &ClusterSetTopology{
		DomainName:            root.DomainName,
		GlobalPrimaryInstance: root.GlobalPrimaryInstance,
		PrimaryCluster:        root.PrimaryCluster,
		Status:                root.Status,
		StatusText:            root.StatusText,
		MetadataServer:        root.MetadataServer,
	}
	for name, rc := range root.Clusters {
		cl := CSCluster{
			Name:                name,
			ClusterRole:         rc.ClusterRole,
			GlobalStatus:        rc.GlobalStatus,
			Status:              rc.Status,
			StatusText:          rc.StatusText,
			Primary:             rc.Primary,
			TransactionSet:      rc.TransactionSet,
			TxConsistencyStatus: rc.TxConsistencyStatus,
			TxErrantGTID:        rc.TxErrantGTID,
			TxMissingGTID:       rc.TxMissingGTID,
		}
		if rc.ClusterSetReplication != nil {
			r := rc.ClusterSetReplication
			cl.CSReplSource = r.Source
			cl.CSReplReceiver = r.Receiver
			cl.CSReplReceiverStatus = r.ReceiverStatus
			cl.CSReplApplierStatus = r.ApplierStatus
			cl.CSReplApplierThreads = r.ApplierWorkerThreads
			cl.CSReplReceiverThreadState = r.ReceiverThreadState
			cl.CSReplApplierThreadState = r.ApplierThreadState
			cl.CSReplSSLMode = r.ReplicationSslMode
		}
		for _, node := range rc.Topology {
			cl.Nodes = append(cl.Nodes, CSTopologyNode{
				Address:        node.Address,
				MemberRole:     node.MemberRole,
				Mode:           node.Mode,
				Status:         node.Status,
				Version:        node.Version,
				ReplicationLag: node.ReplicationLag,
			})
		}
		if name == root.PrimaryCluster {
			topo.PrimaryClusterData = &cl
		} else {
			topo.ReplicaClusters = append(topo.ReplicaClusters, cl)
		}
	}
	return topo, nil
}

// PageData holds all variables injected into the HTML template
type PageData struct {
	Summary              SummaryInfo
	EngineMetrics        []KeyVal
	ConfigVariables      []KeyVal
	StatusCounters       []KeyVal
	SemiSyncDetails      []KeyVal
	Processes            []ProcessInfo
	GroupMembers         []GroupMember
	ReplicationStates    []ReplicationStatus
	GRQueues             []GRMemberStats
	GRFlowControlLimit   string
	GRFlowConfig         GRFlowConfig
	GaleraSummary        GaleraSummary
	GaleraFlowControl    string
	GaleraQueues         GaleraQueueStats
	WsrepProviderOptions []WsrepProviderOption
	WsrepQueueMax        WsrepQueueMax
	MasterStatus         []KeyVal
	CurrentBinlogFile    string        // active binary log file from SHOW MASTER STATUS / SHOW BINARY LOG STATUS
	BinaryLogCount       int           // total binary log files from SHOW BINARY LOGS
	BinaryLogSize        int64         // total size in bytes for all binary logs
	BinaryLogSizeHuman   string        // human readable total binary log size for display
	CurrentBinlogEvents  []BinlogEvent // recent events from the active binary log file
	ReplicaWorkers       []ReplicaWorkerStatus
	GTIDInfo             GTIDInfo
	InnodbStatus         string
	SemaphoreAnalysis    SemaphoreAnalysis
	MemoryEvents         []MemoryEvent
	WaitEvents           []WaitEvent
	FileIOEvents         []FileIOEvent
	ClusterStatus        string
	ClusterSetStatus     string
	ClusterSetTopology   *ClusterSetTopology
	CSRouters            []CSRouter
	CSRoutingOptions     *CSRoutingOptions
	SingleClusterTopo    *ClusterSetTopology // for non-ClusterSet single cluster
	RouterList           string
	RouterOptions        string
	Routers              []RouterDetails
	DigestStats          []DigestStat
	LockWaits            []LockWait
	SchemaTableLockWaits []SchemaTableLockWait
	DDLLocks             []DDLLock
	InnodbTrx            []InnodbTrx
	PfsThreads           []PfsThread
	UserDetails          []UserDetail
	Recommendations      []Recommendation
}

// Format bytes into human-readable MB/GB strings where applicable
func formatBytes(valStr string) string {
	val, err := strconv.ParseFloat(valStr, 64)
	if err != nil {
		return valStr
	}
	if val < 0 {
		return valStr
	}
	const unit = 1024.0
	if val < unit {
		return fmt.Sprintf("%.0f B", val)
	}
	suffixes := []string{"KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB"}
	exp := 0
	for val >= unit && exp < len(suffixes) {
		val /= unit
		exp++
	}
	if exp == 0 {
		return fmt.Sprintf("%.0f B", val)
	}
	return fmt.Sprintf("%.2f %s", val, suffixes[exp-1])
}

// parseSemaphores extracts mutex/semaphore hotspot analysis from SHOW ENGINE INNODB STATUS output.
// It replicates the four summaries of the classic pt_print_semaphores_summary bash tool
// entirely in Go with zero OS dependencies.
func parseSemaphores(innodbStatus string) SemaphoreAnalysis {
	var result SemaphoreAnalysis

	// Extract the SEMAPHORES block between "SEMAPHORES" and the next "--------" section header
	semStart := strings.Index(innodbStatus, "SEMAPHORES")
	if semStart == -1 {
		return result
	}
	// Find the next section separator after SEMAPHORES
	rest := innodbStatus[semStart:]
	semEnd := strings.Index(rest[20:], "----------")
	var block string
	if semEnd == -1 {
		block = rest
	} else {
		block = rest[:semEnd+20]
	}
	result.RawBlock = block

	// frequency map helper
	type freqEntry struct {
		loc   string
		count int
	}
	buildTop := func(lines []string) []SemaphoreEntry {
		freq := map[string]int{}
		for _, l := range lines {
			l = strings.TrimSpace(l)
			if l != "" {
				freq[l]++
			}
		}
		var entries []SemaphoreEntry
		for loc, cnt := range freq {
			entries = append(entries, SemaphoreEntry{Count: cnt, Location: loc})
		}
		// sort descending by count
		for i := 0; i < len(entries); i++ {
			for j := i + 1; j < len(entries); j++ {
				if entries[j].Count > entries[i].Count {
					entries[i], entries[j] = entries[j], entries[i]
				}
			}
		}
		if len(entries) > 20 {
			entries = entries[:20]
		}
		return entries
	}

	var waitingAtLines, holderLines, createdAtLines, writeLockedLines []string

	for _, line := range strings.Split(block, "\n") {
		// "has waited at buf0buf.cc line 4321"  -> extract "file line N"
		if idx := strings.Index(line, "has waited at"); idx != -1 {
			parts := strings.Fields(line[idx+len("has waited at"):])
			if len(parts) >= 3 {
				waitingAtLines = append(waitingAtLines, parts[0]+" line "+parts[2])
			} else if len(parts) >= 1 {
				waitingAtLines = append(waitingAtLines, parts[0])
			}
		}
		// "has reserved it in mode ..."
		if idx := strings.Index(line, "has reserved it in mode"); idx != -1 {
			holderLines = append(holderLines, strings.TrimSpace(line))
		}
		// "created in file buf0buf.cc line 123"  strip address tokens
		if idx := strings.Index(line, "created in file"); idx != -1 {
			sub := line[idx:]
			// remove hex addresses like "at 0x7f1234abcd"
			var cleaned []string
			for _, tok := range strings.Fields(sub) {
				if strings.HasPrefix(tok, "0x") || strings.HasPrefix(tok, "at") {
					continue
				}
				cleaned = append(cleaned, tok)
			}
			createdAtLines = append(createdAtLines, strings.Join(cleaned, " "))
		}
		// "Last time write locked in file row0sel.cc line 5678"
		if idx := strings.Index(line, "Last time write locked"); idx != -1 {
			writeLockedLines = append(writeLockedLines, strings.TrimSpace(line[idx:]))
		}
	}

	result.WaitingAt = buildTop(waitingAtLines)
	result.Holders = buildTop(holderLines)
	result.CreatedAt = buildTop(createdAtLines)
	result.LastWriteLocked = buildTop(writeLockedLines)

	// Parse aggregate counters.
	// MySQL <= 8.0: "Mutex spin waits N, rounds M, OS waits K"
	// MySQL 8.4+:  separate lines "RW-shared spins N, rounds M, OS waits K"
	//               and            "Spin rounds per wait: X RW-shared, Y RW-excl, Z RW-sx"
	var totalSpinWaits, totalSpinRounds, totalOSWaits float64
	for _, line := range strings.Split(block, "\n") {
		line = strings.TrimSpace(line)
		// Old format
		if strings.HasPrefix(line, "Mutex spin waits") {
			parts := strings.Split(line, ",")
			if len(parts) >= 3 {
				totalSpinWaits, _ = strconv.ParseFloat(strings.TrimSpace(strings.TrimPrefix(parts[0], "Mutex spin waits")), 64)
				totalSpinRounds, _ = strconv.ParseFloat(strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(parts[1]), "rounds")), 64)
				totalOSWaits, _ = strconv.ParseFloat(strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(parts[2]), "OS waits")), 64)
			}
		}
		// New format — accumulate across RW-shared / RW-excl / RW-sx lines
		if strings.HasPrefix(line, "RW-shared spins") || strings.HasPrefix(line, "RW-excl spins") || strings.HasPrefix(line, "RW-sx spins") {
			parts := strings.Split(line, ",")
			// parts[0]: "RW-shared spins N"  parts[1]: " rounds M"  parts[2]: " OS waits K"
			if len(parts) >= 3 {
				spinsStr := strings.Fields(parts[0])
				if len(spinsStr) >= 3 {
					v, _ := strconv.ParseFloat(spinsStr[len(spinsStr)-1], 64)
					totalSpinWaits += v
				}
				roundsStr := strings.Fields(parts[1])
				if len(roundsStr) >= 2 {
					v, _ := strconv.ParseFloat(roundsStr[len(roundsStr)-1], 64)
					totalSpinRounds += v
				}
				osStr := strings.Fields(parts[2])
				if len(osStr) >= 3 {
					v, _ := strconv.ParseFloat(osStr[len(osStr)-1], 64)
					totalOSWaits += v
				}
			}
		}
	}
	result.Counters.SpinWaits = fmt.Sprintf("%.0f", totalSpinWaits)
	result.Counters.SpinRounds = fmt.Sprintf("%.0f", totalSpinRounds)
	result.Counters.OSWaits = fmt.Sprintf("%.0f", totalOSWaits)
	if totalOSWaits > 0 {
		result.Counters.SpinRatio = fmt.Sprintf("%.1f", totalSpinRounds/totalOSWaits)
	} else {
		result.Counters.SpinRatio = "0"
	}

	return result
}

// Format status keys to check if they should be converted to human readable forms
func toHumanReadable(key, val string) string {
	lowerKey := strings.ToLower(key)
	if strings.Contains(lowerKey, "size") || strings.Contains(lowerKey, "bytes") || strings.Contains(lowerKey, "allocation") || strings.Contains(lowerKey, "memory") || strings.Contains(lowerKey, "buffer_pool_bytes") {
		return formatBytes(val)
	}
	return val
}

// Safe string-to-int conversion helper for recommendation formulas
func getRawInt(valStr string) int {
	valStr = strings.Split(valStr, " ")[0]
	val, _ := strconv.Atoi(valStr)
	return val
}

// Helper to extract simple string/numeric values from JSON attributes column without external libraries
func fetchJSONValue(jsonStr, key, fallback string) string {
	searchKey := fmt.Sprintf(`"%s"`, key)
	idx := strings.Index(jsonStr, searchKey)
	if idx == -1 {
		return fallback
	}
	sub := jsonStr[idx+len(searchKey):]
	start := -1
	end := -1
	inQuotes := false
	for i, char := range sub {
		if char == ':' || char == ' ' || char == '\t' || char == '\r' || char == '\n' {
			continue
		}
		if char == '"' {
			if !inQuotes {
				inQuotes = true
				start = i + 1
			} else {
				end = i
				break
			}
		} else {
			if start == -1 {
				start = i
			}
			if char == ',' || char == '}' || char == ']' {
				end = i
				break
			}
		}
	}
	if start != -1 && end != -1 && end > start {
		trimmed := strings.Trim(strings.TrimSpace(sub[start:end]), `"`)
		if trimmed != "" {
			return trimmed
		}
	}
	return fallback
}

// Executed precise SQL metadata join query requested by the user
// collectInnoDBTopologySQL builds ClusterSet and single-cluster topology
// entirely from mysql_innodb_cluster_metadata SQL tables — no mysqlsh needed.
func collectInnoDBTopologySQL(db *sql.DB) (csTop *ClusterSetTopology, singleTop *ClusterSetTopology) {
	// Check schema exists
	var schemaExists int
	db.QueryRow(`SELECT COUNT(*) FROM information_schema.schemata
		WHERE schema_name = 'mysql_innodb_cluster_metadata'`).Scan(&schemaExists)
	if schemaExists == 0 {
		return nil, nil
	}

	// ── ClusterSet identity ───────────────────────────────────────────────────
	var domainName, csStatus, globalPrimary string
	_ = db.QueryRow(`
		SELECT
			IFNULL(domain_name,''),
			IFNULL(JSON_UNQUOTE(JSON_EXTRACT(attributes,'$.globalStatus')),''),
			IFNULL(JSON_UNQUOTE(JSON_EXTRACT(attributes,'$.primaryCluster')),'')
		FROM mysql_innodb_cluster_metadata.clustersets
		LIMIT 1`).Scan(&domainName, &csStatus, &globalPrimary)

	isClusterSet := domainName != ""

	// ── All clusters in this setup ────────────────────────────────────────────
	type sqlCluster struct {
		id          string
		name        string
		clusterType string // gr or ar
	}
	var allClusters []sqlCluster
	cRows, cErr := db.Query(`
		SELECT cluster_id, cluster_name, IFNULL(cluster_type,'gr')
		FROM mysql_innodb_cluster_metadata.clusters
		ORDER BY cluster_name`)
	if cErr == nil {
		defer cRows.Close()
		for cRows.Next() {
			var sc sqlCluster
			cRows.Scan(&sc.id, &sc.name, &sc.clusterType)
			allClusters = append(allClusters, sc)
		}
	}
	if len(allClusters) == 0 {
		return nil, nil
	}

	// ── Determine primary cluster name from clusterset_members ───────────────
	var primaryClusterName string
	_ = db.QueryRow(`
		SELECT c.cluster_name
		FROM mysql_innodb_cluster_metadata.clusters c
		JOIN mysql_innodb_cluster_metadata.clusterset_members csm
		  ON c.cluster_id = csm.cluster_id
		WHERE csm.master_cluster_id IS NULL OR csm.master_cluster_id = csm.cluster_id
		LIMIT 1`).Scan(&primaryClusterName)
	if primaryClusterName == "" && len(allClusters) == 1 {
		primaryClusterName = allClusters[0].name
	}

	// ── Per-cluster topology nodes ────────────────────────────────────────────
	buildCluster := func(clusterID, clusterName string) CSCluster {
		cl := CSCluster{Name: clusterName}
		// Determine if this cluster is primary or replica in a ClusterSet
		if isClusterSet {
			var role string
			_ = db.QueryRow(`
				SELECT CASE WHEN master_cluster_id IS NULL OR master_cluster_id = cluster_id
				            THEN 'PRIMARY' ELSE 'REPLICA' END
				FROM mysql_innodb_cluster_metadata.clusterset_members
				WHERE cluster_id = ? LIMIT 1`, clusterID).Scan(&role)
			cl.ClusterRole = role
		} else {
			cl.ClusterRole = "PRIMARY"
		}

		// Nodes from instances + GR member status
		iRows, iErr := db.Query(`
			SELECT
				IFNULL(i.address,''),
				IFNULL(i.mysql_server_uuid,''),
				IFNULL(JSON_UNQUOTE(JSON_EXTRACT(i.addresses,'$.mysqlClassic')),'') AS classic_addr,
				IFNULL(m.MEMBER_ROLE,'UNKNOWN') AS member_role,
				IFNULL(m.MEMBER_STATE,'UNKNOWN') AS member_state,
				IFNULL(m.MEMBER_VERSION,'') AS member_version
			FROM mysql_innodb_cluster_metadata.instances i
			LEFT JOIN performance_schema.replication_group_members m
			  ON i.mysql_server_uuid = m.MEMBER_ID
			WHERE i.cluster_id = ?
			ORDER BY i.instance_id`, clusterID)
		if iErr == nil {
			defer iRows.Close()
			for iRows.Next() {
				var addr, uuid, classicAddr, role, state, version string
				iRows.Scan(&addr, &uuid, &classicAddr, &role, &state, &version)
				displayAddr := addr
				if displayAddr == "" {
					displayAddr = classicAddr
				}
				mode := "R/O"
				if role == "PRIMARY" {
					mode = "R/W"
				}
				cl.Nodes = append(cl.Nodes, CSTopologyNode{
					Address: displayAddr, MemberRole: role,
					Mode: mode, Status: state, Version: version,
				})
			}
		}

		// ClusterSet replication channel for REPLICA clusters
		if cl.ClusterRole == "REPLICA" {
			var src, rcvr, rcvrState, applState string
			var workers int
			_ = db.QueryRow(`
				SELECT
					IFNULL(CONCAT(css.HOST,':',css.PORT),'') AS source,
					IFNULL(csa.HOST,'') AS receiver,
					IFNULL(ios.SERVICE_STATE,'') AS receiver_state,
					IFNULL(apps.SERVICE_STATE,'') AS applier_state,
					IFNULL(asw.WORKER_COUNT,0) AS workers
				FROM performance_schema.replication_connection_status ios
				LEFT JOIN performance_schema.replication_connection_configuration css
				  ON ios.CHANNEL_NAME = css.CHANNEL_NAME
				LEFT JOIN performance_schema.replication_applier_status apps
				  ON ios.CHANNEL_NAME = apps.CHANNEL_NAME
				LEFT JOIN (
					SELECT CHANNEL_NAME, COUNT(*) AS WORKER_COUNT
					FROM performance_schema.replication_applier_status_by_worker
					GROUP BY CHANNEL_NAME
				) asw ON ios.CHANNEL_NAME = asw.CHANNEL_NAME
				LEFT JOIN performance_schema.replication_connection_configuration csa
				  ON ios.CHANNEL_NAME = csa.CHANNEL_NAME
				WHERE ios.CHANNEL_NAME LIKE '%clusterset%'
				LIMIT 1`).Scan(&src, &rcvr, &rcvrState, &applState, &workers)
			cl.CSReplSource = src
			cl.CSReplReceiver = rcvr
			cl.CSReplReceiverStatus = rcvrState
			cl.CSReplApplierStatus = applState
			cl.CSReplApplierThreads = workers

			// GTID info
			var txSet, txConsistency, txErrant, txMissing string
			_ = db.QueryRow(`
				SELECT
					IFNULL(RECEIVED_TRANSACTION_SET,''),
					'OK',
					'',
					''
				FROM performance_schema.replication_connection_status
				WHERE CHANNEL_NAME LIKE '%clusterset%'
				LIMIT 1`).Scan(&txSet, &txConsistency, &txErrant, &txMissing)
			cl.TransactionSet = txSet
			cl.TxConsistencyStatus = txConsistency
		} else {
			// Primary: get executed GTID set
			_ = db.QueryRow(`SELECT IFNULL(VARIABLE_VALUE,'')
				FROM performance_schema.global_variables
				WHERE VARIABLE_NAME = 'gtid_executed'`).Scan(&cl.TransactionSet)
		}

		// Cluster global status from GR summary
		var onlineCount int
		db.QueryRow(`SELECT COUNT(*) FROM performance_schema.replication_group_members
			WHERE MEMBER_STATE = 'ONLINE'`).Scan(&onlineCount)
		if onlineCount > 0 {
			cl.GlobalStatus = "OK"
		} else {
			cl.GlobalStatus = "ERROR"
		}

		return cl
	}

	if isClusterSet {
		topo := &ClusterSetTopology{
			DomainName:     domainName,
			Status:         csStatus,
			PrimaryCluster: primaryClusterName,
		}
		// Global primary from GR
		_ = db.QueryRow(`
			SELECT IFNULL(CONCAT(i.address),'')
			FROM mysql_innodb_cluster_metadata.instances i
			JOIN performance_schema.replication_group_members m
			  ON i.mysql_server_uuid = m.MEMBER_ID
			WHERE m.MEMBER_ROLE = 'PRIMARY'
			LIMIT 1`).Scan(&topo.GlobalPrimaryInstance)

		for _, sc := range allClusters {
			cl := buildCluster(sc.id, sc.name)
			if sc.name == primaryClusterName {
				clCopy := cl
				topo.PrimaryClusterData = &clCopy
			} else {
				topo.ReplicaClusters = append(topo.ReplicaClusters, cl)
			}
		}
		return topo, nil
	}

	// Single cluster (no ClusterSet)
	sc := allClusters[0]
	cl := buildCluster(sc.id, sc.name)
	var globalPrimaryAddr string
	_ = db.QueryRow(`
		SELECT IFNULL(i.address,'')
		FROM mysql_innodb_cluster_metadata.instances i
		JOIN performance_schema.replication_group_members m
		  ON i.mysql_server_uuid = m.MEMBER_ID
		WHERE m.MEMBER_ROLE = 'PRIMARY'
		LIMIT 1`).Scan(&globalPrimaryAddr)
	singleTopo := &ClusterSetTopology{
		GlobalPrimaryInstance: globalPrimaryAddr,
		Status:                cl.GlobalStatus,
		PrimaryClusterData:    &cl,
	}
	return nil, singleTopo
}

// collectRoutingOptionsSQL fetches router routing configuration from metadata tables
func collectRoutingOptionsSQL(db *sql.DB) *CSRoutingOptions {
	var schemaExists int
	db.QueryRow(`SELECT COUNT(*) FROM information_schema.schemata
		WHERE schema_name = 'mysql_innodb_cluster_metadata'`).Scan(&schemaExists)
	if schemaExists == 0 {
		return nil
	}

	opts := &CSRoutingOptions{RouterOverrides: map[string]string{}}

	// Global routing options
	_ = db.QueryRow(`
		SELECT
			IFNULL(JSON_UNQUOTE(JSON_EXTRACT(router_options,'$.target_cluster')),''),
			IFNULL(JSON_UNQUOTE(JSON_EXTRACT(router_options,'$.invalidated_cluster_policy')),''),
			IFNULL(JSON_UNQUOTE(JSON_EXTRACT(router_options,'$.stats_updates_frequency')),'0')
		FROM mysql_innodb_cluster_metadata.v2_router_options
		LIMIT 1`).Scan(&opts.GlobalTargetCluster, &opts.GlobalInvalidatedPolicy, &opts.GlobalStatsFrequency)

	// Per-router target_cluster overrides
	rRows, err := db.Query(`
		SELECT
			IFNULL(r.router_name,''),
			IFNULL(JSON_UNQUOTE(JSON_EXTRACT(o.router_options,'$.target_cluster')),'')
		FROM mysql_innodb_cluster_metadata.v2_routers r
		JOIN mysql_innodb_cluster_metadata.v2_router_options o USING(router_id)
		WHERE JSON_UNQUOTE(JSON_EXTRACT(o.router_options,'$.target_cluster')) IS NOT NULL
		  AND JSON_UNQUOTE(JSON_EXTRACT(o.router_options,'$.target_cluster')) != ''`)
	if err == nil {
		defer rRows.Close()
		for rRows.Next() {
			var name, target string
			if rRows.Scan(&name, &target) == nil && name != "" {
				opts.RouterOverrides[name] = target
			}
		}
	}

	if opts.GlobalTargetCluster == "" && opts.GlobalInvalidatedPolicy == "" {
		return nil
	}
	return opts
}

func queryRouterMetadata(db *sql.DB) []RouterDetails {
	var list []RouterDetails

	var exists int
	err := db.QueryRow(`
		SELECT COUNT(*) 
		FROM information_schema.tables 
		WHERE TABLE_SCHEMA = 'mysql_innodb_cluster_metadata' 
		  AND TABLE_NAME IN ('v2_routers', 'v2_router_options')
	`).Scan(&exists)

	if err != nil || exists < 2 {
		log.Printf("[HA Discovery] mysql_innodb_cluster_metadata tables not found. Skipping router join query.")
		return list
	}

	query := `
		SELECT
			r.router_id,
			IFNULL(r.router_name, ''),
			IFNULL(o.router_label, ''),
			IFNULL(r.address, ''),
			IFNULL(r.version, ''),
			IFNULL(r.last_check_in, ''),
			IFNULL(JSON_UNQUOTE(JSON_EXTRACT(r.attributes, '$.RWEndpoint')), '')      AS rw_port,
			IFNULL(JSON_UNQUOTE(JSON_EXTRACT(r.attributes, '$.ROEndpoint')), '')      AS ro_port,
			IFNULL(JSON_UNQUOTE(JSON_EXTRACT(r.attributes, '$.RWXEndpoint')), '')     AS rwx_port,
			IFNULL(JSON_UNQUOTE(JSON_EXTRACT(r.attributes, '$.ROXEndpoint')), '')     AS rox_port,
			IFNULL(JSON_UNQUOTE(JSON_EXTRACT(r.attributes, '$.RWSplitEndpoint')), '') AS rw_split_port,
			IFNULL(JSON_UNQUOTE(JSON_EXTRACT(r.attributes, '$.MetadataUser')), '')    AS metadata_user,
			IFNULL(JSON_UNQUOTE(JSON_EXTRACT(o.router_options, '$.read_only_targets')), '') AS read_only_targets
		FROM mysql_innodb_cluster_metadata.v2_routers r
		JOIN mysql_innodb_cluster_metadata.v2_router_options o
			USING(router_id)
		WHERE r.router_name IS NOT NULL
		  AND r.router_name <> '';`

	rows, err := db.Query(query)
	if err != nil {
		log.Printf("[HA Discovery] Error executing requested Router JOIN query: %v", err)
		return list
	}
	defer rows.Close()

	for rows.Next() {
		var rd RouterDetails
		err := rows.Scan(
			&rd.RouterID,
			&rd.RouterName,
			&rd.RouterLabel,
			&rd.Address,
			&rd.Version,
			&rd.LastCheckIn,
			&rd.RWPort,
			&rd.ROPort,
			&rd.RWXPort,
			&rd.ROXPort,
			&rd.RWSplitPort,
			&rd.MetadataUser,
			&rd.ReadOnlyTargets,
		)
		if err == nil {
			list = append(list, rd)
		}
	}
	return list
}

func main() {
	// 1. Command Line Flags for Connection Parameters (Zero Hardcoding)
	user := flag.String("user", "root", "MySQL database user")
	password := flag.String("password", "", "MySQL database password")
	host := flag.String("host", "127.0.0.1", "MySQL host address")
	port := flag.Int("port", 3306, "MySQL host port")
	output := flag.String("output", "mysql_gather.html", "Path to write the standalone HTML report")
	flag.Parse()

	log.Printf("Starting MySQL Gatherer. Connecting to %s:%d...", *host, *port)

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/?parseTime=true&loc=Local", *user, *password, *host, *port)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("Error parsing connection parameters: %v", err)
	}
	defer db.Close()

	db.SetConnMaxLifetime(15 * time.Second)
	db.SetConnMaxIdleTime(5 * time.Second)
	db.SetMaxIdleConns(1)
	db.SetMaxOpenConns(2)

	if err := db.Ping(); err != nil {
		log.Fatalf("Failed to establish database connection check: %v", err)
	}

	// 3. Populate System Summary Information
	data := PageData{
		Summary: SummaryInfo{
			CollectedAt:        time.Now().UTC().Format("2006-01-02 15:04:05 (UTC)"),
			Host:               *host,
			Port:               strconv.Itoa(*port),
			User:               *user,
			ClusterFlowControl: "Not Configured / Single Instance", // Default fallback
		},
	}

	_ = db.QueryRow("SELECT @@hostname;").Scan(&data.Summary.Hostname)
	_ = db.QueryRow("SELECT VERSION();").Scan(&data.Summary.ServerVersion)
	_ = db.QueryRow("SELECT @@transaction_isolation;").Scan(&data.Summary.TxnIsolation)

	isMariaDB := strings.Contains(strings.ToLower(data.Summary.ServerVersion), "mariadb")

	var ro int
	if err := db.QueryRow("SELECT @@global.read_only;").Scan(&ro); err == nil {
		if ro == 1 {
			data.Summary.ReadOnly = "True (Read Only)"
		} else {
			data.Summary.ReadOnly = "False (Read/Write)"
		}
	}

	var uptimeSeconds int64

	variablesMap := make(map[string]string)
	statusMap := make(map[string]string)

	// Section 1: Innodb metrics from information_schema.innodb_metrics
	if rows, err := db.Query(`
		SELECT NAME,
		       CASE
		           WHEN NAME = 'log_lsn_checkpoint_age' THEN ROUND(COUNT/1024/1024, 2)
		           ELSE COUNT
		       END AS VALUE
		FROM information_schema.innodb_metrics
		WHERE NAME IN ('log_lsn_checkpoint_age', 'trx_rseg_history_len', 'ibuf_size')
	`); err == nil {
		metrics := map[string]string{}
		for rows.Next() {
			var name string
			var value sql.NullString
			if err := rows.Scan(&name, &value); err == nil {
				if value.Valid {
					metrics[name] = value.String
				}
			}
		}
		rows.Close()
		if v, ok := metrics["log_lsn_checkpoint_age"]; ok {
			data.EngineMetrics = append(data.EngineMetrics, KeyVal{"Checkpoint Age", v})
		}
		if v, ok := metrics["trx_rseg_history_len"]; ok {
			data.EngineMetrics = append(data.EngineMetrics, KeyVal{"History list length", v})
		}
		if v, ok := metrics["ibuf_size"]; ok {
			data.EngineMetrics = append(data.EngineMetrics, KeyVal{"Ibuf:size", v})
		}
	}

	// Section 2: SHOW GLOBAL VARIABLES (formatted to human-readable)
	globalVarQuery := "SHOW GLOBAL VARIABLES;"
	if rows, err := db.Query(globalVarQuery); err == nil {
		for rows.Next() {
			var kv KeyVal
			if err := rows.Scan(&kv.Key, &kv.Value); err == nil {
				variablesMap[strings.ToLower(kv.Key)] = kv.Value
				kv.Value = toHumanReadable(kv.Key, kv.Value)
				data.ConfigVariables = append(data.ConfigVariables, kv)
			}
		}
		rows.Close()
	}

	// Section 3: SHOW GLOBAL STATUS (formatted to human-readable)
	globalStatusQuery := "SHOW GLOBAL STATUS;"
	if rows, err := db.Query(globalStatusQuery); err == nil {
		for rows.Next() {
			var kv KeyVal
			if err := rows.Scan(&kv.Key, &kv.Value); err == nil {
				statusMap[strings.ToLower(kv.Key)] = kv.Value
				kv.Value = toHumanReadable(kv.Key, kv.Value)
				data.StatusCounters = append(data.StatusCounters, kv)
			}
		}
		rows.Close()
	}

	// Section 5a: Semi-synchronous replication detail variables
	semiSyncKeys := []string{
		"rpl_semi_sync_master_enabled",
		"rpl_semi_sync_slave_enabled",
		"rpl_semi_sync_master_timeout",
		"rpl_semi_sync_master_wait_point",
		"rpl_semi_sync_master_wait_for_slave_count",
		"Rpl_semi_sync_master_tx_waits",
		"Rpl_semi_sync_master_tx_wait_time",
		"Rpl_semi_sync_master_clients",
		"Rpl_semi_sync_master_net_wait_time",
		"Rpl_semi_sync_master_no_tx",
	}
	for _, key := range semiSyncKeys {
		value := ""
		if v, ok := variablesMap[strings.ToLower(key)]; ok && v != "" {
			value = v
		} else if v, ok := statusMap[strings.ToLower(key)]; ok && v != "" {
			value = v
		}
		if value == "" {
			value = "N/A"
		}
		data.SemiSyncDetails = append(data.SemiSyncDetails, KeyVal{Key: key, Value: toHumanReadable(key, value)})
	}

	appendEngineStatusMetric := func(label, key string) {
		if v, ok := statusMap[strings.ToLower(key)]; ok && v != "" {
			if label == "Total large memory allocated" {
				v = formatBytes(v)
			}
			data.EngineMetrics = append(data.EngineMetrics, KeyVal{label, v})
		}
	}
	appendEngineStatusMetric("Pending normal aio reads", "Innodb_data_pending_reads")
	appendEngineStatusMetric("Pending normal aio writes", "Innodb_data_pending_writes")
	appendEngineStatusMetric("Pending flushes (log)", "Innodb_os_log_pending_writes")
	appendEngineStatusMetric("Pending flushes (buffer pool)", "Innodb_buffer_pool_pages_flushed")
	appendEngineStatusMetric("Queries inside InnoDB", "Innodb_thread_active")
	appendEngineStatusMetric("Queries in queue", "Innodb_thread_queue")
	appendEngineStatusMetric("Total large memory allocated", "Innodb_buffer_pool_bytes_data")

	// Populate summary values from global status
	if uptimeValue, ok := statusMap["uptime"]; ok && uptimeValue != "" {
		if up, err := strconv.ParseInt(uptimeValue, 10, 64); err == nil {
			uptimeSeconds = up
			data.Summary.UptimeSec = fmt.Sprintf("%d", uptimeSeconds)
			days := uptimeSeconds / 86400
			hours := (uptimeSeconds % 86400) / 3600
			minutes := (uptimeSeconds % 3600) / 60
			data.Summary.Uptime = fmt.Sprintf("%dd %dh %dm", days, hours, minutes)
		}
	}
	data.Summary.Threads = statusMap["threads_connected"]
	data.Summary.Questions = statusMap["questions"]
	data.Summary.SlowQueries = statusMap["slow_queries"]
	data.Summary.Opens = statusMap["opened_tables"]
	data.Summary.FlushTables = statusMap["flush_commands"]
	data.Summary.OpenTables = statusMap["open_tables"]
	if uptimeSeconds > 0 && data.Summary.Questions != "" {
		qCount, _ := strconv.ParseFloat(data.Summary.Questions, 64)
		data.Summary.QueriesPerSec = fmt.Sprintf("%.3f", qCount/float64(uptimeSeconds))
	} else {
		data.Summary.QueriesPerSec = "0.000"
	}

	// Section 4: SHOW ENGINE INNODB STATUS Raw Text
	var engine string
	var statusText string
	if err := db.QueryRow("SHOW ENGINE INNODB STATUS;").Scan(&engine, &statusText, &statusText); err == nil {
		data.InnodbStatus = statusText
		data.SemaphoreAnalysis = parseSemaphores(statusText)
	}

	// Collect SHOW ENGINE INNODB MUTEX into SemaphoreAnalysis
	if mutexRows, err := db.Query("SHOW ENGINE INNODB MUTEX;"); err == nil {
		for mutexRows.Next() {
			var m InnodbMutex
			if err := mutexRows.Scan(&m.Type, &m.Name, &m.Status); err == nil {
				data.SemaphoreAnalysis.MutexRows = append(data.SemaphoreAnalysis.MutexRows, m)
			}
		}
		mutexRows.Close()
	}

	// Section 5: HA / Replication Topology Consolidated
	replicaRows, rErr := db.Query("SHOW REPLICA STATUS;")
	if rErr != nil {
		replicaRows, rErr = db.Query("SHOW SLAVE STATUS;")
	}
	if rErr == nil {
		cols, _ := replicaRows.Columns()
		for replicaRows.Next() {
			values := make([]sql.RawBytes, len(cols))
			scanArgs := make([]interface{}, len(cols))
			for i := range values {
				scanArgs[i] = &values[i]
			}
			if err := replicaRows.Scan(scanArgs...); err == nil {
				status := ReplicationStatus{}
				for i, col := range cols {
					valStr := string(values[i])
					switch strings.ToUpper(col) {
					case "CONNECTION_NAME", "CHANNEL_NAME":
						status.ChannelName = valStr
					case "REPLICA_IO_RUNNING", "SLAVE_IO_RUNNING":
						status.ReplicaIORunning = valStr
					case "REPLICA_SQL_RUNNING", "SLAVE_SQL_RUNNING":
						status.ReplicaSQLRunning = valStr
					case "SOURCE_HOST", "MASTER_HOST":
						status.SourceHost = valStr
					case "SOURCE_USER", "MASTER_USER":
						status.SourceUser = valStr
					case "SOURCE_PORT", "MASTER_PORT":
						status.SourcePort = valStr
					case "SOURCE_LOG_FILE", "MASTER_LOG_FILE":
						status.MasterLogFile = valStr
					case "READ_SOURCE_LOG_POS", "READ_MASTER_LOG_POS":
						status.ReadMasterLogPos = valStr
					case "RELAY_LOG_FILE":
						status.RelayLogFile = valStr
					case "RELAY_LOG_POS":
						status.RelayLogPos = valStr
					case "RELAY_SOURCE_LOG_FILE", "RELAY_MASTER_LOG_FILE":
						status.RelayMasterLogFile = valStr
					case "EXEC_SOURCE_LOG_POS", "EXEC_MASTER_LOG_POS":
						status.ExecMasterLogPos = valStr
					case "SECONDS_BEHIND_SOURCE", "SECONDS_BEHIND_MASTER":
						status.SecondsBehind = valStr
					case "RETRIEVED_GTID_SET":
						status.RetrievedGtidSet = valStr
					case "EXECUTED_GTID_SET":
						status.ExecutedGtidSet = valStr
					case "LAST_IO_ERROR":
						status.LastIOError = valStr
					case "LAST_SQL_ERROR":
						status.LastSQLError = valStr
					case "AUTO_POSITION":
						status.AutoPosition = valStr
					}
				}
				if status.ChannelName == "" {
					status.ChannelName = "default"
				}
				data.ReplicationStates = append(data.ReplicationStates, status)
			}
		}
		replicaRows.Close()
	}

	// ── NEW: Replication Applier Worker Status ──────────────────────────────
	// Mirrors: SELECT CHANNEL_NAME, THREAD_ID, LAST_ERROR_NUMBER,
	//                 LAST_ERROR_MESSAGE, LAST_ERROR_TIMESTAMP
	//          FROM performance_schema.replication_applier_status_by_worker
	workerSQL := `SELECT
		IFNULL(CHANNEL_NAME, '') AS CHANNEL_NAME,
		WORKER_ID,
		LAST_ERROR_NUMBER,
		IFNULL(LAST_ERROR_MESSAGE, '') AS LAST_ERROR_MESSAGE,
		IFNULL(DATE_FORMAT(LAST_ERROR_TIMESTAMP, '%Y-%m-%d %H:%i:%s.%f'), '') AS LAST_ERROR_TIMESTAMP
	FROM performance_schema.replication_applier_status_by_worker
	ORDER BY CHANNEL_NAME, WORKER_ID`
	if isMariaDB {
		workerSQL = `SELECT
			IFNULL(CHANNEL_NAME, '') AS CHANNEL_NAME,
			THREAD_ID,
			LAST_ERROR_NUMBER,
			IFNULL(LAST_ERROR_MESSAGE, '') AS LAST_ERROR_MESSAGE,
			IFNULL(DATE_FORMAT(LAST_ERROR_TIMESTAMP, '%Y-%m-%d %H:%i:%s.%f'), '') AS LAST_ERROR_TIMESTAMP
		FROM performance_schema.replication_applier_status_by_worker
		ORDER BY CHANNEL_NAME, THREAD_ID`
	}
	if wrows, werr := db.Query(workerSQL); werr == nil {
		for wrows.Next() {
			var w ReplicaWorkerStatus
			var chanName, errMsg, errTs sql.NullString
			var errNum sql.NullInt64
			if scanErr := wrows.Scan(&chanName, &w.WorkerID, &errNum, &errMsg, &errTs); scanErr == nil {
				if chanName.Valid {
					w.ChannelName = chanName.String
				}
				if errNum.Valid {
					w.LastErrorNumber = strconv.FormatInt(errNum.Int64, 10)
				} else {
					w.LastErrorNumber = "0"
				}
				if errMsg.Valid {
					w.LastErrorMessage = errMsg.String
				}
				if errTs.Valid {
					w.LastErrorTimestamp = errTs.String
				}
				data.ReplicaWorkers = append(data.ReplicaWorkers, w)
			} else {
				log.Printf("[replication_applier_status_by_worker] scan error: %v", scanErr)
			}
		}
		wrows.Close()
	} else {
		log.Printf("[replication_applier_status_by_worker] query error: %v", werr)
	}

	// ── NEW: GTID State ──────────────────────────────────────────────────────
	var gtidSQL string
	if isMariaDB {
		gtidSQL = `SELECT VARIABLE_NAME, IFNULL(VARIABLE_VALUE, '') AS VARIABLE_VALUE
		FROM information_schema.global_variables
		WHERE VARIABLE_NAME IN ('gtid_current_pos', 'gtid_slave_pos')
		ORDER BY VARIABLE_NAME`
	} else {
		gtidSQL = `SELECT VARIABLE_NAME, IFNULL(VARIABLE_VALUE, '') AS VARIABLE_VALUE
		FROM performance_schema.global_variables
		WHERE VARIABLE_NAME IN ('gtid_executed', 'gtid_purged')
		ORDER BY VARIABLE_NAME`
	}
	if grows, gerr := db.Query(gtidSQL); gerr == nil {
		for grows.Next() {
			var name, value string
			if scanErr := grows.Scan(&name, &value); scanErr == nil {
				switch strings.ToLower(name) {
				case "gtid_executed", "gtid_current_pos":
					data.GTIDInfo.GTIDExecuted = value
				case "gtid_purged", "gtid_slave_pos":
					data.GTIDInfo.GTIDPurged = value
				}
			}
		}
		grows.Close()
	} else {
		log.Printf("[gtid_state] query error: %v", gerr)
	}

	// Connected Group Replication node list
	grQuery := `SELECT IFNULL(CHANNEL_NAME, 'group_replication'), MEMBER_ID, MEMBER_HOST, MEMBER_PORT, MEMBER_STATE, MEMBER_ROLE, MEMBER_VERSION 
	            FROM performance_schema.replication_group_members;`
	if rows, err := db.Query(grQuery); err == nil {
		for rows.Next() {
			var m GroupMember
			if err := rows.Scan(&m.ChannelName, &m.MemberID, &m.MemberHost, &m.MemberPort, &m.MemberState, &m.MemberRole, &m.Version); err == nil {
				data.GroupMembers = append(data.GroupMembers, m)
			}
		}
		rows.Close()
	}

	// Dynamic Flow Control State Tracking Evaluator
	clusterFCStatus := "Inactive (Healthy)"
	isClusterConfigured := false

	// Group Replication Flow Control status checks
	var grFCActive string
	_ = db.QueryRow("SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME = 'group_replication_flow_control_active'").Scan(&grFCActive)
	if grFCActive != "" {
		isClusterConfigured = true
		if grFCActive == "ON" || grFCActive == "1" || strings.ToLower(grFCActive) == "active" {
			clusterFCStatus = "Active (Group Replication Throttling)"
		} else {
			clusterFCStatus = "Inactive (Group Replication Healthy)"
		}
		data.GRFlowControlLimit = fmt.Sprintf("Flow Control Active State: %s", grFCActive)
	}

	// Group Replication Queue sizes
	grQueueQuery := `
		SELECT MEMBER_ID, 
		       COUNT_TRANSACTIONS_IN_QUEUE AS cert_queue, 
		       COUNT_TRANSACTIONS_REMOTE_IN_APPLIER_QUEUE AS applier_queue 
		FROM performance_schema.replication_group_member_stats;`
	if rows, err := db.Query(grQueueQuery); err == nil {
		for rows.Next() {
			var qs GRMemberStats
			if err := rows.Scan(&qs.MemberID, &qs.CertQueue, &qs.ApplierQueue); err == nil {
				data.GRQueues = append(data.GRQueues, qs)
				isClusterConfigured = true
				// If queue limit boundaries are exceeded, explicitly escalate status
				if qs.CertQueue > 25000 || qs.ApplierQueue > 25000 {
					clusterFCStatus = "Active (GR Queue Backlog Trigger)"
				}
			}
		}
		rows.Close()
	}

	// PXC / Galera Flow Control & Cluster queue checking
	var pxcFCStatus string
	_ = db.QueryRow("SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME = 'wsrep_flow_control_status'").Scan(&pxcFCStatus)
	if pxcFCStatus != "" {
		isClusterConfigured = true
		data.GaleraFlowControl = pxcFCStatus
		if pxcFCStatus != "OFF" && pxcFCStatus != "0" && pxcFCStatus != "0.000000" {
			clusterFCStatus = "Active (PXC Galera Paused State)"
		} else {
			clusterFCStatus = "Inactive (Galera Cluster Healthy)"
		}
	}

	// Detailed aggregated PXC queue check
	galeraStatsQuery := `
		SELECT
			IFNULL(MAX(CASE WHEN VARIABLE_NAME='wsrep_local_recv_queue' THEN VARIABLE_VALUE END), '0') AS recv_queue,
			IFNULL(MAX(CASE WHEN VARIABLE_NAME='wsrep_local_send_queue' THEN VARIABLE_VALUE END), '0') AS send_queue,
			IFNULL(MAX(CASE WHEN VARIABLE_NAME='wsrep_flow_control_paused' THEN VARIABLE_VALUE END), '0.000000') AS flow_control_paused
		FROM performance_schema.global_status
		WHERE VARIABLE_NAME IN ('wsrep_local_recv_queue', 'wsrep_local_send_queue', 'wsrep_flow_control_paused');`

	var gqs GaleraQueueStats
	var recvQ, sendQ, fcPaused sql.NullString
	if err := db.QueryRow(galeraStatsQuery).Scan(&recvQ, &sendQ, &fcPaused); err == nil {
		if recvQ.Valid && recvQ.String != "" {
			gqs.RecvQueue = recvQ.String
		} else {
			gqs.RecvQueue = "0"
		}
		if sendQ.Valid && sendQ.String != "" {
			gqs.SendQueue = sendQ.String
		} else {
			gqs.SendQueue = "0"
		}
		if fcPaused.Valid && fcPaused.String != "" {
			gqs.FlowControlPaused = fcPaused.String
			pf, parseErr := strconv.ParseFloat(fcPaused.String, 64)
			if parseErr == nil && pf > 0.05 {
				isClusterConfigured = true
				clusterFCStatus = fmt.Sprintf("Active (Galera Paused: %.1f%%)", pf*100.0)
			}
		} else {
			gqs.FlowControlPaused = "0.000000"
		}

		if gqs.RecvQueue != "0" || gqs.SendQueue != "0" || gqs.FlowControlPaused != "0.000000" {
			data.GaleraQueues = gqs
			isClusterConfigured = true
		}
	}

	galeraQuery := `
		SELECT VARIABLE_NAME, VARIABLE_VALUE 
		FROM performance_schema.global_status 
		WHERE VARIABLE_NAME IN (
			'wsrep_cluster_size','wsrep_cluster_status','wsrep_incoming_addresses');`
	if rows, err := db.Query(galeraQuery); err == nil {
		for rows.Next() {
			var k, v string
			if err := rows.Scan(&k, &v); err == nil {
				switch k {
				case "wsrep_cluster_size":
					data.GaleraSummary.ClusterSize = v
				case "wsrep_cluster_status":
					data.GaleraSummary.ClusterStatus = v
				case "wsrep_incoming_addresses":
					data.GaleraSummary.IncomingAddresses = v
				}
				isClusterConfigured = true
			}
		}
		rows.Close()
	}

	// wsrep_cluster_name lives in global_variables, not global_status
	clusterNameQuery := `
		SELECT VARIABLE_VALUE FROM performance_schema.global_variables
		WHERE VARIABLE_NAME = 'wsrep_cluster_name';`
	if row := db.QueryRow(clusterNameQuery); row != nil {
		var name string
		if err := row.Scan(&name); err == nil {
			data.GaleraSummary.ClusterName = name
		}
	}

	// ── NEW: wsrep_provider_options — parse into key/value table rows ────────
	// SELECT * FROM performance_schema.global_variables
	//   WHERE variable_name LIKE '%wsrep_provider_options%'
	var wsrepRaw string
	_ = db.QueryRow(`SELECT IFNULL(VARIABLE_VALUE,'') FROM performance_schema.global_variables
		WHERE VARIABLE_NAME = 'wsrep_provider_options'`).Scan(&wsrepRaw)
	if wsrepRaw != "" {
		// Parse "key = value; key = value; …" format
		parts := strings.Split(wsrepRaw, ";")
		for _, part := range parts {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			eqIdx := strings.Index(part, "=")
			if eqIdx < 0 {
				continue
			}
			k := strings.TrimSpace(part[:eqIdx])
			v := strings.TrimSpace(part[eqIdx+1:])
			data.WsrepProviderOptions = append(data.WsrepProviderOptions, WsrepProviderOption{k, v})
			// Extract key fc fields for the queue comparison alert
			switch k {
			case "gcs.fc_limit":
				data.WsrepQueueMax.FCLimit = v
			case "gcs.fc_master_slave":
				data.WsrepQueueMax.FCMasterSlave = v
			case "gcs.fc_single_primary":
				data.WsrepQueueMax.FCSinglePrimary = v
			}
		}
	}

	// ── NEW: wsrep_local_recv_queue + wsrep_local_recv_queue_max ─────────────
	// SELECT * FROM performance_schema.global_status
	//   WHERE Variable_name IN ('wsrep_local_recv_queue','wsrep_local_recv_queue_max')
	_ = db.QueryRow(`SELECT IFNULL(VARIABLE_VALUE,'0') FROM performance_schema.global_status
		WHERE VARIABLE_NAME = 'wsrep_local_recv_queue'`).Scan(&data.WsrepQueueMax.RecvQueue)
	_ = db.QueryRow(`SELECT IFNULL(VARIABLE_VALUE,'0') FROM performance_schema.global_status
		WHERE VARIABLE_NAME = 'wsrep_local_recv_queue_max'`).Scan(&data.WsrepQueueMax.RecvQueueMax)
	// Alert if recv_queue_max >= fc_limit (user's rule #1)
	if data.WsrepQueueMax.FCLimit != "" && data.WsrepQueueMax.RecvQueueMax != "" {
		fcLimitN, _ := strconv.Atoi(data.WsrepQueueMax.FCLimit)
		recvMaxN, _ := strconv.Atoi(data.WsrepQueueMax.RecvQueueMax)
		if fcLimitN > 0 && recvMaxN >= fcLimitN {
			data.WsrepQueueMax.Alert = true
		}
	}

	// ── NEW: Group Replication flow control + consistency config ─────────────
	// SELECT * FROM performance_schema.global_variables
	//   WHERE variable_name IN ('group_replication_communication_stack',
	//     'group_replication_consistency',
	//     'group_replication_flow_control_applier_threshold',
	//     'group_replication_flow_control_certifier_threshold',
	//     'group_replication_flow_control_mode',
	//     'group_replication_bootstrap_group')
	grCfgRows, grCfgErr := db.Query(`
		SELECT VARIABLE_NAME, IFNULL(VARIABLE_VALUE,'') AS VARIABLE_VALUE
		FROM performance_schema.global_variables
		WHERE VARIABLE_NAME IN (
			'group_replication_bootstrap_group',
			'group_replication_communication_stack',
			'group_replication_consistency',
			'group_replication_flow_control_applier_threshold',
			'group_replication_flow_control_certifier_threshold',
			'group_replication_flow_control_mode'
		)
		ORDER BY VARIABLE_NAME`)
	if grCfgErr == nil {
		for grCfgRows.Next() {
			var k, v string
			if grCfgRows.Scan(&k, &v) == nil {
				switch k {
				case "group_replication_bootstrap_group":
					data.GRFlowConfig.BootstrapGroup = v
				case "group_replication_communication_stack":
					data.GRFlowConfig.CommStack = v
				case "group_replication_consistency":
					data.GRFlowConfig.Consistency = v
				case "group_replication_flow_control_applier_threshold":
					data.GRFlowConfig.ApplierThreshold = v
				case "group_replication_flow_control_certifier_threshold":
					data.GRFlowConfig.CertThreshold = v
				case "group_replication_flow_control_mode":
					data.GRFlowConfig.FlowControlMode = v
				}
			}
		}
		grCfgRows.Close()
	}

	// Final Summary Cluster Flow Control assignment
	if !isClusterConfigured {
		data.Summary.ClusterFlowControl = "Not Configured / Single Instance"
	} else {
		data.Summary.ClusterFlowControl = clusterFCStatus
	}
	// #region agent log
	debugLog("D", "main.go:cluster-fc", "cluster flow control summary resolved", map[string]interface{}{
		"isClusterConfigured": isClusterConfigured,
		"clusterFCStatus":     clusterFCStatus,
		"grFCActiveSet":       data.GRFlowControlLimit != "",
		"galeraFCSet":         data.GaleraFlowControl != "",
		"grQueueCount":        len(data.GRQueues),
	})
	// #endregion

	// Cluster name from metadata registry (schema mysql_innodb_cluster_metadata only).
	// Note: there is no mysql_innodb_cluster database, and metadata tables do not
	// store Shell status() JSON — that comes from mysqlsh below.
	var clusterNameVal string
	for _, q := range []string{
		"SELECT cluster_name FROM mysql_innodb_cluster_metadata.clusters LIMIT 1",
		"SELECT cluster_name FROM mysql_innodb_cluster_metadata.v2_gr_clusters LIMIT 1",
	} {
		if err := db.QueryRow(q).Scan(&clusterNameVal); err == nil && clusterNameVal != "" {
			break
		}
	}
	if clusterNameVal == "" {
		clusterNameVal = "cluster"
	}

	// ── InnoDB Cluster & ClusterSet topology ─────────────────────────────────
	// Step 1: Check if this server is part of an InnoDB Cluster/ClusterSet (pure SQL).
	// Step 2: If yes, auto-spawn mysqlsh with the same credentials to get the rich
	//         status JSON (listRouters, routingOptions, status({extended:1})).
	//         No extra flag needed — command stays:
	//         ./mysql_gather --user=root --password=x --host=h --port=p
	var isInnoDBCluster bool
	db.QueryRow(`SELECT COUNT(*) > 0 FROM information_schema.schemata
		WHERE schema_name = 'mysql_innodb_cluster_metadata'`).Scan(&isInnoDBCluster)

	if isInnoDBCluster {
		log.Println("[InnoDB] Cluster metadata detected — attempting mysqlsh for rich topology...")
		uri := fmt.Sprintf("%s@%s:%d", *user, *host, *port)

		// Single JS call: try ClusterSet first, fall back to single Cluster.
		// All three API calls (status, listRouters, routingOptions) in one exec.
		jsCode := `
var out = { type: "none" };
try {
  var cs = dba.getClusterSet();
  out.type     = "clusterset";
  out.status   = cs.status({extended:1});
  out.routers  = cs.listRouters();
  out.routing  = cs.routingOptions();
} catch(eCS) {
  try {
    var cl = dba.getCluster();
    out.type    = "cluster";
    out.status  = cl.status({extended:1});
    out.routers = cl.listRouters();
    out.routing = cl.routingOptions();
  } catch(eCL) {
    out.type  = "none";
    out.error = eCL.message;
  }
}
print(JSON.stringify(out));
`
		raw, execErr := csRunMysqlsh("mysqlsh", uri, *password, jsCode)
		if execErr != nil {
			// mysqlsh not installed or unreachable — fall back to pure SQL silently
			log.Printf("[InnoDB] mysqlsh unavailable (%v) — using SQL fallback", execErr)
			data.ClusterSetTopology, data.SingleClusterTopo = collectInnoDBTopologySQL(db)
			// #region agent log
			debugLog("E", "main.go:mysqlsh-fallback", "SQL fallback after mysqlsh failure", map[string]interface{}{
				"execErr":               execErr.Error(),
				"gotClusterSetTopology": data.ClusterSetTopology != nil,
				"gotSingleClusterTopo":  data.SingleClusterTopo != nil,
			})
			// #endregion
			data.CSRoutingOptions = collectRoutingOptionsSQL(db)
		} else if len(raw) > 2 {
			var wrapper struct {
				Type    string          `json:"type"`
				Error   string          `json:"error"`
				Status  json.RawMessage `json:"status"`
				Routers json.RawMessage `json:"routers"`
				Routing json.RawMessage `json:"routing"`
			}
			if jsonErr := json.Unmarshal(raw, &wrapper); jsonErr != nil {
				log.Printf("[InnoDB] JSON parse error: %v — using SQL fallback", jsonErr)
				data.ClusterSetTopology, data.SingleClusterTopo = collectInnoDBTopologySQL(db)
			} else {
				switch wrapper.Type {
				case "clusterset":
					data.ClusterSetStatus = prettyJSONRaw(wrapper.Status)
					if topo, pErr := parseClusterSetJSON(wrapper.Status); pErr == nil {
						data.ClusterSetTopology = topo
						log.Printf("[InnoDB] ClusterSet: domain=%s primary=%s status=%s",
							topo.DomainName, topo.PrimaryCluster, topo.Status)
					}
				case "cluster":
					data.ClusterStatus = prettyJSONRaw(wrapper.Status)
					if topo, pErr := parseClusterSetJSON(wrapper.Status); pErr == nil {
						data.SingleClusterTopo = topo
						log.Printf("[InnoDB] Single cluster: primary=%s status=%s",
							topo.GlobalPrimaryInstance, topo.Status)
					}
				default:
					// Neither ClusterSet nor Cluster via mysqlsh — use SQL
					data.ClusterSetTopology, data.SingleClusterTopo = collectInnoDBTopologySQL(db)
				}
				// Routers from mysqlsh
				if len(wrapper.Routers) > 2 {
					if routers, rErr := parseRouterList(wrapper.Routers); rErr == nil {
						data.CSRouters = routers
					}
				}
				// Routing options from mysqlsh
				if len(wrapper.Routing) > 2 {
					if opts, oErr := parseRoutingOptions(wrapper.Routing); oErr == nil {
						data.CSRoutingOptions = opts
					}
				}
			}
		}
	}

	// Merge router sources into one unified CSRouters list.
	// Priority: mysqlsh listRouters() is richer (has TargetCluster).
	// SQL metadata fills in anything mysqlsh didn't provide.
	// Deduplicate by RouterKey / router name so nothing appears twice.
	sqlRouters := queryRouterMetadata(db)
	if len(data.CSRouters) == 0 {
		// mysqlsh gave nothing — convert SQL rows to CSRouter
		for _, r := range sqlRouters {
			data.CSRouters = append(data.CSRouters, CSRouter{
				RouterKey:   r.RouterName,
				Hostname:    r.Address,
				Version:     r.Version,
				LastCheckIn: r.LastCheckIn,
				RWPort:      r.RWPort,
				ROPort:      r.ROPort,
				RWXPort:     r.RWXPort,
				ROXPort:     r.ROXPort,
			})
		}
	} else {
		// mysqlsh gave data — backfill MetadataUser from SQL where missing
		sqlByName := map[string]RouterDetails{}
		for _, r := range sqlRouters {
			sqlByName[r.RouterName] = r
		}
		for i, cr := range data.CSRouters {
			// Router1 key from mysqlsh is "hostname::Router1"; extract the name part
			namePart := cr.RouterKey
			if idx := strings.LastIndex(namePart, "::"); idx >= 0 {
				namePart = namePart[idx+2:]
			}
			if sqlR, ok := sqlByName[namePart]; ok {
				if data.CSRouters[i].RWSplitPort == "" {
					data.CSRouters[i].RWSplitPort = sqlR.RWSplitPort
				}
			}
		}
	}
	// Deduplicate: if mysqlsh returned both "hostname::" (blank name) and
	// "hostname::RouterName", keep only the named entry.
	seen := map[string]bool{}
	var deduped []CSRouter
	for _, r := range data.CSRouters {
		key := r.RouterKey
		if strings.HasSuffix(key, "::") {
			// blank router name — skip if a named version exists
			prefix := strings.TrimSuffix(key, "::")
			hasBetter := false
			for _, other := range data.CSRouters {
				if other.RouterKey != key && strings.HasPrefix(other.RouterKey, prefix+"::") {
					hasBetter = true
					break
				}
			}
			if hasBetter {
				continue
			}
		}
		if !seen[key] {
			seen[key] = true
			deduped = append(deduped, r)
		}
	}
	data.CSRouters = deduped
	data.Routers = nil // clear SQL list — CSRouters is now the single source

	// Routing options
	if data.CSRoutingOptions == nil {
		data.CSRoutingOptions = collectRoutingOptionsSQL(db)
	}

	// #region agent log
	innoDBOuterGate := data.ClusterStatus != "" || data.ClusterSetStatus != "" || data.ClusterSetTopology != nil || len(data.Routers) > 0
	singleTopoNodes := 0
	if data.SingleClusterTopo != nil && data.SingleClusterTopo.PrimaryClusterData != nil {
		singleTopoNodes = len(data.SingleClusterTopo.PrimaryClusterData.Nodes)
	}
	csTopoNodes := 0
	if data.ClusterSetTopology != nil && data.ClusterSetTopology.PrimaryClusterData != nil {
		csTopoNodes = len(data.ClusterSetTopology.PrimaryClusterData.Nodes)
	}
	debugLog("A", "main.go:innoDB-gate", "InnoDB Cluster HTML outer gate evaluation", map[string]interface{}{
		"outerGateWouldOpen":    innoDBOuterGate,
		"hasClusterStatus":      data.ClusterStatus != "",
		"hasClusterSetStatus":   data.ClusterSetStatus != "",
		"hasClusterSetTopology": data.ClusterSetTopology != nil,
		"hasSingleClusterTopo":  data.SingleClusterTopo != nil,
		"singleTopoNodes":       singleTopoNodes,
		"csTopoNodes":           csTopoNodes,
		"routersLen":            len(data.Routers),
		"hiddenSingleTopoBug":   data.SingleClusterTopo != nil && !innoDBOuterGate,
	})
	debugLog("B", "main.go:router-gate", "MySQL Router HTML gate evaluation", map[string]interface{}{
		"csRoutersCount":      len(data.CSRouters),
		"hasCSRoutingOptions": data.CSRoutingOptions != nil,
		"routerInnerGateOpen": len(data.CSRouters) > 0 || data.CSRoutingOptions != nil,
		"hiddenRouterBug":     (len(data.CSRouters) > 0 || data.CSRoutingOptions != nil) && !innoDBOuterGate,
	})
	// #endregion

	// Section 6: Current Binary Log Status
	// This section gathers the current master/binlog status, then computes the
	// total number and total size of all binary log files. It also captures
	// recent events from the current active binary log file for diagnostics.
	binlogRows, bErr := db.Query("SHOW BINARY LOG STATUS;")
	if bErr != nil {
		binlogRows, bErr = db.Query("SHOW MASTER STATUS;")
	}
	currentBinlogFile := ""
	if bErr == nil {
		cols, _ := binlogRows.Columns()
		if binlogRows.Next() {
			values := make([]sql.RawBytes, len(cols))
			scanArgs := make([]interface{}, len(cols))
			for i := range values {
				scanArgs[i] = &values[i]
			}
			if err := binlogRows.Scan(scanArgs...); err == nil {
				for i, col := range cols {
					val := string(values[i])
					data.MasterStatus = append(data.MasterStatus, KeyVal{Key: col, Value: val})
					if strings.EqualFold(col, "File") || strings.EqualFold(col, "Log_name") {
						currentBinlogFile = val
					}
				}
			}
		}
		binlogRows.Close()
	}

	if currentBinlogFile == "" {
		for _, kv := range data.MasterStatus {
			if strings.EqualFold(kv.Key, "File") || strings.EqualFold(kv.Key, "Log_name") {
				currentBinlogFile = kv.Value
				break
			}
		}
	}
	// Preserve the active binary log file name for the HTML report and later event lookup.
	data.CurrentBinlogFile = currentBinlogFile

	// Count all available binary log files and accumulate their byte sizes.
	if logsRows, err := db.Query("SHOW BINARY LOGS;"); err == nil {
		defer logsRows.Close()
		for logsRows.Next() {
			var logName string
			var fileSize sql.NullInt64
			var encrypted sql.NullString
			if err := logsRows.Scan(&logName, &fileSize, &encrypted); err == nil {
				data.BinaryLogCount++
				if fileSize.Valid {
					data.BinaryLogSize += fileSize.Int64
				}
			}
		}
		data.BinaryLogSizeHuman = formatBytes(strconv.FormatInt(data.BinaryLogSize, 10))
	}

	if currentBinlogFile != "" {
		// Capture the most recent events from the current active log file.
		query := fmt.Sprintf("SHOW BINLOG EVENTS IN '%s' LIMIT 20;", currentBinlogFile)
		if eventsRows, err := db.Query(query); err == nil {
			defer eventsRows.Close()
			for eventsRows.Next() {
				var event BinlogEvent
				var serverID sql.NullString
				var info sql.NullString
				if err := eventsRows.Scan(&event.LogName, &event.Pos, &event.EventType, &serverID, &event.EndLogPos, &info); err == nil {
					if serverID.Valid {
						event.ServerID = serverID.String
					}
					if info.Valid {
						event.Info = info.String
					}
					data.CurrentBinlogEvents = append(data.CurrentBinlogEvents, event)
				}
			}
		}
	}

	// Section 7: Process List - SHOW FULL PROCESSLIST details
	if rows, err := db.Query("SHOW FULL PROCESSLIST;"); err == nil {
		for rows.Next() {
			var p ProcessInfo
			var dbVal sql.NullString
			var infoVal sql.NullString
			var stateVal sql.NullString
			if err := rows.Scan(&p.ID, &p.User, &p.Host, &dbVal, &p.Command, &p.Time, &stateVal, &infoVal); err == nil {
				if dbVal.Valid {
					p.DB = dbVal.String
				} else {
					p.DB = "NULL"
				}
				if stateVal.Valid {
					p.State = stateVal.String
				} else {
					p.State = ""
				}
				if infoVal.Valid {
					p.Info = infoVal.String
				} else {
					p.Info = "NULL"
				}
				data.Processes = append(data.Processes, p)
			}
		}
		rows.Close()
	}

	// Section 7 (Continued): History TOP query stats from performance_schema
	historyQuery := `
		SELECT
			IFNULL(DIGEST, 'NULL') AS DIGEST,
			IFNULL(SCHEMA_NAME, 'NULL') AS SCHEMA_NAME,
			LEFT(DIGEST_TEXT, 120) AS query_sample,
			COUNT_STAR AS exec_count,
			ROUND(SUM_TIMER_WAIT/1000000000000, 2) AS total_exec_sec,
			ROUND(AVG_TIMER_WAIT/1000000000, 2) AS avg_exec_ms,
			ROUND(SUM_LOCK_TIME/1000000000000, 2) AS total_lock_sec,
			SUM_ROWS_EXAMINED,
			SUM_ROWS_SENT,
			SUM_CREATED_TMP_TABLES,
			SUM_CREATED_TMP_DISK_TABLES,
			SUM_SORT_ROWS,
			SUM_NO_INDEX_USED,
			SUM_NO_GOOD_INDEX_USED
		FROM performance_schema.events_statements_summary_by_digest
		ORDER BY SUM_TIMER_WAIT DESC
		LIMIT 10;`

	if rows, err := db.Query(historyQuery); err == nil {
		for rows.Next() {
			var d DigestStat
			if err := rows.Scan(
				&d.Digest, &d.SchemaName, &d.QuerySample, &d.ExecCount,
				&d.TotalExecSec, &d.AvgExecMs, &d.TotalLockSec, &d.RowsExamined,
				&d.RowsSent, &d.CreatedTmpTables, &d.CreatedTmpDiskTables,
				&d.SortRows, &d.NoIndexUsed, &d.NoGoodIndexUsed,
			); err == nil {
				data.DigestStats = append(data.DigestStats, d)
			}
		}
		rows.Close()
	}

	// Section 7 (Continued): Locking and Waiting stats from sys.innodb_lock_waits
	lockWaitsQuery := `
		SELECT 
			IFNULL(wait_started, 'NULL') AS wait_started,
			IFNULL(wait_age_secs, 0) AS wait_age_secs,
			IFNULL(locked_table, 'NULL') AS locked_table,
			IFNULL(locked_index, 'NULL') AS locked_index,
			IFNULL(locked_type, 'NULL') AS locked_type,
			IFNULL(waiting_pid, 0) AS waiting_pid,
			IFNULL(waiting_trx_id, 'NULL') AS waiting_trx_id,
			IFNULL(waiting_lock_mode, 'NULL') AS waiting_lock_mode,
			IFNULL(waiting_query, 'NULL') AS waiting_query,
			IFNULL(blocking_pid, 0) AS blocking_pid,
			IFNULL(blocking_trx_id, 'NULL') AS blocking_trx_id,
			IFNULL(blocking_lock_mode, 'NULL') AS blocking_lock_mode,
			IFNULL(blocking_query, 'NULL') AS blocking_query,
			IFNULL(sql_kill_blocking_connection, 'NULL') AS sql_kill_blocking_connection
		FROM sys.innodb_lock_waits 
		ORDER BY wait_age_secs DESC;`

	if rows, err := db.Query(lockWaitsQuery); err == nil {
		for rows.Next() {
			var l LockWait
			if err := rows.Scan(
				&l.WaitStarted, &l.WaitAgeSecs, &l.LockedTable, &l.LockedIndex, &l.LockedType,
				&l.WaitingPid, &l.WaitingTrxId, &l.WaitingLockMode, &l.WaitingQuery,
				&l.BlockingPid, &l.BlockingTrxId, &l.BlockingLockMode, &l.BlockingQuery,
				&l.SQLKillBlockingConnection,
			); err == nil {
				data.LockWaits = append(data.LockWaits, l)
			}
		}
		rows.Close()
	}

	// Section 7 (Continued): DDL & Schema lock waits (sys.schema_table_lock_waits + events_statements_history JOIN)
	var ddlTableExists int
	_ = db.QueryRow(`
		SELECT COUNT(*) 
		FROM information_schema.tables 
		WHERE TABLE_SCHEMA = 'sys' AND TABLE_NAME = 'schema_table_lock_waits'
	`).Scan(&ddlTableExists)

	if ddlTableExists > 0 {
		// 1. Fetch raw lock waits details (such as waiting vs blocking coordinates)
		rawLockQuery := `
			SELECT 
				IFNULL(object_schema, ''), 
				IFNULL(object_name, ''), 
				IFNULL(waiting_pid, 0), 
				IFNULL(waiting_lock_type, ''), 
				IFNULL(waiting_query, ''), 
				IFNULL(waiting_query_secs, 0), 
				IFNULL(blocking_pid, 0), 
				IFNULL(blocking_lock_type, ''), 
				IFNULL(sql_kill_blocking_connection, '') 
			FROM sys.schema_table_lock_waits;`
		if rows, err := db.Query(rawLockQuery); err == nil {
			for rows.Next() {
				var s SchemaTableLockWait
				if err := rows.Scan(&s.ObjectSchema, &s.ObjectName, &s.WaitingPid, &s.WaitingLockType, &s.WaitingQuery, &s.WaitingQuerySecs, &s.BlockingPid, &s.BlockingLockType, &s.SQLKillBlockingConnection); err == nil {
					data.SchemaTableLockWaits = append(data.SchemaTableLockWaits, s)
				}
			}
			rows.Close()
		}

		// 2. Fetch blocking statement execution query history
		ddlQuery := `
			SELECT 
				stlw.blocking_pid, 
				IFNULL(stlw.object_schema, ''), 
				IFNULL(stlw.object_name, ''), 
				stlw.blocking_thread_id, 
				IFNULL(GROUP_CONCAT(esh.sql_text SEPARATOR '\n'), '') AS sql_query 
			FROM sys.schema_table_lock_waits AS stlw 
			LEFT JOIN performance_schema.events_statements_history AS esh 
				ON stlw.blocking_thread_id = esh.THREAD_ID 
			GROUP BY stlw.blocking_pid, stlw.object_schema, stlw.object_name, stlw.blocking_thread_id;`

		if rows, err := db.Query(ddlQuery); err == nil {
			for rows.Next() {
				var d DDLLock
				if err := rows.Scan(&d.BlockingPid, &d.ObjectSchema, &d.ObjectName, &d.BlockingThreadID, &d.SQLQuery); err == nil {
					data.DDLLocks = append(data.DDLLocks, d)
				}
			}
			rows.Close()
		}
	}

	// Section 7 (Continued): Active Transactions from information_schema.innodb_trx
	trxQuery := `
		SELECT
			trx_id,
			trx_state,
			trx_started,
			IFNULL(trx_wait_started, '') AS trx_wait_started,
			trx_weight,
			trx_mysql_thread_id,
			IFNULL(LEFT(trx_query, 120), '') AS trx_query,
			IFNULL(trx_operation_state, '') AS trx_operation_state,
			trx_tables_in_use,
			trx_tables_locked,
			trx_lock_structs,
			trx_rows_locked,
			trx_rows_modified,
			trx_isolation_level
		FROM information_schema.innodb_trx
		ORDER BY trx_started ASC;`
	if rows, err := db.Query(trxQuery); err == nil {
		for rows.Next() {
			var t InnodbTrx
			if err := rows.Scan(
				&t.TrxID, &t.TrxState, &t.TrxStarted, &t.TrxWaitStarted,
				&t.TrxWeight, &t.TrxMysqlThreadID, &t.TrxQuery, &t.TrxOperation,
				&t.TrxTablesInUse, &t.TrxTablesLocked, &t.TrxLockStructs,
				&t.TrxRowsLocked, &t.TrxRowsModified, &t.TrxIsolationLevel,
			); err == nil {
				data.InnodbTrx = append(data.InnodbTrx, t)
			}
		}
		rows.Close()
	}

	// Section 8 (Continued): Performance Schema Threads
	pfsThreadQuery := `
		SELECT
			THREAD_ID,
			NAME,
			TYPE,
			IFNULL(PROCESSLIST_ID, '') AS PROCESSLIST_ID,
			IFNULL(PROCESSLIST_USER, '') AS PROCESSLIST_USER,
			IFNULL(PROCESSLIST_HOST, '') AS PROCESSLIST_HOST,
			IFNULL(PROCESSLIST_DB, '') AS PROCESSLIST_DB,
			IFNULL(PROCESSLIST_COMMAND, '') AS PROCESSLIST_COMMAND,
			IFNULL(PROCESSLIST_TIME, '') AS PROCESSLIST_TIME,
			IFNULL(PROCESSLIST_STATE, '') AS PROCESSLIST_STATE,
			IFNULL(LEFT(PROCESSLIST_INFO, 100), '') AS PROCESSLIST_INFO
		FROM performance_schema.threads
		ORDER BY TYPE, THREAD_ID;`
	if rows, err := db.Query(pfsThreadQuery); err == nil {
		for rows.Next() {
			var t PfsThread
			if err := rows.Scan(
				&t.ThreadID, &t.Name, &t.Type,
				&t.ProcesslistID, &t.ProcesslistUser, &t.ProcesslistHost,
				&t.ProcesslistDB, &t.ProcesslistCommand, &t.ProcesslistTime,
				&t.ProcesslistState, &t.ProcesslistInfo,
			); err == nil {
				data.PfsThreads = append(data.PfsThreads, t)
			}
		}
		rows.Close()
	}

	// User Details from mysql.user
	userQuery := `SELECT User, Host, plugin, password_expired FROM mysql.user ORDER BY User, Host;`
	if rows, err := db.Query(userQuery); err == nil {
		for rows.Next() {
			var u UserDetail
			if err := rows.Scan(&u.User, &u.Host, &u.Plugin, &u.PasswordExpired); err == nil {
				data.UserDetails = append(data.UserDetails, u)
			}
		}
		rows.Close()
	}

	// Section 8: Performance Schema profiling details
	// 8a. Global memory event allocations
	memQuery := "SELECT event_name, current_alloc FROM sys.memory_global_by_current_bytes LIMIT 10;"
	if rows, err := db.Query(memQuery); err == nil {
		for rows.Next() {
			var m MemoryEvent
			if err := rows.Scan(&m.EventName, &m.CurrentAlloc); err == nil {
				m.CurrentAlloc = formatBytes(m.CurrentAlloc)
				data.MemoryEvents = append(data.MemoryEvents, m)
			}
		}
		rows.Close()
	}

	// 8b. Global Wait thread event summary
	waitsQuery := `
		SELECT 
			EVENT_NAME AS wait_event, 
			COUNT_STAR AS occurrence_count, 
			ROUND(SUM_TIMER_WAIT / 1000000000000, 4) AS total_wait_sec, 
			ROUND(AVG_TIMER_WAIT / 1000000000, 4) AS avg_wait_ms
		FROM performance_schema.events_waits_summary_global_by_event_name
		WHERE COUNT_STAR > 0 
		AND EVENT_NAME NOT LIKE '%idle%'
		ORDER BY SUM_TIMER_WAIT DESC 
		LIMIT 10;`
	if rows, err := db.Query(waitsQuery); err == nil {
		for rows.Next() {
			var w WaitEvent
			if err := rows.Scan(&w.EventName, &w.CountStar, &w.TotalWaitSec, &w.AvgWaitMs); err == nil {
				data.WaitEvents = append(data.WaitEvents, w)
			}
		}
		rows.Close()
	}

	// 8c. Physical File IO profile
	ioQuery := `
		SELECT 
			EVENT_NAME AS io_event,
			COUNT_READ AS total_reads,
			COUNT_WRITE AS total_writes,
			ROUND(SUM_NUMBER_OF_BYTES_READ / 1024 / 1024, 2) AS mb_read,
			ROUND(SUM_NUMBER_OF_BYTES_WRITE / 1024 / 1024, 2) AS mb_written,
			ROUND(SUM_TIMER_READ / 1000000000000, 2) AS read_latency_sec,
			ROUND(SUM_TIMER_WRITE / 1000000000000, 2) AS write_latency_sec
		FROM performance_schema.file_summary_by_event_name
		WHERE COUNT_READ > 0 OR COUNT_WRITE > 0
		ORDER BY (SUM_TIMER_READ + SUM_TIMER_WRITE) DESC
		LIMIT 10;`
	if rows, err := db.Query(ioQuery); err == nil {
		for rows.Next() {
			var f FileIOEvent
			if err := rows.Scan(&f.EventName, &f.CountRead, &f.CountWrite, &f.MBRead, &f.MBWritten, &f.ReadLatency, &f.WriteLatency); err == nil {
				data.FileIOEvents = append(data.FileIOEvents, f)
			}
		}
		rows.Close()
	}

	// Section 10: Optimization Recommendations (Implementing status_variables_mapping.md rules)
	data.Recommendations = make([]Recommendation, 0)

	// Rule 1: Thread Pool Cache Miss Rate
	threadsCreatedStr := statusMap["threads_created"]
	connectionsStr := statusMap["connections"]
	threadCacheSizeStr := variablesMap["thread_cache_size"]
	if threadsCreatedStr != "" && connectionsStr != "" {
		tCreated := getRawInt(threadsCreatedStr)
		conns := getRawInt(connectionsStr)
		if conns > 100 {
			missRate := (float64(tCreated) / float64(conns)) * 100.0
			if missRate > 10.0 {
				data.Recommendations = append(data.Recommendations, Recommendation{
					Type:        "WARNING",
					Parameter:   "thread_cache_size (Current: " + threadCacheSizeStr + ")",
					Description: fmt.Sprintf("High Thread Cache Miss Rate detected (%.2f%%). Spawning raw OS threads under high connection activity causes excessive CPU context-switching. Increase your thread_cache_size parameter progressively (e.g. to 32, 64, or 128) to cache connection streams.", missRate),
				})
			}
		}
	}

	// Rule 2: Memory Sorts vs Disk-Backed Temporary Tables (Disk Overflow Ratio)
	tmpTablesStr := statusMap["created_tmp_tables"]
	tmpDiskTablesStr := statusMap["created_tmp_disk_tables"]
	if tmpTablesStr != "" && tmpDiskTablesStr != "" {
		tTables := getRawInt(tmpTablesStr)
		tDiskTables := getRawInt(tmpDiskTablesStr)
		if tTables > 0 {
			diskRatio := (float64(tDiskTables) / float64(tTables)) * 100.0
			if diskRatio > 25.0 {
				data.Recommendations = append(data.Recommendations, Recommendation{
					Type:        "WARNING",
					Parameter:   "tmp_table_size & max_heap_table_size",
					Description: fmt.Sprintf("High Disk Temporary Table Ratio (%.2f%% of %d total temp tables). Over 25%% of internal GROUP BY or DISTINCT queries are spilling from memory to disk. Increase both tmp_table_size and max_heap_table_size variables in tandem to 32M or 64M to avoid slow disk I/O.", diskRatio, tTables),
				})
			}
		}
	}

	// Rule 3: Sort Buffer Capacity
	sortMergePassesStr := statusMap["sort_merge_passes"]
	if sortMergePassesStr != "" && uptimeSeconds > 0 {
		merges := getRawInt(sortMergePassesStr)
		mergeRate := float64(merges) / float64(uptimeSeconds)
		if mergeRate > 1.0 {
			data.Recommendations = append(data.Recommendations, Recommendation{
				Type:        "WARNING",
				Parameter:   "sort_buffer_size",
				Description: fmt.Sprintf("Active sort merge pass rate is steadily rising (%.3f merges/sec). Large queries are currently splitting filesort passes into temporary files on disk. Increase sort_buffer_size moderately to 1M or 2M to optimize.", mergeRate),
			})
		}
	}

	// Rule 4: InnoDB Buffer Pool Hit Ratio
	poolReadReqStr := statusMap["innodb_buffer_pool_read_requests"]
	poolReadsStr := statusMap["innodb_buffer_pool_reads"]
	if poolReadReqStr != "" && poolReadsStr != "" {
		reads := float64(getRawInt(poolReadsStr))
		requests := float64(getRawInt(poolReadReqStr))
		if requests > 0 {
			hitRatio := (1.0 - (reads / requests)) * 100.0
			if hitRatio < 98.0 {
				data.Recommendations = append(data.Recommendations, Recommendation{
					Type:        "CRITICAL",
					Parameter:   "innodb_buffer_pool_size",
					Description: fmt.Sprintf("Low InnoDB Buffer Pool Cache Hit Ratio (%.2f%%). The cache cannot contain your active dataset and is bypassing memory to pull blocks from disk. Allocate additional RAM to innodb_buffer_pool_size (target 70-80%% of dedicated system memory).", hitRatio),
				})
			}
		}
	}

	// Rule 5: Redo Log Flush Capacity (Hourly Redo Volume)
	osLogWrittenStr := statusMap["innodb_os_log_written"]
	logFileSizeStr := variablesMap["innodb_log_file_size"]
	logFilesGroupStr := variablesMap["innodb_log_files_in_group"]
	if osLogWrittenStr != "" && logFileSizeStr != "" && uptimeSeconds > 0 {
		written := float64(getRawInt(osLogWrittenStr))
		hourlyRedoBytes := (written / float64(uptimeSeconds)) * 3600.0

		logSize := float64(getRawInt(logFileSizeStr))
		logGroup := float64(getRawInt(logFilesGroupStr))
		if logGroup == 0 {
			logGroup = 2.0
		}
		totalCapacity := logSize * logGroup

		if hourlyRedoBytes > totalCapacity {
			data.Recommendations = append(data.Recommendations, Recommendation{
				Type:        "WARNING",
				Parameter:   "innodb_log_file_size",
				Description: fmt.Sprintf("Estimated Hourly Write Redo volume is %s, exceeding your total log file capacity of %s. Logs are rotating too frequently, forcing heavy flush checkpoints. Increase innodb_log_file_size.", formatBytes(strconv.FormatFloat(hourlyRedoBytes, 'f', 0, 64)), formatBytes(strconv.FormatFloat(totalCapacity, 'f', 0, 64))),
			})
		}
	}

	// Rule 6: Table Open Cache Capacity (Opened Tables Miss Rate)
	openedTablesStr := statusMap["opened_tables"]
	openTablesStr := statusMap["open_tables"]
	openCacheStr := variablesMap["table_open_cache"]
	if openedTablesStr != "" && openTablesStr != "" && openCacheStr != "" && uptimeSeconds > 0 {
		opened := float64(getRawInt(openedTablesStr))
		openCur := getRawInt(openTablesStr)
		cacheCap := getRawInt(openCacheStr)

		missRate := opened / float64(uptimeSeconds)
		if missRate > 1.0 && openCur >= cacheCap {
			data.Recommendations = append(data.Recommendations, Recommendation{
				Type:        "WARNING",
				Parameter:   "table_open_cache (Current Limit: " + openCacheStr + ")",
				Description: fmt.Sprintf("High open table cache miss rate (%.2f tables opened/sec) with saturated descriptors file pool (%d open / %d cache capacity). Descriptors are constantly being evicted. Increase table_open_cache.", missRate, openCur, cacheCap),
			})
		}
	}

	// Rule 7: Connection Pool Saturation
	maxUsedConnectionsStr := statusMap["max_used_connections"]
	maxConnectionsStr := variablesMap["max_connections"]
	if maxUsedConnectionsStr != "" && maxConnectionsStr != "" {
		maxUsed := getRawInt(maxUsedConnectionsStr)
		maxAllowed := getRawInt(maxConnectionsStr)
		if maxAllowed > 0 {
			saturation := (float64(maxUsed) / float64(maxAllowed)) * 100.0
			if saturation > 85.0 {
				data.Recommendations = append(data.Recommendations, Recommendation{
					Type:        "CRITICAL",
					Parameter:   "max_connections",
					Description: fmt.Sprintf("High Connection Pool Saturation (%.2f%% utilized). Peak concurrent user threads reached %d of %d maximum connections. Increase max_connections to avoid immediate connection timeout failures.", saturation, maxUsed, maxAllowed),
				})
			}
		}
	}

	// Rule 8: Group Replication Queue & Flow Control Warning
	for _, q := range data.GRQueues {
		certThreshold := 25000
		applierThreshold := 25000
		if certLimitStr, exists := variablesMap["group_replication_flow_control_certifier_threshold"]; exists {
			certThreshold = getRawInt(certLimitStr)
		}
		if applierLimitStr, exists := variablesMap["group_replication_flow_control_applier_threshold"]; exists {
			applierThreshold = getRawInt(applierLimitStr)
		}

		if q.CertQueue > certThreshold {
			data.Recommendations = append(data.Recommendations, Recommendation{
				Type:        "CRITICAL",
				Parameter:   "Group Replication Flow Control (Certifier Queue)",
				Description: fmt.Sprintf("Certifier transaction queue size (%d) exceeds flow control threshold limit (%d) on node %s. Flow control triggers are active, throttling primary write rates.", q.CertQueue, certThreshold, q.MemberID),
			})
		}
		if q.ApplierQueue > applierThreshold {
			data.Recommendations = append(data.Recommendations, Recommendation{
				Type:        "CRITICAL",
				Parameter:   "Group Replication Flow Control (Applier Queue)",
				Description: fmt.Sprintf("Applier queue size (%d) exceeds flow control threshold limit (%d) on node %s. Flow control triggers are active, throttling primary write rates.", q.ApplierQueue, applierThreshold, q.MemberID),
			})
		}
	}

	// Rule 9: Galera / PXC Flow Control Warnings
	if data.GaleraFlowControl != "" {
		fcStatus, _ := strconv.ParseFloat(data.GaleraFlowControl, 64)
		if fcStatus > 0.1 {
			data.Recommendations = append(data.Recommendations, Recommendation{
				Type:        "WARNING",
				Parameter:   "wsrep_flow_control_status",
				Description: fmt.Sprintf("PXC/Galera Cluster Flow Control is active (%.2f%% of time spent paused). Secondary nodes are falling behind, causing replication write stalls.", fcStatus*100.0),
			})
		}
	}

	// Rule 10: PXC — wsrep_local_recv_queue_max vs gcs.fc_limit comparison
	// If recv_queue_max >= fc_limit, the node has already hit the flow-control ceiling.
	// Also recommend enabling gcs.fc_master_slave / gcs.fc_single_primary when writes
	// are only performed on a single PXC node.
	if data.WsrepQueueMax.FCLimit != "" {
		fcLimitN, _ := strconv.Atoi(strings.TrimSpace(data.WsrepQueueMax.FCLimit))
		recvMaxN, _ := strconv.Atoi(strings.TrimSpace(data.WsrepQueueMax.RecvQueueMax))
		recvCurN, _ := strconv.Atoi(strings.TrimSpace(data.WsrepQueueMax.RecvQueue))

		if fcLimitN > 0 && recvMaxN >= fcLimitN {
			data.Recommendations = append(data.Recommendations, Recommendation{
				Type:      "CRITICAL",
				Parameter: "gcs.fc_limit (wsrep_provider_options)",
				Description: fmt.Sprintf(
					"wsrep_local_recv_queue_max (%d) has reached or exceeded gcs.fc_limit (%d). "+
						"This means the receive queue has already triggered flow control at its current ceiling. "+
						"Increase gcs.fc_limit in wsrep_provider_options (e.g. SET GLOBAL wsrep_provider_options='gcs.fc_limit=200') "+
						"to give the cluster more headroom before throttling writes.",
					recvMaxN, fcLimitN),
			})
		} else if fcLimitN > 0 && recvCurN > 0 && float64(recvCurN)/float64(fcLimitN) > 0.75 {
			data.Recommendations = append(data.Recommendations, Recommendation{
				Type:      "WARNING",
				Parameter: "gcs.fc_limit (wsrep_provider_options)",
				Description: fmt.Sprintf(
					"wsrep_local_recv_queue (%d) is above 75%% of gcs.fc_limit (%d). "+
						"Flow control may activate soon if applier lag increases further. "+
						"Monitor closely and consider increasing gcs.fc_limit.",
					recvCurN, fcLimitN),
			})
		}

		// Recommend fc_master_slave / fc_single_primary if not already enabled
		// and writes are on a single node (standard PXC deployment)
		fcMS := strings.ToLower(strings.TrimSpace(data.WsrepQueueMax.FCMasterSlave))
		fcSP := strings.ToLower(strings.TrimSpace(data.WsrepQueueMax.FCSinglePrimary))
		if (fcMS == "no" || fcMS == "") && (fcSP == "no" || fcSP == "") {
			data.Recommendations = append(data.Recommendations, Recommendation{
				Type:      "INFO",
				Parameter: "gcs.fc_master_slave / gcs.fc_single_primary (wsrep_provider_options)",
				Description: "Both gcs.fc_master_slave and gcs.fc_single_primary are currently set to 'no'. " +
					"If your application performs writes exclusively on one PXC node (single-writer topology), " +
					"enabling either of these options disables flow control on the writer node and significantly " +
					"reduces unnecessary replication stalls. " +
					"Set via: SET GLOBAL wsrep_provider_options='gcs.fc_single_primary=yes'; " +
					"(or gcs.fc_master_slave=yes for older Galera versions).",
			})
		}
	}

	// Rule 11: Group Replication — flag non-default flow control mode or low thresholds
	if data.GRFlowConfig.FlowControlMode != "" {
		if data.GRFlowConfig.FlowControlMode == "DISABLED" {
			data.Recommendations = append(data.Recommendations, Recommendation{
				Type:      "WARNING",
				Parameter: "group_replication_flow_control_mode",
				Description: "Group Replication flow control is DISABLED. Without flow control, a slow secondary " +
					"node will fall arbitrarily far behind the primary, risking data inconsistency under heavy write load. " +
					"Unless deliberately tuned for high-throughput benchmarking, set flow_control_mode=QUOTA.",
			})
		}

		applierT, _ := strconv.Atoi(strings.TrimSpace(data.GRFlowConfig.ApplierThreshold))
		certT, _ := strconv.Atoi(strings.TrimSpace(data.GRFlowConfig.CertThreshold))
		if applierT > 0 && certT > 0 {
			if applierT > 100000 || certT > 100000 {
				data.Recommendations = append(data.Recommendations, Recommendation{
					Type:      "WARNING",
					Parameter: "group_replication_flow_control_applier/certifier_threshold",
					Description: fmt.Sprintf(
						"GR flow control thresholds are very high (applier=%d, certifier=%d). "+
							"These values delay flow control activation, allowing large backlogs to build before "+
							"throttling begins. Review whether these values are intentional for your workload.",
						applierT, certT),
				})
			}
		}
	}

	// Check History List Length purge metric
	for _, m := range data.EngineMetrics {
		if m.Key == "History list length" {
			hll := getRawInt(m.Value)
			if hll > 200000 {
				data.Recommendations = append(data.Recommendations, Recommendation{
					Type:        "CRITICAL",
					Parameter:   "trx_rseg_history_len",
					Description: fmt.Sprintf("History list length is extremely high (%d blocks). Your InnoDB transaction purge workers cannot clean old undo logs. Investigate and terminate long-running active transactions.", hll),
				})
			}
		}
	}

	// Rule 12: wait_timeout at default value
	waitTimeoutStr := variablesMap["wait_timeout"]
	if waitTimeoutStr != "" {
		waitTimeout := getRawInt(waitTimeoutStr)
		if waitTimeout == 28800 {
			data.Recommendations = append(data.Recommendations, Recommendation{
				Type:      "WARNING",
				Parameter: "wait_timeout (Current: " + waitTimeoutStr + " seconds)",
				Description: fmt.Sprintf("wait_timeout is set to %d seconds (%.1f hours), which is the MySQL default. "+
					"This means idle client connections are kept open for up to 8 hours before the server closes them. "+
					"In busy OLTP envivronment it can lead to resource exhaustion and eventually reaching maximum connection limit. "+
					"Consider lowering it to 60–120 seconds if your application allow to release idle connections faster and reduce resource pressure.",
					waitTimeout, float64(waitTimeout)/3600.0),
			})
		}
	}

	// Default Fallback Recommendation
	if len(data.Recommendations) == 0 {
		data.Recommendations = append(data.Recommendations, Recommendation{
			Type:        "INFO",
			Parameter:   "Baseline Optimization Analysis",
			Description: "All checked performance counters, temporary disk tables allocations, thread cache misses, and cache ratios fall within safe, healthy database limits.",
		})
	}

	// 4. Compile Standalone Diagnostic HTML Output
	outFile, err := os.Create(*output)
	if err != nil {
		log.Fatalf("Error producing report output file: %v", err)
	}
	defer outFile.Close()

	tmpl, err := template.New("report").Funcs(template.FuncMap{
		"toInt": func(s string) int {
			n, _ := strconv.Atoi(strings.TrimSpace(s))
			return n
		},
		"mod3": func(i int) int {
			return i % 3
		},
		"masterStatusValue": func(items []KeyVal, key string) string {
			for _, kv := range items {
				if strings.EqualFold(kv.Key, key) {
					return kv.Value
				}
			}
			return ""
		},
	}).Parse(htmlTemplate)
	if err != nil {
		log.Fatalf("Failed to compile layout elements: %v", err)
	}

	if err := tmpl.Execute(outFile, data); err != nil {
		log.Fatalf("Failed to execute data injection into template: %v", err)
	}

	log.Printf("MySQL Diagnostic Report built successfully at '%s'. Exiting collector...", *output)
}

// Minimal, clean HTML matching the exact structure approved in the Canvas
const htmlTemplate = `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>mysql_gather — Database Health Report</title>
    <style>
        /* ── Reset & Base ─────────────────────────────────────── */
        *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
            font-size: 12px;
            line-height: 1.5;
            color: #1a202c;
            background: #f0f4f8;
        }

        /* ── Top Header Bar ───────────────────────────────────── */
        .top-bar {
            background: linear-gradient(135deg, #1a365d 0%, #2b6cb0 100%);
            color: #fff;
            padding: 0 28px;
            display: flex;
            align-items: center;
            justify-content: space-between;
            height: 54px;
            position: sticky;
            top: 0;
            z-index: 100;
            box-shadow: 0 2px 8px rgba(0,0,0,.25);
        }
        .top-bar-brand {
            display: flex;
            align-items: center;
            gap: 10px;
        }
        .top-bar-logo {
            font-size: 22px;
            line-height: 1;
        }
        .top-bar-title {
            font-size: 16px;
            font-weight: 700;
            letter-spacing: .5px;
            font-family: Menlo, "Courier New", monospace;
        }
        .top-bar-sub {
            font-size: 11px;
            opacity: .75;
            margin-left: 4px;
            font-weight: 400;
        }
        .top-bar-meta {
            font-size: 11px;
            opacity: .85;
            text-align: right;
            line-height: 1.6;
        }

        /* ── Section Nav Pills ────────────────────────────────── */
        .sec-nav {
            background: #fff;
            border-bottom: 1px solid #cbd5e0;
            padding: 0 28px;
            display: flex;
            flex-wrap: wrap;
            gap: 2px 0;
            align-items: center;
            position: sticky;
            top: 54px;
            z-index: 99;
            box-shadow: 0 1px 4px rgba(0,0,0,.06);
        }
        .sec-nav a {
            display: inline-block;
            padding: 8px 12px;
            font-size: 11px;
            font-weight: 600;
            color: #4a5568;
            text-decoration: none;
            border-bottom: 3px solid transparent;
            white-space: nowrap;
            transition: color .15s, border-color .15s;
        }
        .sec-nav a:hover {
            color: #2b6cb0;
            border-bottom-color: #2b6cb0;
        }
        .sec-nav-sep {
            color: #cbd5e0;
            padding: 0 2px;
            font-size: 11px;
        }

        /* ── Main Content Area ────────────────────────────────── */
        .content {
            max-width: 1600px;
            margin: 0 auto;
            padding: 20px 28px 40px;
        }

        /* ── Section Card ─────────────────────────────────────── */
        .sec-card {
            background: #fff;
            border: 1px solid #e2e8f0;
            border-radius: 6px;
            margin-bottom: 14px;
            box-shadow: 0 1px 3px rgba(0,0,0,.06);
            overflow: hidden;
        }
        .sec-hdr {
            display: flex;
            align-items: center;
            justify-content: space-between;
            padding: 11px 16px;
            cursor: pointer;
            user-select: none;
            background: #f7fafc;
            border-bottom: 1px solid #e2e8f0;
            transition: background .15s;
        }
        .sec-hdr:hover { background: #edf2f7; }
        .sec-hdr-left {
            display: flex;
            align-items: center;
            gap: 10px;
        }
        .sec-num {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            width: 22px;
            height: 22px;
            background: #2b6cb0;
            color: #fff;
            border-radius: 50%;
            font-size: 10px;
            font-weight: 700;
            flex-shrink: 0;
        }
        .sec-hdr h2 {
            font-size: 13px;
            font-weight: 700;
            color: #1a365d;
            margin: 0;
            border: none;
            padding: 0;
        }
        .sec-toggle {
            font-size: 18px;
            color: #718096;
            background: none;
            border: none;
            cursor: pointer;
            padding: 0 4px;
            line-height: 1;
            transition: color .15s;
        }
        .sec-toggle:hover { color: #2b6cb0; }
        .sec-body { padding: 16px; }
        .sec-body.collapsed { display: none; }

        /* ── Typography inside sections ───────────────────────── */
        h3 {
            font-size: 12px;
            font-weight: 700;
            color: #2b6cb0;
            margin: 14px 0 6px 0;
            padding-bottom: 3px;
            border-bottom: 1px solid #e2e8f0;
        }
        h4 {
            font-size: 11px;
            font-weight: 700;
            color: #4a5568;
            margin: 10px 0 4px 0;
        }
        h5 {
            font-size: 11px;
            font-weight: 600;
            color: #718096;
            margin: 8px 0 4px 0;
        }
        p { margin: 0 0 8px 0; }
        a { color: #2b6cb0; text-decoration: none; }
        a:hover { text-decoration: underline; }
        code {
            font-family: Menlo, "Courier New", monospace;
            font-size: 10.5px;
            background: #edf2f7;
            padding: 1px 4px;
            border-radius: 3px;
        }

        /* ── Tables ───────────────────────────────────────────── */
        table {
            border-collapse: collapse;
            width: 100%;
            margin-bottom: 16px;
            font-size: 11px;
            border: 1px solid #cbd5e0;
            border-radius: 4px;
            overflow: hidden;
        }
        th, td {
            border: 1px solid #e2e8f0;
            padding: 5px 8px;
            text-align: left;
            vertical-align: top;
        }
        th {
            background: #ebf4ff;
            font-weight: 700;
            color: #1a365d;
            font-size: 10.5px;
            white-space: nowrap;
        }
        tr:nth-child(even) td { background: #f7faff; }
        tr:hover td { background: #ebf8ff !important; }

        /* ── Pre / Code blocks ────────────────────────────────── */
        pre {
            background: #f7fafc;
            border: 1px solid #e2e8f0;
            border-radius: 4px;
            padding: 10px 12px;
            font-family: Menlo, "Courier New", monospace;
            font-size: 10.5px;
            overflow-x: auto;
            white-space: pre-wrap;
            word-wrap: break-word;
            margin: 8px 0;
            line-height: 1.6;
        }

        /* ── Badges ───────────────────────────────────────────── */
        .badge {
            display: inline-block;
            padding: 2px 6px;
            font-weight: 700;
            font-size: 9px;
            border-radius: 10px;
            text-transform: uppercase;
            letter-spacing: .3px;
        }
        .badge-critical { background: #fed7d7; color: #822727; border: 1px solid #fc8181; }
        .badge-warning  { background: #fefcbf; color: #744210; border: 1px solid #f6e05e; }
        .badge-ok       { background: #c6f6d5; color: #22543d; border: 1px solid #68d391; }

        /* ── Misc ─────────────────────────────────────────────── */
        .text-right { text-align: right; }
        .search-box {
            padding: 4px 8px;
            border: 1px solid #cbd5e0;
            border-radius: 4px;
            font-size: 11px;
            font-family: inherit;
            margin-bottom: 8px;
            width: 100%;
            max-width: 280px;
            outline: none;
        }
        .search-box:focus { border-color: #2b6cb0; box-shadow: 0 0 0 2px rgba(43,108,176,.15); }
        .recommendation-card {
            border-left: 4px solid #aaa;
            padding: 8px 12px;
            margin-bottom: 10px;
            border-radius: 0 4px 4px 0;
        }
        .rec-CRITICAL { border-left-color: #e53e3e; background: #fff5f5; }
        .rec-WARNING  { border-left-color: #dd6b20; background: #fffaf0; }
        .rec-INFO     { border-left-color: #3182ce; background: #ebf8ff; }
        footer {
            text-align: center;
            margin-top: 32px;
            padding: 12px;
            font-size: 10px;
            color: #718096;
            border-top: 1px solid #e2e8f0;
        }
    </style>
</head>
<body>

    <!-- ── Top Header ── -->
    <div class="top-bar">
        <div class="top-bar-brand">
            <span class="top-bar-logo">🐬</span>
            <span class="top-bar-title">mysql_gather<span class="top-bar-sub">/ Database Health Report</span></span>
        </div>
        <div class="top-bar-meta">
            <strong>{{.Summary.Hostname}}</strong> &nbsp;·&nbsp; MySQL {{.Summary.ServerVersion}}<br>
            Uptime: {{.Summary.Uptime}} &nbsp;·&nbsp; {{.Summary.CollectedAt}}
        </div>
    </div>

    <!-- ── Section Nav ── -->
    <div class="sec-nav">
        <a href="#summary">System Summary</a><span class="sec-nav-sep">·</span>
        <a href="#config">Configuration Variables</a><span class="sec-nav-sep">·</span>
        <a href="#status">Status Variables</a><span class="sec-nav-sep">·</span>
        <a href="#innodb">InnoDB Monitor Stats</a><span class="sec-nav-sep">·</span>
        <a href="#mutexes">Mutexes / Semaphores</a><span class="sec-nav-sep">·</span>
        <a href="#replication">HA &amp; Replication</a><span class="sec-nav-sep">·</span>
        <a href="#replica-source">Binary Log Status</a><span class="sec-nav-sep">·</span>
        <a href="#process">Process List &amp; Query Locks </a><span class="sec-nav-sep">·</span>
        <a href="#perf-schema">Performance Schema Insight</a><span class="sec-nav-sep">·</span>
        <a href="#user-details">User Details</a><span class="sec-nav-sep">·</span>
        <a href="#recommendations">Recommendations</a>
    </div>

    <div class="content">

    <!-- 1. System Summary & Engine Metrics -->
    </div><!-- end sec-body -->
    <div class="sec-card">
    <div class="sec-hdr" onclick="toggleSec('summary')">
        <div class="sec-hdr-left"><span class="sec-num">1</span><h2 id="summary">System Summary</h2></div>
        <button class="sec-toggle" id="btn-summary">▲</button>
    </div>
    <div class="sec-body" id="body-summary">
    <table style="max-width: 800px; margin-bottom: 15px;">
        <tr>
            <th width="25%">Hostname</th>
            <td><strong>{{.Summary.Hostname}}</strong></td>
            <th width="20%">Collected At</th>
            <td>{{.Summary.CollectedAt}}</td>
        </tr>
        <tr>
            <th>Server Version</th>
            <td>{{.Summary.ServerVersion}}</td>
            <th>Uptime</th>
            <td>{{.Summary.Uptime}} ({{.Summary.UptimeSec}} s)</td>
        </tr>
        <tr>
            <th>Database Host</th>
            <td><code>{{.Summary.Host}}</code></td>
            <th>Database Port</th>
            <td><code>{{.Summary.Port}}</code></td>
        </tr>
        <tr>
            <th>Database User</th>
            <td><code>{{.Summary.User}}</code></td>
            <th>Default Isolation</th>
            <td>{{.Summary.TxnIsolation}}</td>
        </tr>
        <tr>
            <th>Read Only status</th>
            <td><strong>{{.Summary.ReadOnly}}</strong></td>
            <th>Cluster Flow Control Status</th>
            <td><strong style="{{if eq .Summary.ClusterFlowControl "Inactive (Healthy)"}}color: #22543d{{else if eq .Summary.ClusterFlowControl "Not Configured / Single Instance"}}color: #744210{{else}}color: #c53030{{end}}">{{.Summary.ClusterFlowControl}}</strong></td>
        </tr>
        <tr>
            <th>Threads Connected</th>
            <td>{{.Summary.Threads}}</td>
            <th>Total Questions</th>
            <td>{{.Summary.Questions}}</td>
        </tr>
        <tr>
            <th>Slow Queries</th>
            <td><strong style="color: #900;">{{.Summary.SlowQueries}}</strong></td>
            <th>Opens</th>
            <td>{{.Summary.Opens}}</td>
        </tr>
        <tr>
            <th>Flush Tables</th>
            <td>{{.Summary.FlushTables}}</td>
            <th>Open Tables</th>
            <td>{{.Summary.OpenTables}}</td>
        </tr>
        <tr>
            <th>Queries Per Second Avg</th>
            <td colspan="3"><strong>{{.Summary.QueriesPerSec}}</strong></td>
        </tr>
    </table>

    {{if .EngineMetrics}}
    <div style="display:flex; flex-wrap:wrap; gap:8px; max-width:800px; margin-bottom:16px;">
        {{range .EngineMetrics}}
        <div style="flex:1 1 180px; min-width:160px; max-width:240px; background:#f0f6ff; border:1px solid #bee3f8; border-radius:6px; padding:8px 12px;">
            <div style="font-size:9.5px; color:#2c5282; font-weight:600; text-transform:uppercase; letter-spacing:.4px; margin-bottom:3px;">{{.Key}}</div>
            <div style="font-size:13px; font-weight:700; color:#1a202c;">{{.Value}}</div>
        </div>
        {{end}}
    </div>
    {{end}}

    <!-- 2. Critical Configurations -->
    </div><!-- end sec-body -->
    </div></div><!-- end sec-body/sec-card -->
    <div class="sec-card">
    <div class="sec-hdr" onclick="toggleSec('config')">
        <div class="sec-hdr-left"><span class="sec-num">2</span><h2 id="config">Configuration Variables</h2></div>
        <button class="sec-toggle" id="btn-config">▼</button>
    </div>
    <div class="sec-body collapsed" id="body-config">
    <p>Filter global configuration variables dynamically:</p>
    <input type="text" id="config-search" class="search-box" placeholder="Filter variables..." onkeyup="filterTable('config-table', 'config-search')">
    <table id="config-table">
        <thead>
            <tr>
                <th width="40%">Configuration Parameter</th>
                <th>Value</th>
            </tr>
        </thead>
        <tbody>
            {{range .ConfigVariables}}
            <tr>
                <td><strong>{{.Key}}</strong></td>
                <td>{{.Value}}</td>
            </tr>
            {{else}}
            <tr><td colspan="2">No variable entries found.</td></tr>
            {{end}}
        </tbody>
    </table>

    <!-- 3. Performance Metrics -->
    </div><!-- end sec-body -->
    </div></div><!-- end sec-body/sec-card -->
    <div class="sec-card">
    <div class="sec-hdr" onclick="toggleSec('status')">
        <div class="sec-hdr-left"><span class="sec-num">3</span><h2 id="status">Status Variables</h2></div>
        <button class="sec-toggle" id="btn-status">▼</button>
    </div>
    <div class="sec-body collapsed" id="body-status">
    <p>Filter global status parameters dynamically:</p>
    <input type="text" id="status-search" class="search-box" placeholder="Filter status..." onkeyup="filterTable('status-table', 'status-search')">
    <table id="status-table">
        <thead>
            <tr>
                <th width="40%">Status Variable</th>
                <th>Current Value</th>
            </tr>
        </thead>
        <tbody>
            {{range .StatusCounters}}
            <tr>
                <td><strong>{{.Key}}</strong></td>
                <td>{{.Value}}</td>
            </tr>
            {{else}}
            <tr><td colspan="2">No status counter entries found.</td></tr>
            {{end}}
        </tbody>
    </table>

    <!-- 4. Storage Engine State -->
    </div><!-- end sec-body -->
    </div></div><!-- end sec-body/sec-card -->
    <div class="sec-card">
    <div class="sec-hdr" onclick="toggleSec('innodb')">
        <div class="sec-hdr-left"><span class="sec-num">4</span><h2 id="innodb">Innodb Monitor Stats</h2></div>
        <button class="sec-toggle" id="btn-innodb">▲</button>
    </div>
    <div class="sec-body" id="body-innodb">
    <pre>{{.InnodbStatus}}</pre>

    <!-- 5. Mutexes / Semaphores Analysis -->
    </div><!-- end sec-body -->
    </div></div><!-- end sec-body/sec-card -->
    <div class="sec-card">
    <div class="sec-hdr" onclick="toggleSec('mutexes')">
        <div class="sec-hdr-left"><span class="sec-num">5</span><h2 id="mutexes">Mutexes / Semaphores</h2></div>
        <button class="sec-toggle" id="btn-mutexes">▲</button>
    </div>
    <div class="sec-body" id="body-mutexes">

    {{if .SemaphoreAnalysis.RawBlock}}

    <!-- Aggregate counters -->
    <div style="display:flex; flex-wrap:wrap; gap:10px; margin-bottom:20px; max-width:860px;">
        <div style="flex:1 1 160px; background:#f0f6ff; border:1px solid #bee3f8; border-radius:6px; padding:10px 14px;">
            <div style="font-size:9.5px; color:#2c5282; font-weight:600; text-transform:uppercase; letter-spacing:.4px; margin-bottom:4px;">Spin Waits</div>
            <div style="font-size:18px; font-weight:700; color:#1a202c;">{{.SemaphoreAnalysis.Counters.SpinWaits}}</div>
        </div>
        <div style="flex:1 1 160px; background:#f0f6ff; border:1px solid #bee3f8; border-radius:6px; padding:10px 14px;">
            <div style="font-size:9.5px; color:#2c5282; font-weight:600; text-transform:uppercase; letter-spacing:.4px; margin-bottom:4px;">Spin Rounds</div>
            <div style="font-size:18px; font-weight:700; color:#1a202c;">{{.SemaphoreAnalysis.Counters.SpinRounds}}</div>
        </div>
        <div style="flex:1 1 160px; background:#fff8f0; border:1px solid #fbd38d; border-radius:6px; padding:10px 14px;">
            <div style="font-size:9.5px; color:#7b341e; font-weight:600; text-transform:uppercase; letter-spacing:.4px; margin-bottom:4px;">OS Waits</div>
            <div style="font-size:18px; font-weight:700; color:#c05621;">{{.SemaphoreAnalysis.Counters.OSWaits}}</div>
        </div>
        <div style="flex:1 1 160px; background:#f0fff4; border:1px solid #9ae6b4; border-radius:6px; padding:10px 14px;">
            <div style="font-size:9.5px; color:#22543d; font-weight:600; text-transform:uppercase; letter-spacing:.4px; margin-bottom:4px;">Spin / OS Ratio</div>
            <div style="font-size:18px; font-weight:700; color:#276749;">{{if .SemaphoreAnalysis.Counters.SpinRatio}}{{.SemaphoreAnalysis.Counters.SpinRatio}}x{{else}}—{{end}}</div>
        </div>
    </div>

    <!-- Waiting At hotspots -->
    {{if .SemaphoreAnalysis.WaitingAt}}
    <h3>Threads Waiting At (file:line hotspots)</h3>
    <table style="max-width:700px;">
        <thead><tr><th width="80">Count</th><th>Source Location</th></tr></thead>
        <tbody>
        {{range .SemaphoreAnalysis.WaitingAt}}
        <tr>
            <td><strong style="{{if gt .Count 5}}color:#c05621;{{end}}">{{.Count}}</strong></td>
            <td><code>{{.Location}}</code></td>
        </tr>
        {{end}}
        </tbody>
    </table>
    {{end}}

    <!-- Mutex Holders -->
    {{if .SemaphoreAnalysis.Holders}}
    <h3>Threads Holding Mutexes</h3>
    <table style="max-width:900px;">
        <thead><tr><th width="80">Count</th><th>Details</th></tr></thead>
        <tbody>
        {{range .SemaphoreAnalysis.Holders}}
        <tr>
            <td><strong>{{.Count}}</strong></td>
            <td><small>{{.Location}}</small></td>
        </tr>
        {{end}}
        </tbody>
    </table>
    {{end}}

    <!-- Mutex Creation Sites -->
    {{if .SemaphoreAnalysis.CreatedAt}}
    <h3>Mutex Creation Sites</h3>
    <table style="max-width:700px;">
        <thead><tr><th width="80">Count</th><th>Created In</th></tr></thead>
        <tbody>
        {{range .SemaphoreAnalysis.CreatedAt}}
        <tr>
            <td><strong>{{.Count}}</strong></td>
            <td><code>{{.Location}}</code></td>
        </tr>
        {{end}}
        </tbody>
    </table>
    {{end}}

    <!-- Last Write Locked -->
    {{if .SemaphoreAnalysis.LastWriteLocked}}
    <h3>Last Write-Lock Locations</h3>
    <table style="max-width:700px;">
        <thead><tr><th width="80">Count</th><th>Location</th></tr></thead>
        <tbody>
        {{range .SemaphoreAnalysis.LastWriteLocked}}
        <tr>
            <td><strong>{{.Count}}</strong></td>
            <td><code>{{.Location}}</code></td>
        </tr>
        {{end}}
        </tbody>
    </table>
    {{end}}

    <!-- SHOW ENGINE INNODB MUTEX output -->
    {{if .SemaphoreAnalysis.MutexRows}}
    <h3>Innodb Mutex Status</h3>
    <table style="max-width:900px;">
        <thead>
            <tr>
                <th width="80">Type</th>
                <th>Name</th>
                <th>Status</th>
            </tr>
        </thead>
        <tbody>
        {{range .SemaphoreAnalysis.MutexRows}}
        <tr>
            <td>{{.Type}}</td>
            <td><code>{{.Name}}</code></td>
            <td>{{.Status}}</td>
        </tr>
        {{end}}
        </tbody>
    </table>
    {{else}}
    <h3>SHOW ENGINE INNODB MUTEX</h3>
    <p style="color:#718096; font-size:12px;">No mutex rows returned — no active contention or command not supported.</p>
    {{end}}

    <!-- Raw SEMAPHORES block -->
    <h3>Raw Semaphores Block</h3>
    <pre style="font-size:10px; max-height:260px; overflow-y:auto;">{{.SemaphoreAnalysis.RawBlock}}</pre>

    {{else}}
    <p style="color:#718096; font-size:12px;">No SEMAPHORES data found — SHOW ENGINE INNODB STATUS output may be unavailable or empty.</p>
    {{end}}

    <!-- 6. HA / Replication Topology Consolidated -->
    </div><!-- end sec-body -->
    </div></div><!-- end sec-body/sec-card -->
    <div class="sec-card">
    <div class="sec-hdr" onclick="toggleSec('replication')">
        <div class="sec-hdr-left"><span class="sec-num">6</span><h2 id="replication">HA &amp; Replication Topology</h2></div>
        <button class="sec-toggle" id="btn-replication">▲</button>
    </div>
    <div class="sec-body" id="body-replication">
    
    <h3>Async Replication</h3>
    {{if .ReplicationStates}}
    {{ $root := . }}
    {{range .ReplicationStates}}
    <div style="overflow-x:auto; margin-bottom:20px;">
    <table style="min-width:2200px; width:100%; margin-bottom:0;">
        <thead>
            <tr>
                <th>Channel</th>
                <th>IO Running</th>
                <th>SQL Running</th>
                <th>Source Host</th>
                <th>Source User</th>
                <th>Source Port</th>
                <th>Lag (s)</th>
                <th>Auto Position</th>
                <th>Source Log File</th>
                <th>Read Source Log Pos</th>
                <th>Relay Log File</th>
                <th>Relay Log Pos</th>
                <th>Relay Source Log File</th>
                <th>Exec Source Log Pos</th>
                <th>Replicate_Do_DB</th>
                <th>Replicate_Ignore_DB</th>
                <th>Replicate_Do_Table</th>
                <th>Replicate_Ignore_Table</th>
                <th>Replicate_Wild_Do_Table</th>
                <th>Replicate_Wild_Ignore_Table</th>
                <th>SQL_Delay</th>
                <th>Replicate_Rewrite_DB</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td><strong>{{.ChannelName}}</strong></td>
                <td>{{if eq .ReplicaIORunning "Yes"}}<span style="color:green;font-weight:bold;">Yes</span>{{else}}<span style="color:red;font-weight:bold;">{{.ReplicaIORunning}}</span>{{end}}</td>
                <td>{{if eq .ReplicaSQLRunning "Yes"}}<span style="color:green;font-weight:bold;">Yes</span>{{else}}<span style="color:red;font-weight:bold;">{{.ReplicaSQLRunning}}</span>{{end}}</td>
                <td><strong>{{.SourceHost}}</strong></td>
                <td>{{.SourceUser}}</td>
                <td>{{.SourcePort}}</td>
                <td><strong>{{.SecondsBehind}}</strong></td>
                <td>{{.AutoPosition}}</td>
                <td><code style="white-space:nowrap;">{{.MasterLogFile}}</code></td>
                <td class="text-right">{{.ReadMasterLogPos}}</td>
                <td><code style="white-space:nowrap;">{{.RelayLogFile}}</code></td>
                <td class="text-right">{{.RelayLogPos}}</td>
                <td><code style="white-space:nowrap;">{{.RelayMasterLogFile}}</code></td>
                <td class="text-right">{{.ExecMasterLogPos}}</td>
                <td><small>{{if .ReplicateDoDB}}{{.ReplicateDoDB}}{{else}}—{{end}}</small></td>
                <td><small>{{if .ReplicateIgnoreDB}}{{.ReplicateIgnoreDB}}{{else}}—{{end}}</small></td>
                <td><small>{{if .ReplicateDoTable}}{{.ReplicateDoTable}}{{else}}—{{end}}</small></td>
                <td><small>{{if .ReplicateIgnoreTable}}{{.ReplicateIgnoreTable}}{{else}}—{{end}}</small></td>
                <td><small>{{if .ReplicateWildDoTable}}{{.ReplicateWildDoTable}}{{else}}—{{end}}</small></td>
                <td><small>{{if .ReplicateWildIgnoreTable}}{{.ReplicateWildIgnoreTable}}{{else}}—{{end}}</small></td>
                <td><small>{{if .SQLDelay}}{{.SQLDelay}}{{else}}—{{end}}</small></td>
                <td><small>{{if .ReplicateRewriteDB}}{{.ReplicateRewriteDB}}{{else}}—{{end}}</small></td>
            </tr>
        </tbody>
    </table>
    </div>
    {{if or .RetrievedGtidSet .ExecutedGtidSet $root.GTIDInfo.GTIDPurged}}
    <div style="overflow-x:auto; margin-bottom:20px;">
    <table style="min-width:900px; width:100%; margin-bottom:0;">
        <thead>
            <tr>
                <th>Retrieved GTID Set</th>
                <th>gtid_purged</th>
                <th>Executed GTID Set</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td><small><code style="white-space:nowrap;">{{if .RetrievedGtidSet}}{{.RetrievedGtidSet}}{{else}}—{{end}}</code></small></td>
                <td><small><code style="white-space:nowrap;">{{if $root.GTIDInfo.GTIDPurged}}{{ $root.GTIDInfo.GTIDPurged }}{{else}}—{{end}}</code></small></td>
                <td><small><code style="white-space:nowrap;">{{if .ExecutedGtidSet}}{{.ExecutedGtidSet}}{{else}}—{{end}}</code></small></td>
            </tr>
        </tbody>
    </table>
    </div>
    {{end}}
    {{if or .LastIOError .LastSQLError}}
    <table style="max-width:900px; margin-bottom:20px;">
        <thead>
            <tr>
                <th>Last IO Error</th>
                <th>Last SQL Error</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td><small style="color:red;">{{if .LastIOError}}{{.LastIOError}}{{else}}—{{end}}</small></td>
                <td><small style="color:red;">{{if .LastSQLError}}{{.LastSQLError}}{{else}}—{{end}}</small></td>
            </tr>
        </tbody>
    </table>
    {{end}}
    {{end}}
    {{else}}
    <table><tbody><tr><td>No active replica / replication channels configured on this node.</td></tr></tbody></table>
    {{end}}

    <h3>Semi-Sync Replication Stats</h3>
    {{if .SemiSyncDetails}}
    <table style="max-width:900px; margin-bottom:20px;">
        <thead>
            <tr>
                <th style="width:55%;">Variable</th>
                <th style="width:45%;">Value</th>
            </tr>
        </thead>
        <tbody>
            {{range .SemiSyncDetails}}
            <tr>
                <td><strong>{{.Key}}</strong></td>
                <td>{{.Value}}</td>
            </tr>
            {{end}}
        </tbody>
    </table>
    {{else}}
    <p><em>No semi-synchronous replication variables found.</em></p>
    {{end}}

    <!-- ═══ Replication Applier Worker Status ═══════════════════════════════ -->
    <h3>Replication Applier Worker Status</h3>
    {{if .ReplicaWorkers}}
    <table style="width:100%; max-width:1200px; margin-bottom:20px;">
        <thead>
            <tr>
                <th style="width:10%;">Channel Name</th>
                <th style="width:7%;text-align:center;">Worker ID</th>
                <th style="width:8%;text-align:center;">Last Error #</th>
                <th style="width:50%;">Last Error Message</th>
                <th style="width:20%;">Last Error Timestamp</th>
            </tr>
        </thead>
        <tbody>
            {{range .ReplicaWorkers}}
            <tr{{if ne .LastErrorNumber "0"}} style="background-color:#fff5f5;"{{end}}>
                <td><strong>{{if .ChannelName}}{{.ChannelName}}{{else}}<em style="color:#999;">(default)</em>{{end}}</strong></td>
                <td style="text-align:center;"><strong>{{.WorkerID}}</strong></td>
                <td style="text-align:center;">
                    {{if ne .LastErrorNumber "0"}}
                        <span style="color:#c00;font-weight:bold;">{{.LastErrorNumber}}</span>
                    {{else}}
                        <span style="color:green;font-weight:bold;">0</span>
                    {{end}}
                </td>
                <td style="word-break:break-word;font-size:11px;">
                    {{if .LastErrorMessage}}
                        <span style="color:#c00;font-weight:bold;">{{.LastErrorMessage}}</span>
                    {{else}}
                        <span style="color:#999;">—</span>
                    {{end}}
                </td>
                <td style="font-size:10px;white-space:nowrap;">
                    {{if and .LastErrorTimestamp (ne .LastErrorTimestamp "") (ne .LastErrorTimestamp "0000-00-00 00:00:00.000000")}}
                        <code style="color:#c00;">{{.LastErrorTimestamp}}</code>
                    {{else}}
                        <span style="color:#999;">—</span>
                    {{end}}
                </td>
            </tr>
            {{end}}
        </tbody>
    </table>
    {{else}}
    <table style="max-width:900px; margin-bottom:20px;"><tbody>
        <tr><td style="color:#888;font-style:italic;">
            No rows from performance_schema.replication_applier_status_by_worker.
            Table is populated only when slave_parallel_workers &gt; 0 and this is a replica.
        </td></tr>
    </tbody></table>
    {{end}}

    <h3>Group Replication Members</h3>
    <table style="max-width:1100px; margin-bottom:20px;">
        <thead>
            <tr>
                <th>Channel Name</th>
                <th>Member ID</th>
                <th>Member Host</th>
                <th>Port</th>
                <th>Member State</th>
                <th>Member Role</th>
                <th>Version</th>
            </tr>
        </thead>
        <tbody>
            {{range .GroupMembers}}
            <tr>
                <td><strong>{{if .ChannelName}}{{.ChannelName}}{{else}}—{{end}}</strong></td>
                <td style="font-family:monospace;font-size:11px;">{{if .MemberID}}{{.MemberID}}{{else}}—{{end}}</td>
                <td><strong>{{if .MemberHost}}{{.MemberHost}}{{else}}—{{end}}</strong></td>
                <td style="text-align:center;">{{if .MemberPort}}{{.MemberPort}}{{else}}—{{end}}</td>
                <td style="font-weight:bold; {{if eq .MemberState "ONLINE"}}color:green;{{else if .MemberState}}color:#c00;{{end}}">
                    {{if .MemberState}}{{.MemberState}}{{else}}—{{end}}
                </td>
                <td><strong>{{if .MemberRole}}{{.MemberRole}}{{else}}—{{end}}</strong></td>
                <td>{{if .Version}}{{.Version}}{{else}}—{{end}}</td>
            </tr>
            {{else}}
            <tr><td colspan="7" style="color:#888;font-style:italic;">No group replication members detected.</td></tr>
            {{end}}
        </tbody>
    </table>

    {{if or .GRFlowControlLimit .GRQueues}}
    <h3>Group Replication Queues</h3>
    {{if .GRFlowControlLimit}}
    <p style="margin-bottom:8px;"><strong>{{.GRFlowControlLimit}}</strong></p>
    {{end}}
    <table style="max-width:800px; margin-bottom:20px;">
        <thead>
            <tr>
                <th>Member ID</th>
                <th style="text-align:center;">Cert Queue (certifier)</th>
                <th style="text-align:center;">Applier Queue</th>
            </tr>
        </thead>
        <tbody>
            {{range .GRQueues}}
            <tr>
                <td style="font-family:monospace;font-size:11px;">{{.MemberID}}</td>
                <td style="text-align:center; font-weight:bold;
                    {{if gt .CertQueue 1000}}color:#c00;{{else if gt .CertQueue 100}}color:#d97706;{{else}}color:green;{{end}}">
                    {{.CertQueue}}
                </td>
                <td style="text-align:center; font-weight:bold;
                    {{if gt .ApplierQueue 1000}}color:#c00;{{else if gt .ApplierQueue 100}}color:#d97706;{{else}}color:green;{{end}}">
                    {{.ApplierQueue}}
                </td>
            </tr>
            {{else}}
            <tr><td colspan="3" style="color:#888;font-style:italic;">No active member queue stats.</td></tr>
            {{end}}
        </tbody>
    </table>
    {{end}}

    {{if or .GRFlowConfig.CommStack .GRFlowConfig.FlowControlMode}}
    <h3>Group Replication — Flow Control &amp; Consistency Configuration</h3>
    <table style="max-width:1200px; margin-bottom:20px;">
        <thead>
            <tr>
                <th>bootstrap_group</th>
                <th>communication_stack</th>
                <th>consistency</th>
                <th>flow_control_mode</th>
                <th>flow_control_applier_threshold</th>
                <th>flow_control_certifier_threshold</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td><strong>{{if .GRFlowConfig.BootstrapGroup}}{{.GRFlowConfig.BootstrapGroup}}{{else}}—{{end}}</strong></td>
                <td><strong>{{if .GRFlowConfig.CommStack}}{{.GRFlowConfig.CommStack}}{{else}}—{{end}}</strong></td>
                <td><strong>{{if .GRFlowConfig.Consistency}}{{.GRFlowConfig.Consistency}}{{else}}—{{end}}</strong></td>
                <td><strong>{{if .GRFlowConfig.FlowControlMode}}{{.GRFlowConfig.FlowControlMode}}{{else}}—{{end}}</strong></td>
                <td style="text-align:center; font-weight:bold; {{if gt (toInt .GRFlowConfig.ApplierThreshold) 25000}}color:#c00;{{end}}">
                    {{if .GRFlowConfig.ApplierThreshold}}{{.GRFlowConfig.ApplierThreshold}}{{else}}—{{end}}
                </td>
                <td style="text-align:center; font-weight:bold; {{if gt (toInt .GRFlowConfig.CertThreshold) 25000}}color:#c00;{{end}}">
                    {{if .GRFlowConfig.CertThreshold}}{{.GRFlowConfig.CertThreshold}}{{else}}—{{end}}
                </td>
            </tr>
        </tbody>
    </table>
    {{end}}

    {{if or .GaleraFlowControl .GaleraSummary.ClusterSize}}
    <h3>Galera / Percona XtraDB Cluster (PXC) Flow Control &amp; Status</h3>

    {{if .GaleraQueues.RecvQueue}}
    <table style="max-width:600px; margin-bottom:15px;">
        <thead>
            <tr>
                <th>Recv Queue</th>
                <th>Send Queue</th>
                <th>Flow Control Paused</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td><strong>{{.GaleraQueues.RecvQueue}}</strong></td>
                <td><strong>{{.GaleraQueues.SendQueue}}</strong></td>
                <td><strong style="color: #900;">{{.GaleraQueues.FlowControlPaused}}</strong></td>
            </tr>
        </tbody>
    </table>
    {{end}}

    <table style="max-width:600px; margin-bottom:15px;">
        <thead>
            <tr>
                <th>Cluster Name</th>
                <th>Cluster Size</th>
                <th>Cluster Status</th>
                <th>Flow Control</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td><strong>{{.GaleraSummary.ClusterName}}</strong></td>
                <td class="text-right"><strong>{{.GaleraSummary.ClusterSize}}</strong></td>
                <td>{{.GaleraSummary.ClusterStatus}}</td>
                <td>{{if .GaleraFlowControl}}{{.GaleraFlowControl}}{{else}}—{{end}}</td>
            </tr>
        </tbody>
    </table>

    {{if .GaleraSummary.IncomingAddresses}}
    <h4>PXC Cluster Node Members</h4>
    <table style="max-width:400px;">
        <thead>
            <tr>
                <th>#</th>
                <th>Node Address</th>
                <th>Port</th>
            </tr>
        </thead>
        <tbody id="pxc-nodes-tbody">
            <tr><td colspan="3">Loading...</td></tr>
        </tbody>
    </table>
    <script>
    (function() {
        var raw = "{{.GaleraSummary.IncomingAddresses}}";
        var nodes = raw.split(",");
        var tbody = document.getElementById("pxc-nodes-tbody");
        tbody.innerHTML = "";
        nodes.forEach(function(addr, i) {
            addr = addr.trim();
            var parts = addr.split(":");
            var host = parts[0] || addr;
            var port = parts[1] || "";
            var tr = document.createElement("tr");
            tr.innerHTML = "<td>" + (i + 1) + "</td><td><strong>" + host + "</strong></td><td><code>" + port + "</code></td>";
            tbody.appendChild(tr);
        });
    })();
    </script>
    {{end}}

    {{if or .WsrepQueueMax.RecvQueue .WsrepQueueMax.RecvQueueMax}}
    <h4>Galera Receive Queue vs Flow Control Limit</h4>
    <table style="max-width:700px; margin-bottom:12px;">
        <thead>
            <tr>
                <th>VARIABLE_NAME</th>
                <th>VARIABLE_VALUE</th>
                <th>gcs.fc_limit</th>
                <th>Alert</th>
            </tr>
        </thead>
        <tbody>
            <tr{{if .WsrepQueueMax.Alert}} style="background:#fff0f0;"{{end}}>
                <td>wsrep_local_recv_queue</td>
                <td><strong>{{.WsrepQueueMax.RecvQueue}}</strong></td>
                <td rowspan="2" style="vertical-align:middle;text-align:center;">
                    <strong>{{if .WsrepQueueMax.FCLimit}}{{.WsrepQueueMax.FCLimit}}{{else}}—{{end}}</strong>
                </td>
                <td rowspan="2" style="vertical-align:middle;text-align:center;">
                    {{if .WsrepQueueMax.Alert}}
                        <strong style="color:#c00;">⚠ recv_queue_max ≥ fc_limit — consider increasing gcs.fc_limit</strong>
                    {{else}}
                        <span style="color:green;">OK</span>
                    {{end}}
                </td>
            </tr>
            <tr{{if .WsrepQueueMax.Alert}} style="background:#fff0f0;"{{end}}>
                <td>wsrep_local_recv_queue_max</td>
                <td><strong{{if .WsrepQueueMax.Alert}} style="color:#c00;"{{end}}>{{.WsrepQueueMax.RecvQueueMax}}</strong></td>
            </tr>
        </tbody>
    </table>
    {{end}}

    {{if .WsrepProviderOptions}}
    <h4>wsrep_provider_options</h4>
    <div style="display:flex; flex-wrap:wrap; gap:0; border-top:1px solid #6FAEBF; border-left:1px solid #6FAEBF; margin-bottom:20px; max-width:1200px;">
        {{range .WsrepProviderOptions}}
        <div style="border-right:1px solid #6FAEBF; border-bottom:1px solid #6FAEBF; padding:4px 10px; min-width:140px; max-width:220px; flex:1;">
            <div style="font-family:monospace; font-size:10px; color:#1a3a6a; white-space:nowrap; overflow:hidden; text-overflow:ellipsis;" title="{{.Key}}">{{.Key}}</div>
            <div style="font-family:monospace; font-size:12px; font-weight:bold; color:#222; word-break:break-all;">{{if .Value}}{{.Value}}{{else}}&nbsp;{{end}}</div>
        </div>
        {{end}}
    </div>
    {{end}}

    {{end}}

    {{if or .ClusterStatus .ClusterSetStatus .ClusterSetTopology .Routers}}
    <h3>InnoDB Cluster &amp; ClusterSet Topology</h3>

    {{if .ClusterSetTopology}}
    {{$topo := .ClusterSetTopology}}

    <!-- ClusterSet Global Summary -->
    <h4>ClusterSet: {{$topo.DomainName}}</h4>
    <table style="max-width:700px; margin-bottom:18px;">
        <thead><tr><th>Property</th><th>Value</th></tr></thead>
        <tbody>
            <tr><td>Domain Name</td><td><strong>{{$topo.DomainName}}</strong></td></tr>
            <tr><td>Global Status</td>
                <td><strong style="{{if eq $topo.Status "HEALTHY"}}color:green{{else}}color:#c00{{end}}">{{$topo.Status}}</strong></td>
            </tr>
            <tr><td>Global Primary Instance</td><td>{{if $topo.GlobalPrimaryInstance}}<code>{{$topo.GlobalPrimaryInstance}}</code>{{end}}</td></tr>
            <tr><td>Primary Cluster (DC)</td><td><strong>{{$topo.PrimaryCluster}}</strong></td></tr>
            <tr><td>Metadata Server</td><td>{{if $topo.MetadataServer}}<code>{{$topo.MetadataServer}}</code>{{end}}</td></tr>
        </tbody>
    </table>

    <!-- Primary Cluster -->
    {{if $topo.PrimaryClusterData}}
    {{$pc := $topo.PrimaryClusterData}}
    <h4 style="color:#1a5276;">&#9654; Primary Cluster (DC): {{$pc.Name}}
        {{if $pc.StatusText}}<span style="font-weight:normal;font-size:12px;color:#555;"> — {{$pc.StatusText}}</span>{{end}}
    </h4>
    <table style="max-width:900px; margin-bottom:8px;">
        <thead>
            <tr>
                <th>Node Address</th>
                <th>Member Role</th>
                <th>Mode</th>
                <th>Status</th>
                <th>Version</th>
            </tr>
        </thead>
        <tbody>
            {{range $pc.Nodes}}
            <tr>
                <td><code>{{.Address}}</code></td>
                <td><strong>{{.MemberRole}}</strong></td>
                <td>{{.Mode}}</td>
                <td style="font-weight:bold; {{if eq .Status "ONLINE"}}color:green{{else}}color:#c00{{end}}">{{.Status}}</td>
                <td>{{.Version}}</td>
            </tr>
            {{end}}
        </tbody>
    </table>
    <table style="max-width:700px; margin-bottom:18px;">
        <tbody>
            <tr><td style="color:#555;width:200px;">Cluster Role</td><td><strong>{{$pc.ClusterRole}}</strong></td></tr>
            <tr><td style="color:#555;">Global Status</td>
                <td><strong style="{{if eq $pc.GlobalStatus "OK"}}color:green{{else}}color:#c00{{end}}">{{$pc.GlobalStatus}}</strong></td>
            </tr>
            <tr><td style="color:#555;">Status</td><td><strong>{{$pc.Status}}</strong></td></tr>
            {{if $pc.TransactionSet}}<tr><td style="color:#555;">Transaction Set (GTID)</td><td><code style="font-size:11px;">{{$pc.TransactionSet}}</code></td></tr>{{end}}
        </tbody>
    </table>
    {{end}}

    <!-- Replica Clusters -->
    {{range $topo.ReplicaClusters}}
    {{$rc := .}}
    <h4 style="color:#922b21;">&#9654; Secondary Cluster (DR): {{$rc.Name}}
        {{if $rc.StatusText}}<span style="font-weight:normal;font-size:12px;color:#555;"> — {{$rc.StatusText}}</span>{{end}}
    </h4>
    <table style="max-width:900px; margin-bottom:8px;">
        <thead>
            <tr>
                <th>Node Address</th>
                <th>Member Role</th>
                <th>Mode</th>
                <th>Status</th>
                <th>Version</th>
                <th>Replication Lag</th>
            </tr>
        </thead>
        <tbody>
            {{range $rc.Nodes}}
            <tr>
                <td><code>{{.Address}}</code></td>
                <td><strong>{{.MemberRole}}</strong></td>
                <td>{{.Mode}}</td>
                <td style="font-weight:bold; {{if eq .Status "ONLINE"}}color:green{{else}}color:#c00{{end}}">{{.Status}}</td>
                <td>{{.Version}}</td>
                <td>{{if .ReplicationLag}}{{.ReplicationLag}}{{else}}—{{end}}</td>
            </tr>
            {{end}}
        </tbody>
    </table>
    <table style="max-width:900px; margin-bottom:8px;">
        <tbody>
            <tr><td style="color:#555;width:240px;">Cluster Role</td><td><strong>{{$rc.ClusterRole}}</strong></td></tr>
            <tr><td style="color:#555;">Global Status</td>
                <td><strong style="{{if eq $rc.GlobalStatus "OK"}}color:green{{else}}color:#c00{{end}}">{{$rc.GlobalStatus}}</strong></td>
            </tr>
            <tr><td style="color:#555;">ClusterSet Replication Status</td><td><strong>{{$rc.Status}}</strong></td></tr>
            {{if $rc.TransactionSet}}<tr><td style="color:#555;">Transaction Set (GTID)</td><td><code style="font-size:11px;">{{$rc.TransactionSet}}</code></td></tr>{{end}}
            {{if $rc.TxConsistencyStatus}}<tr><td style="color:#555;">TX Consistency Status</td>
                <td><strong style="{{if eq $rc.TxConsistencyStatus "OK"}}color:green{{else}}color:#c00{{end}}">{{$rc.TxConsistencyStatus}}</strong></td>
            </tr>{{end}}
            {{if $rc.TxErrantGTID}}<tr><td style="color:#555;">Errant GTID Set</td><td><code style="color:#c00;font-size:11px;">{{$rc.TxErrantGTID}}</code></td></tr>{{end}}
            {{if $rc.TxMissingGTID}}<tr><td style="color:#555;">Missing GTID Set</td><td><code style="color:#c00;font-size:11px;">{{$rc.TxMissingGTID}}</code></td></tr>{{end}}
        </tbody>
    </table>
    <!-- ClusterSet Replication Channel (DC → DR async link) -->
    {{if $rc.CSReplSource}}
    <h5 style="margin:6px 0 4px;color:#444;">ClusterSet Async Replication Channel (DC → DR)</h5>
    <table style="max-width:900px; margin-bottom:18px;">
        <thead>
            <tr>
                <th>Source</th>
                <th>Receiver (DR)</th>
                <th>Receiver Status</th>
                <th>Receiver Thread State</th>
                <th>Applier Status</th>
                <th>Applier Threads</th>
                <th>Applier Thread State</th>
                <th>SSL Mode</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td><code>{{$rc.CSReplSource}}</code></td>
                <td><code>{{$rc.CSReplReceiver}}</code></td>
                <td style="font-weight:bold; {{if eq $rc.CSReplReceiverStatus "ON"}}color:green{{else}}color:#c00{{end}}">{{$rc.CSReplReceiverStatus}}</td>
                <td style="font-size:11px;">{{$rc.CSReplReceiverThreadState}}</td>
                <td style="font-weight:bold; {{if eq $rc.CSReplApplierStatus "APPLIED_ALL"}}color:green{{else}}color:#c00{{end}}">{{$rc.CSReplApplierStatus}}</td>
                <td style="text-align:center;">{{$rc.CSReplApplierThreads}}</td>
                <td style="font-size:11px;">{{$rc.CSReplApplierThreadState}}</td>
                <td>{{$rc.CSReplSSLMode}}</td>
            </tr>
        </tbody>
    </table>
    {{end}}
    {{end}}

    {{else if .ClusterSetStatus}}
    <!-- Fallback: raw JSON from metadata tables -->
    <p style="font-size:12px;color:#888;margin-bottom:4px;">ClusterSet status (raw from metadata):</p>
    <pre style="font-size:11px;background:#fafeff;border:1px solid #6FAEBF;padding:10px;overflow-x:auto;max-height:400px;">{{.ClusterSetStatus}}</pre>
    {{end}}

    {{if .ClusterStatus}}
    <h4>InnoDB Cluster Status (raw metadata)</h4>
    <pre style="font-size:11px;background:#fafeff;border:1px solid #6FAEBF;padding:10px;overflow-x:auto;max-height:300px;">{{.ClusterStatus}}</pre>
    {{end}}

    <!-- Single InnoDB Cluster (no ClusterSet) -->
    {{if .SingleClusterTopo}}
    {{$sc := .SingleClusterTopo}}
    <h4 style="color:#1a5276;">&#9654; InnoDB Cluster
        <span style="font-weight:normal;font-size:12px;color:#555;"> — {{$sc.Status}}</span>
    </h4>
    <table style="max-width:700px; margin-bottom:10px;">
        <thead><tr><th>Property</th><th>Value</th></tr></thead>
        <tbody>
            <tr><td>Global Primary</td><td><code>{{$sc.GlobalPrimaryInstance}}</code></td></tr>
            <tr><td>Status</td><td><strong style="{{if eq $sc.Status "OK"}}color:green{{else}}color:#c00{{end}}">{{$sc.Status}}</strong></td></tr>
        </tbody>
    </table>
    {{if $sc.PrimaryClusterData}}
    <table style="max-width:1000px; margin-bottom:18px;">
        <thead>
            <tr>
                <th>Node Address</th><th>Member Role</th><th>Mode</th>
                <th>Status</th><th>Version</th><th>Replication Lag</th>
            </tr>
        </thead>
        <tbody>
            {{range $sc.PrimaryClusterData.Nodes}}
            <tr>
                <td><code>{{.Address}}</code></td>
                <td><strong>{{.MemberRole}}</strong></td>
                <td>{{.Mode}}</td>
                <td style="font-weight:bold; {{if eq .Status "ONLINE"}}color:green{{else}}color:#c00{{end}}">{{.Status}}</td>
                <td>{{.Version}}</td>
                <td>{{if .ReplicationLag}}{{.ReplicationLag}}{{else}}—{{end}}</td>
            </tr>
            {{end}}
        </tbody>
    </table>
    {{end}}
    {{end}}

    <!-- MySQL Router — single table, routing options merged as columns -->
    {{if or .CSRouters .CSRoutingOptions}}
    <h3>MySQL Router Instances</h3>
    {{if .CSRouters}}
    <table style="max-width:1400px; margin-bottom:10px;">
        <thead>
            <tr>
                <th>Router Name</th>
                <th>Hostname</th>
                <th>Version</th>
                <th>Last Check-In</th>
                <th>RW Port</th>
                <th>RO Port</th>
                <th>RWX Port</th>
                <th>ROX Port</th>
                <th>RW Split</th>
                <th>Target Cluster</th>
                <th>Global target_cluster</th>
                <th>On Invalidate</th>
            </tr>
        </thead>
        <tbody>
            {{range .CSRouters}}
            <tr>
                <td><strong>{{.RouterKey}}</strong></td>
                <td>{{.Hostname}}</td>
                <td>{{.Version}}</td>
                <td style="font-size:11px;">{{if .LastCheckIn}}{{.LastCheckIn}}{{else}}—{{end}}</td>
                <td style="text-align:center;">{{if .RWPort}}{{.RWPort}}{{else}}—{{end}}</td>
                <td style="text-align:center;">{{if .ROPort}}{{.ROPort}}{{else}}—{{end}}</td>
                <td style="text-align:center;">{{if .RWXPort}}{{.RWXPort}}{{else}}—{{end}}</td>
                <td style="text-align:center;">{{if .ROXPort}}{{.ROXPort}}{{else}}—{{end}}</td>
                <td style="text-align:center;">{{if .RWSplitPort}}{{.RWSplitPort}}{{else}}—{{end}}</td>
                <td><strong>{{if .TargetCluster}}{{.TargetCluster}}{{else}}—{{end}}</strong></td>
                <td><strong>{{if $.CSRoutingOptions}}{{$.CSRoutingOptions.GlobalTargetCluster}}{{else}}—{{end}}</strong></td>
                <td><strong>{{if $.CSRoutingOptions}}{{$.CSRoutingOptions.GlobalInvalidatedPolicy}}{{else}}—{{end}}</strong></td>
            </tr>
            {{end}}
        </tbody>
    </table>
    {{else if .CSRoutingOptions}}
    {{$ro := .CSRoutingOptions}}
    <table style="max-width:600px; margin-bottom:10px;">
        <thead><tr><th>Option</th><th>Value</th></tr></thead>
        <tbody>
            <tr><td>target_cluster</td><td><strong>{{$ro.GlobalTargetCluster}}</strong></td></tr>
            <tr><td>invalidated_cluster_policy</td><td><strong>{{$ro.GlobalInvalidatedPolicy}}</strong></td></tr>
            <tr><td>stats_updates_frequency</td><td>{{$ro.GlobalStatsFrequency}}</td></tr>
            {{range $key, $val := $ro.RouterOverrides}}
            <tr><td>{{$key}} target_cluster override</td><td><strong style="color:#c00;">{{$val}}</strong></td></tr>
            {{end}}
        </tbody>
    </table>
    {{end}}
    {{end}}

    {{end}}{{/* end: if ClusterStatus/ClusterSetStatus/ClusterSetTopology/Routers */}}

    <!-- 6. Current Binary Log Status -->
    </div><!-- end sec-body -->
    </div></div><!-- end sec-body/sec-card -->
    <div class="sec-card">
    <div class="sec-hdr" onclick="toggleSec('replica-source')">
        <div class="sec-hdr-left"><span class="sec-num">7</span><h2 id="replica-source">Binary Log Status</h2></div>
        <button class="sec-toggle" id="btn-replica-source">▲</button>
    </div>
    <div class="sec-body" id="body-replica-source">
    {{if .MasterStatus}}
    <table style="max-width: 100%; margin-top: 10px;">
        <thead>
            <tr>
                <th>File</th>
                <th>Position</th>
                <th>Binlog_Do_DB</th>
                <th>Binlog_Ignore_DB</th>
                <th>Executed_Gtid_Set</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td>{{.CurrentBinlogFile}}</td>
                <td>{{masterStatusValue .MasterStatus "Position"}}</td>
                <td>{{masterStatusValue .MasterStatus "Binlog_Do_DB"}}</td>
                <td>{{masterStatusValue .MasterStatus "Binlog_Ignore_DB"}}</td>
                <td>{{masterStatusValue .MasterStatus "Executed_Gtid_Set"}}</td>
            </tr>
        </tbody>
    </table>
    {{else}}
    <table style="max-width: 600px; margin-top: 10px;">
        <tbody>
            <tr><td colspan="2">Local binary logging parameters are inactive or master status is empty.</td></tr>
        </tbody>
    </table>
    {{end}}

    {{if .BinaryLogCount}}
    <table style="max-width: 600px; margin-top: 10px;">
        <thead>
            <tr>
                <th>Binary Log Count</th>
                <th>Total Binary Log Size</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td>{{.BinaryLogCount}}</td>
                <td>{{if .BinaryLogSizeHuman}}{{.BinaryLogSizeHuman}}{{else}}{{.BinaryLogSize}}{{end}}</td>
            </tr>
        </tbody>
    </table>
    {{end}}

    {{if .CurrentBinlogEvents}}
    <h3 style="margin-top: 16px;">Recent Events for Current Binary Log</h3>
    <table style="max-width: 100%;">
        <thead>
            <tr>
                <th>Log Name</th>
                <th>Pos</th>
                <th>Event Type</th>
                <th>Server ID</th>
                <th>End Log Pos</th>
                <th>Info</th>
            </tr>
        </thead>
        <tbody>
            {{range .CurrentBinlogEvents}}
            <tr>
                <td>{{.LogName}}</td>
                <td>{{.Pos}}</td>
                <td>{{.EventType}}</td>
                <td>{{.ServerID}}</td>
                <td>{{.EndLogPos}}</td>
                <td>{{.Info}}</td>
            </tr>
            {{end}}
        </tbody>
    </table>
    {{end}}

    <!-- 7. Process List & Query History -->
    </div><!-- end sec-body -->
    </div></div><!-- end sec-body/sec-card -->
    <div class="sec-card">
    <div class="sec-hdr" onclick="toggleSec('process')">
        <div class="sec-hdr-left"><span class="sec-num">8</span><h2 id="process">Process List &amp; Query Locks</h2></div>
        <button class="sec-toggle" id="btn-process">▲</button>
    </div>
    <div class="sec-body" id="body-process">
    
    <h3>Active Connections Thread Status</h3>
    <table>
        <thead>
            <tr>
                <th>ID</th>
                <th>User</th>
                <th>Host IP Address</th>
                <th>DB</th>
                <th>Command</th>
                <th>Time (s)</th>
                <th>State</th>
                <th>Executing Statement Info</th>
            </tr>
        </thead>
        <tbody>
            {{range .Processes}}
            <tr>
                <td>{{.ID}}</td>
                <td><strong>{{.User}}</strong></td>
                <td>{{.Host}}</td>
                <td>{{.DB}}</td>
                <td>{{.Command}}</td>
                <td class="text-right"><strong>{{.Time}}</strong></td>
                <td>{{.State}}</td>
                <td><small>{{.Info}}</small></td>
            </tr>
            {{else}}
            <tr><td colspan="8">No active user connection threads detected.</td></tr>
            {{end}}
        </tbody>
    </table>

    <h3>Historical Statement Summary Digests</h3>
    <table>
        <thead>
            <tr>
                <th>Schema</th>
                <th>Query sample</th>
                <th>Executions</th>
                <th>Total Exec (s)</th>
                <th>Avg Exec (ms)</th>
                <th>Total Lock (s)</th>
                <th>Rows Examined</th>
                <th>Rows Sent</th>
                <th>Tmp Tables</th>
                <th>Tmp Disk Tables</th>
                <th>Sort Rows</th>
                <th>No Index</th>
                <th>No Good Index</th>
            </tr>
        </thead>
        <tbody>
            {{range .DigestStats}}
            <tr>
                <td><strong>{{.SchemaName}}</strong></td>
                <td><code>{{.QuerySample}}</code></td>
                <td class="text-right">{{.ExecCount}}</td>
                <td class="text-right"><strong>{{.TotalExecSec}} s</strong></td>
                <td class="text-right">{{.AvgExecMs}} ms</td>
                <td class="text-right"><strong>{{.TotalLockSec}} s</strong></td>
                <td class="text-right">{{.RowsExamined}}</td>
                <td class="text-right">{{.RowsSent}}</td>
                <td class="text-right">{{.CreatedTmpTables}}</td>
                <td class="text-right">{{.CreatedTmpDiskTables}}</td>
                <td class="text-right">{{.SortRows}}</td>
                <td class="text-right">{{.NoIndexUsed}}</td>
                <td class="text-right">{{.NoGoodIndexUsed}}</td>
            </tr>
            {{else}}
            <tr><td colspan="13">No historical statement digests found in performance_schema.</td></tr>
            {{end}}
        </tbody>
    </table>

    <h3>Lock Waits &amp; Blocking Transactions</h3>
    <table>
        <thead>
            <tr>
                <th>Wait Started</th>
                <th>Age (s)</th>
                <th>Locked Table</th>
                <th>Index</th>
                <th>Type</th>
                <th>Waiting PID</th>
                <th>Waiting Trx ID</th>
                <th>Waiting Lock Mode</th>
                <th>Waiting Query</th>
                <th>Blocking PID</th>
                <th>Blocking Trx ID</th>
                <th>Blocking Lock Mode</th>
                <th>Blocking Query</th>
                <th>Kill Query Instruction</th>
            </tr>
        </thead>
        <tbody>
            {{range .LockWaits}}
            <tr>
                <td>{{.WaitStarted}}</td>
                <td class="text-right"><strong>{{.WaitAgeSecs}}</strong></td>
                <td><code>{{.LockedTable}}</code></td>
                <td>{{.LockedIndex}}</td>
                <td>{{.LockedType}}</td>
                <td>{{.WaitingPid}}</td>
                <td>{{.WaitingTrxId}}</td>
                <td>{{.WaitingLockMode}}</td>
                <td><small>{{.WaitingQuery}}</small></td>
                <td><strong>{{.BlockingPid}}</strong></td>
                <td>{{.BlockingTrxId}}</td>
                <td>{{.BlockingLockMode}}</td>
                <td><small>{{.BlockingQuery}}</small></td>
                <td><code><strong style="color: red;">{{.SQLKillBlockingConnection}}</strong></code></td>
            </tr>
            {{else}}
            <tr><td colspan="14">No current record locking bottlenecks or waits reported.</td></tr>
            {{end}}
        </tbody>
    </table>

    <h3>MDL &amp; DDL Lock Waits</h3>
    <table>
        <thead>
            <tr>
                <th>Object Schema</th>
                <th>Object Name</th>
                <th>Waiting PID</th>
                <th>Waiting Lock Type</th>
                <th>Waiting Query</th>
                <th>Wait (s)</th>
                <th>Blocking PID</th>
                <th>Blocking Lock Type</th>
                <th>Kill Instruction</th>
            </tr>
        </thead>
        <tbody>
            {{range .SchemaTableLockWaits}}
            <tr>
                <td><strong>{{.ObjectSchema}}</strong></td>
                <td><code>{{.ObjectName}}</code></td>
                <td>{{.WaitingPid}}</td>
                <td><span class="badge badge-warning">{{.WaitingLockType}}</span></td>
                <td><small>{{.WaitingQuery}}</small></td>
                <td class="text-right"><strong>{{.WaitingQuerySecs}}</strong></td>
                <td><strong>{{.BlockingPid}}</strong></td>
                <td><span class="badge badge-critical">{{.BlockingLockType}}</span></td>
                <td><code><strong style="color: red;">{{.SQLKillBlockingConnection}}</strong></code></td>
            </tr>
            {{else}}
            <tr><td colspan="9">No metadata lock waits currently detected.</td></tr>
            {{end}}
        </tbody>
    </table>

    {{if .DDLLocks}}
    <h3>Blocking Session SQL History</h3>
    <table>
        <thead>
            <tr>
                <th>Blocking PID</th>
                <th>Object Schema</th>
                <th>Object Name</th>
                <th>Blocking Thread ID</th>
                <th>Active Blocking SQL Queries History</th>
            </tr>
        </thead>
        <tbody>
            {{range .DDLLocks}}
            <tr>
                <td><strong>{{.BlockingPid}}</strong></td>
                <td>{{.ObjectSchema}}</td>
                <td><code>{{.ObjectName}}</code></td>
                <td>{{.BlockingThreadID}}</td>
                <td><pre style="margin: 0; padding: 4px; font-size: 10px; max-height: 100px; overflow-y: auto; white-space: pre-wrap; word-break: break-all;">{{.SQLQuery}}</pre></td>
            </tr>
            {{end}}
        </tbody>
    </table>
    {{end}}

    <h3>Active Transactions</h3>
    <table>
        <thead>
            <tr>
                <th>Trx ID</th>
                <th>State</th>
                <th>Started</th>
                <th>Wait Started</th>
                <th>Thread ID</th>
                <th>Isolation</th>
                <th>Tables In Use</th>
                <th>Tables Locked</th>
                <th>Lock Structs</th>
                <th>Rows Locked</th>
                <th>Rows Modified</th>
                <th>Query</th>
            </tr>
        </thead>
        <tbody>
            {{range .InnodbTrx}}
            <tr>
                <td><code>{{.TrxID}}</code></td>
                <td><span class="badge {{if eq .TrxState "RUNNING"}}badge-ok{{else if eq .TrxState "LOCK WAIT"}}badge-critical{{else}}badge-warning{{end}}">{{.TrxState}}</span></td>
                <td>{{.TrxStarted}}</td>
                <td>{{.TrxWaitStarted}}</td>
                <td>{{.TrxMysqlThreadID}}</td>
                <td><small>{{.TrxIsolationLevel}}</small></td>
                <td class="text-right">{{.TrxTablesInUse}}</td>
                <td class="text-right"><strong>{{.TrxTablesLocked}}</strong></td>
                <td class="text-right">{{.TrxLockStructs}}</td>
                <td class="text-right"><strong>{{.TrxRowsLocked}}</strong></td>
                <td class="text-right">{{.TrxRowsModified}}</td>
                <td><small>{{.TrxQuery}}</small></td>
            </tr>
            {{else}}
            <tr><td colspan="12">No active InnoDB transactions currently running.</td></tr>
            {{end}}
        </tbody>
    </table>

    <!-- 8. Major Performance Schema Insights -->
    </div><!-- end sec-body -->
    </div></div><!-- end sec-body/sec-card -->
    <div class="sec-card">
    <div class="sec-hdr" onclick="toggleSec('perf-schema')">
        <div class="sec-hdr-left"><span class="sec-num">9</span><h2 id="perf-schema">Performance Schema Insight</h2></div>
        <button class="sec-toggle" id="btn-perf-schema">▲</button>
    </div>
    <div class="sec-body" id="body-perf-schema">

    <h3>Threads Running Status</h3>
    <table>
        <thead>
            <tr>
                <th>Thread ID</th>
                <th>Name</th>
                <th>Type</th>
                <th>PID</th>
                <th>User</th>
                <th>Host</th>
                <th>DB</th>
                <th>Command</th>
                <th>Time (s)</th>
                <th>State</th>
                <th>Info</th>
            </tr>
        </thead>
        <tbody>
            {{range .PfsThreads}}
            <tr>
                <td>{{.ThreadID}}</td>
                <td><small>{{.Name}}</small></td>
                <td><span class="badge {{if eq .Type "FOREGROUND"}}badge-ok{{else}}badge-warning{{end}}">{{.Type}}</span></td>
                <td>{{.ProcesslistID}}</td>
                <td><strong>{{.ProcesslistUser}}</strong></td>
                <td>{{.ProcesslistHost}}</td>
                <td>{{.ProcesslistDB}}</td>
                <td>{{.ProcesslistCommand}}</td>
                <td class="text-right">{{.ProcesslistTime}}</td>
                <td>{{.ProcesslistState}}</td>
                <td><small>{{.ProcesslistInfo}}</small></td>
            </tr>
            {{else}}
            <tr><td colspan="11">No thread data returned from performance_schema.</td></tr>
            {{end}}
        </tbody>
    </table>
    
    <h3>Live System memory usage</h3>
    <table style="max-width: 650px;">
        <thead>
            <tr>
                <th>Memory Allocation Event Key</th>
                <th>Allocated Space</th>
            </tr>
        </thead>
        <tbody>
            {{range .MemoryEvents}}
            <tr>
                <td><strong>{{.EventName}}</strong></td>
                <td>{{.CurrentAlloc}}</td>
            </tr>
            {{else}}
            <tr><td colspan="2">No memory details returned.</td></tr>
            {{end}}
        </tbody>
    </table>

    <h3>Critical Event Wait Summary</h3>
    <table>
        <thead>
            <tr>
                <th>Wait Event Identifier</th>
                <th>Occurrence Count</th>
                <th>Total Delay Seconds</th>
                <th>Average Delay Wait (ms)</th>
            </tr>
        </thead>
        <tbody>
            {{range .WaitEvents}}
            <tr>
                <td><strong>{{.EventName}}</strong></td>
                <td>{{.CountStar}}</td>
                <td><strong>{{.TotalWaitSec}} s</strong></td>
                <td>{{.AvgWaitMs}} ms</td>
            </tr>
            {{else}}
            <tr><td colspan="4">No delay wait occurrences tracked.</td></tr>
            {{end}}
        </tbody>
    </table>

    <h3>Active Disk File IO Latency Profile</h3>
    <table>
        <thead>
            <tr>
                <th>IO Operation Event</th>
                <th>Total Reads</th>
                <th>Total Writes</th>
                <th>MB Read</th>
                <th>MB Written</th>
                <th>Read Latency (s)</th>
                <th>Write Latency (s)</th>
            </tr>
        </thead>
        <tbody>
            {{range .FileIOEvents}}
            <tr>
                <td><strong>{{.EventName}}</strong></td>
                <td>{{.CountRead}}</td>
                <td>{{.CountWrite}}</td>
                <td>{{.MBRead}} MB</td>
                <td>{{.MBWritten}} MB</td>
                <td>{{.ReadLatency}} s</td>
                <td>{{.WriteLatency}} s</td>
            </tr>
            {{else}}
            <tr><td colspan="7">No active file IO operations registered.</td></tr>
            {{end}}
        </tbody>
    </table>

    <!-- 9. User Details -->
    </div><!-- end sec-body -->
    </div></div><!-- end sec-body/sec-card -->
    <div class="sec-card">
    <div class="sec-hdr" onclick="toggleSec('user-details')">
        <div class="sec-hdr-left"><span class="sec-num">10</span><h2 id="user-details">User Details</h2></div>
        <button class="sec-toggle" id="btn-user-details">▼</button>
    </div>
    <div class="sec-body collapsed" id="body-user-details">
    <table style="max-width: 750px;">
        <thead>
            <tr>
                <th>User</th>
                <th>Host</th>
                <th>Auth Plugin</th>
                <th>Password Expired</th>
            </tr>
        </thead>
        <tbody>
            {{range .UserDetails}}
            <tr>
                <td><strong>{{.User}}</strong></td>
                <td><code>{{.Host}}</code></td>
                <td>{{.Plugin}}</td>
                <td>
                    {{if eq .PasswordExpired "Y"}}
                        <span class="badge badge-critical">YES</span>
                    {{else}}
                        <span class="badge badge-ok">NO</span>
                    {{end}}
                </td>
            </tr>
            {{else}}
            <tr><td colspan="4">No user records returned from mysql.user.</td></tr>
            {{end}}
        </tbody>
    </table>

    <!-- 10. Recommendations -->
    </div><!-- end sec-body -->
    </div></div><!-- end sec-body/sec-card -->
    <div class="sec-card">
    <div class="sec-hdr" onclick="toggleSec('recommendations')">
        <div class="sec-hdr-left"><span class="sec-num">11</span><h2 id="recommendations">Recommendations</h2></div>
        <button class="sec-toggle" id="btn-recommendations">▲</button>
    </div>
    <div class="sec-body" id="body-recommendations">
    <p>Automated database diagnostic evaluations assessed against current active metrics:</p>
    
    {{range .Recommendations}}
    <div class="recommendation-card rec-{{.Type}}">
        <p><strong>[{{.Type}}] {{.Parameter}}</strong></p>
        <p style="margin: 0;">{{.Description}}</p>
    </div>
    {{else}}
    <p>No operational mismatches or threshold flags detected on this collection run.</p>
    {{end}}

    </div></div><!-- end sec-body/sec-card -->

    </div><!-- end .content -->

    <footer>
        mysql_gather &nbsp;|&nbsp; Database Health Report &nbsp;|&nbsp; Copyright (c) 2026 aniljoshi2022 &nbsp;|&nbsp; MIT License 
    </footer>

    <script>
        function filterTable(tableId, inputId) {
            var input = document.getElementById(inputId);
            var filter = input.value.toUpperCase();
            var table = document.getElementById(tableId);
            if (!table) return;
            var trs = table.getElementsByTagName("tr");

            for (var i = 1; i < trs.length; i++) {
                var match = false;
                var tds = trs[i].getElementsByTagName("td");
                for (var j = 0; j < tds.length; j++) {
                    if (tds[j]) {
                        var textVal = tds[j].textContent || tds[j].innerText;
                        if (textVal.toUpperCase().indexOf(filter) > -1) {
                            match = true;
                            break;
                        }
                    }
                }
                trs[i].style.display = match ? "" : "none";
            }
        }
    </script>

<script>
function toggleSec(id) {
    var body = document.getElementById('body-' + id);
    var btn  = document.getElementById('btn-'  + id);
    if (!body) return;
    if (body.classList.contains('collapsed')) {
        body.classList.remove('collapsed');
        btn.textContent = '▲';
    } else {
        body.classList.add('collapsed');
        btn.textContent = '▼';
    }
}
</script>
</body>
</html>`
