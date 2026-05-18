package main

import (
	"database/sql"
	"flag"
	"fmt"
	"html/template"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

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
	ChannelName       string
	ReplicaIORunning  string
	ReplicaSQLRunning string
	SourceHost        string
	SecondsBehind     string
	LastIOError       string
	LastSQLError      string
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
	Digest                 string
	SchemaName             string
	QuerySample            string
	ExecCount              string
	TotalExecSec           string
	AvgExecMs              string
	TotalLockSec           string
	RowsExamined           string
	RowsSent               string
	CreatedTmpTables       string
	CreatedTmpDiskTables   string
	SortRows               string
	NoIndexUsed            string
	NoGoodIndexUsed        string
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
	ThreadID    string
	Name        string
	Type        string
	ProcesslistID   string
	ProcesslistUser string
	ProcesslistHost string
	ProcesslistDB   string
	ProcesslistCommand string
	ProcesslistTime string
	ProcesslistState string
	ProcesslistInfo string
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

// PageData holds all variables injected into the HTML template
type PageData struct {
	Summary              SummaryInfo
	EngineMetrics        []KeyVal
	ConfigVariables      []KeyVal
	StatusCounters       []KeyVal
	Processes            []ProcessInfo
	GroupMembers         []GroupMember
	ReplicationStates    []ReplicationStatus
	GRQueues             []GRMemberStats
	GRFlowControlLimit   string
	GaleraStatus         []KeyVal
	GaleraFlowControl    string
	GaleraQueues         GaleraQueueStats
	MasterStatus         []KeyVal
	InnodbStatus         string
	MemoryEvents         []MemoryEvent
	WaitEvents           []WaitEvent
	FileIOEvents         []FileIOEvent
	ClusterStatus        string
	ClusterSetStatus     string
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
	
	var ro int
	if err := db.QueryRow("SELECT @@global.read_only;").Scan(&ro); err == nil {
		if ro == 1 {
			data.Summary.ReadOnly = "True (Read Only)"
		} else {
			data.Summary.ReadOnly = "False (Read/Write)"
		}
	}

	var uptimeSeconds int64
	_ = db.QueryRow("SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Uptime';").Scan(&uptimeSeconds)
	if uptimeSeconds > 0 {
		data.Summary.UptimeSec = fmt.Sprintf("%d", uptimeSeconds)
		days := uptimeSeconds / 86400
		hours := (uptimeSeconds % 86400) / 3600
		minutes := (uptimeSeconds % 3600) / 60
		data.Summary.Uptime = fmt.Sprintf("%dd %dh %dm", days, hours, minutes)
	}

	_ = db.QueryRow("SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Threads_connected';").Scan(&data.Summary.Threads)
	_ = db.QueryRow("SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Questions';").Scan(&data.Summary.Questions)
	_ = db.QueryRow("SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Slow_queries';").Scan(&data.Summary.SlowQueries)
	_ = db.QueryRow("SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Opened_tables';").Scan(&data.Summary.Opens)
	_ = db.QueryRow("SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Flush_commands';").Scan(&data.Summary.FlushTables)
	_ = db.QueryRow("SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Open_tables';").Scan(&data.Summary.OpenTables)
	
	if uptimeSeconds > 0 && data.Summary.Questions != "" {
		qCount, _ := strconv.ParseFloat(data.Summary.Questions, 64)
		data.Summary.QueriesPerSec = fmt.Sprintf("%.3f", qCount/float64(uptimeSeconds))
	} else {
		data.Summary.QueriesPerSec = "0.000"
	}

	variablesMap := make(map[string]string)
	statusMap := make(map[string]string)

	// Section 1: Consolidated Engine Metrics (Union All query as requested)
	metricsQuery := `
		SELECT 'Checkpoint Age' AS Metric, ROUND(COUNT / 1024 / 1024, 2) AS Value
		FROM information_schema.innodb_metrics WHERE NAME = 'log_lsn_checkpoint_age'
		UNION ALL
		SELECT 'History list length' AS Metric, COUNT AS Value 
		FROM information_schema.innodb_metrics WHERE NAME = 'trx_rseg_history_len'
		UNION ALL
		SELECT 'Pending normal aio reads', VARIABLE_VALUE 
		FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Innodb_data_pending_reads'
		UNION ALL
		SELECT 'Pending normal aio writes', VARIABLE_VALUE 
		FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Innodb_data_pending_writes'
		UNION ALL
		SELECT 'Pending flushes (log)', VARIABLE_VALUE 
		FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Innodb_os_log_pending_writes'
		UNION ALL
		SELECT 'Pending flushes (buffer pool)', VARIABLE_VALUE 
		FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Innodb_buffer_pool_pages_flushed'
		UNION ALL
		SELECT 'Ibuf:size', COUNT 
		FROM information_schema.innodb_metrics WHERE NAME = 'ibuf_size'
		UNION ALL
		SELECT 'Queries inside InnoDB', VARIABLE_VALUE 
		FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Innodb_thread_active'
		UNION ALL
		SELECT 'Queries in queue', VARIABLE_VALUE 
		FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Innodb_thread_queue'
		UNION ALL
		SELECT 'Total large memory allocated', VARIABLE_VALUE 
		FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Innodb_buffer_pool_bytes_data';`

	if rows, err := db.Query(metricsQuery); err == nil {
		for rows.Next() {
			var kv KeyVal
			if err := rows.Scan(&kv.Key, &kv.Value); err == nil {
				statusMap[strings.ToLower(kv.Key)] = kv.Value
				if kv.Key == "Total large memory allocated" {
					kv.Value = formatBytes(kv.Value)
				}
				data.EngineMetrics = append(data.EngineMetrics, kv)
			}
		}
		rows.Close()
	}

	// Section 2: SHOW GLOBAL VARIABLES (formatted to human-readable)
	if rows, err := db.Query("SHOW GLOBAL VARIABLES;"); err == nil {
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
	if rows, err := db.Query("SHOW GLOBAL STATUS;"); err == nil {
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

	// Section 4: SHOW ENGINE INNODB STATUS Raw Text
	var engine string
	var statusText string
	if err := db.QueryRow("SHOW ENGINE INNODB STATUS;").Scan(&engine, &statusText, &statusText); err == nil {
		data.InnodbStatus = statusText
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
					case "SECONDS_BEHIND_SOURCE", "SECONDS_BEHIND_MASTER":
						status.SecondsBehind = valStr
					case "LAST_IO_ERROR":
						status.LastIOError = valStr
					case "LAST_SQL_ERROR":
						status.LastSQLError = valStr
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
		WHERE VARIABLE_NAME in ('wsrep_incoming_addresses','wsrep_cluster_size','wsrep_cluster_status');`
	if rows, err := db.Query(galeraQuery); err == nil {
		for rows.Next() {
			var kv KeyVal
			if err := rows.Scan(&kv.Key, &kv.Value); err == nil {
				data.GaleraStatus = append(data.GaleraStatus, kv)
				isClusterConfigured = true
			}
		}
		rows.Close()
	}

	// Final Summary Cluster Flow Control assignment
	if !isClusterConfigured {
		data.Summary.ClusterFlowControl = "Not Configured / Single Instance"
	} else {
		data.Summary.ClusterFlowControl = clusterFCStatus
	}

	// Fetch JSON configurations from mysql_innodb_cluster metadata schemas
	var clusterNameVal string
	err = db.QueryRow("SELECT cluster_name FROM mysql_innodb_cluster_metadata.v2_clusters LIMIT 1;").Scan(&clusterNameVal)
	if err != nil {
		err = db.QueryRow("SELECT cluster_name FROM mysql_innodb_cluster.clusters LIMIT 1;").Scan(&clusterNameVal)
	}
	if clusterNameVal == "" {
		clusterNameVal = "testcluster"
	}

	var clusterJSON string
	clusterQueries := []string{
		"SELECT JSON_PRETTY(status) FROM mysql_innodb_cluster_metadata.v2_clusters LIMIT 1;",
		"SELECT status FROM mysql_innodb_cluster_metadata.v2_clusters LIMIT 1;",
		"SELECT JSON_PRETTY(status) FROM mysql_innodb_cluster.clusters LIMIT 1;",
		"SELECT status FROM mysql_innodb_cluster.clusters LIMIT 1;",
	}
	for _, q := range clusterQueries {
		if err := db.QueryRow(q).Scan(&clusterJSON); err == nil && clusterJSON != "" {
			data.ClusterStatus = clusterJSON
			break
		}
	}

	var clusterSetJSON string
	clusterSetQueries := []string{
		"SELECT JSON_PRETTY(status) FROM mysql_innodb_cluster_metadata.v2_clustersets LIMIT 1;",
		"SELECT status FROM mysql_innodb_cluster_metadata.v2_clustersets LIMIT 1;",
		"SELECT JSON_PRETTY(status) FROM mysql_innodb_cluster.clustersets LIMIT 1;",
		"SELECT status FROM mysql_innodb_cluster.clustersets LIMIT 1;",
	}
	for _, q := range clusterSetQueries {
		if err := db.QueryRow(q).Scan(&clusterSetJSON); err == nil && clusterSetJSON != "" {
			data.ClusterSetStatus = clusterSetJSON
			break
		}
	}

	// Final Router metadata extraction using dynamic JOIN queries
	data.Routers = queryRouterMetadata(db)

	// Section 6: Current Binary Log Status
	binlogRows, bErr := db.Query("SHOW BINARY LOG STATUS;")
	if bErr != nil {
		binlogRows, bErr = db.Query("SHOW MASTER STATUS;")
	}
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
					data.MasterStatus = append(data.MasterStatus, KeyVal{Key: col, Value: string(values[i])})
				}
			}
		}
		binlogRows.Close()
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
					Type:      "WARNING",
					Parameter: "thread_cache_size (Current: " + threadCacheSizeStr + ")",
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
					Type:      "WARNING",
					Parameter: "tmp_table_size & max_heap_table_size",
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
				Type:      "WARNING",
				Parameter: "sort_buffer_size",
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
					Type:      "CRITICAL",
					Parameter: "innodb_buffer_pool_size",
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
				Type:      "WARNING",
				Parameter: "innodb_log_file_size",
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
				Type:      "WARNING",
				Parameter: "table_open_cache (Current Limit: " + openCacheStr + ")",
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
					Type:      "CRITICAL",
					Parameter: "max_connections",
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
				Type:      "CRITICAL",
				Parameter: "Group Replication Flow Control (Certifier Queue)",
				Description: fmt.Sprintf("Certifier transaction queue size (%d) exceeds flow control threshold limit (%d) on node %s. Flow control triggers are active, throttling primary write rates.", q.CertQueue, certThreshold, q.MemberID),
			})
		}
		if q.ApplierQueue > applierThreshold {
			data.Recommendations = append(data.Recommendations, Recommendation{
				Type:      "CRITICAL",
				Parameter: "Group Replication Flow Control (Applier Queue)",
				Description: fmt.Sprintf("Applier queue size (%d) exceeds flow control threshold limit (%d) on node %s. Flow control triggers are active, throttling primary write rates.", q.ApplierQueue, applierThreshold, q.MemberID),
			})
		}
	}

	// Rule 9: Galera / PXC Flow Control Warnings
	if data.GaleraFlowControl != "" {
		fcStatus, _ := strconv.ParseFloat(data.GaleraFlowControl, 64)
		if fcStatus > 0.1 {
			data.Recommendations = append(data.Recommendations, Recommendation{
				Type:      "WARNING",
				Parameter: "wsrep_flow_control_status",
				Description: fmt.Sprintf("PXC/Galera Cluster Flow Control is active (%.2f%% of time spent paused). Secondary nodes are falling behind, causing replication write stalls.", fcStatus*100.0),
			})
		}
	}

	// Check History List Length purge metric
	for _, m := range data.EngineMetrics {
		if m.Key == "History list length" {
			hll := getRawInt(m.Value)
			if hll > 200000 {
				data.Recommendations = append(data.Recommendations, Recommendation{
					Type:      "CRITICAL",
					Parameter: "trx_rseg_history_len",
					Description: fmt.Sprintf("History list length is extremely high (%d blocks). Your InnoDB transaction purge workers cannot clean old undo logs. Investigate and terminate long-running active transactions.", hll),
				})
			}
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

	tmpl, err := template.New("report").Parse(htmlTemplate)
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
    <title>MySQL Gather Report</title>
    <style>
        body {
            font-family: Menlo, Monaco, Consolas, "Courier New", monospace, -apple-system, BlinkMacSystemFont, sans-serif;
            font-size: 12px;
            line-height: 1.4;
            color: #000;
            background-color: #fff;
            margin: 15px;
        }
        h1 {
            font-size: 20px;
            font-weight: bold;
            color: #000;
            margin: 0 0 5px 0;
            border-bottom: 2px solid #000;
            padding-bottom: 5px;
        }
        h2 {
            font-size: 15px;
            font-weight: bold;
            color: #1a365d;
            margin: 25px 0 10px 0;
            border-bottom: 1.5px solid #000;
            padding-bottom: 2px;
        }
        h3 {
            font-size: 13px;
            margin: 15px 0 5px 0;
            color: #2b6cb0;
        }
        p {
            margin: 0 0 10px 0;
        }
        a {
            color: #0044cc;
            text-decoration: none;
        }
        a:hover {
            text-decoration: underline;
        }
        ul.sections-list {
            padding-left: 0;
            list-style: none;
            margin: 10px 0 20px 0;
        }
        ul.sections-list li {
            display: inline;
            margin-right: 15px;
        }
        ul.sections-list li::after {
            content: " |";
            color: #999;
            margin-left: 10px;
        }
        ul.sections-list li:last-child::after {
            content: "";
        }
        table {
            border-collapse: collapse;
            width: 100%;
            margin-bottom: 20px;
            font-size: 11px;
            border: 1px solid #777;
        }
        th, td {
            border: 1px solid #aaa;
            padding: 4px 6px;
            text-align: left;
            vertical-align: top;
        }
        th {
            background-color: #d1e2ff;
            font-weight: bold;
            color: #000;
        }
        tr:nth-child(even) {
            background-color: #f6f9fe;
        }
        pre {
            background-color: #f9f9f9;
            border: 1px dashed #777;
            padding: 8px;
            font-family: Menlo, Monaco, Consolas, "Courier New", monospace;
            font-size: 11px;
            overflow-x: auto;
            white-space: pre-wrap;
            word-wrap: break-word;
            margin: 10px 0;
        }
        .text-right {
            text-align: right;
        }
        .badge {
            display: inline-block;
            padding: 1px 4px;
            font-weight: bold;
            font-size: 9px;
            border-radius: 2px;
            text-transform: uppercase;
        }
        .badge-critical {
            background-color: #ffd8d8;
            color: #900;
            border: 1px solid #f88;
        }
        .badge-warning {
            background-color: #ffe8b8;
            color: #850;
            border: 1px solid #e2a050;
        }
        .badge-ok {
            background-color: #d5ffd5;
            color: #060;
            border: 1px solid #8e8;
        }
        .search-box {
            width: 100%;
            max-width: 300px;
            padding: 3px 6px;
            border: 1px solid #777;
            font-size: 11px;
            font-family: inherit;
            margin-bottom: 5px;
        }
        footer {
            margin-top: 40px;
            font-size: 10px;
            color: #555;
            border-top: 1px solid #999;
            padding-top: 5px;
        }
        .recommendation-card {
            border-left: 4px solid #aaa;
            padding-left: 10px;
            margin-bottom: 12px;
        }
        .rec-CRITICAL {
            border-left-color: #d9534f;
            background-color: #fff5f5;
        }
        .rec-WARNING {
            border-left-color: #f0ad4e;
            background-color: #fcf8e3;
        }
        .rec-INFO {
            border-left-color: #5bc0de;
            background-color: #f4f9fa;
        }
    </style>
</head>
<body>

    <h1>🐬 MySQL Gather Report</h1>
    
    <div id="Sections">
        <strong>Sections:</strong>
        <ul class="sections-list">
            <li><a href="#summary">1. System Summary</a></li>
            <li><a href="#config">2. Critical Configurations</a></li>
            <li><a href="#status">3. Performance Metrics</a></li>
            <li><a href="#innodb">4. Storage Engine State</a></li>
            <li><a href="#replication">5. HA &amp; Replication Topology</a></li>
            <li><a href="#replica-source">6. Current Binary Log Status</a></li>
            <li><a href="#process">7. Process List &amp; Query History</a></li>
            <li><a href="#perf-schema">8. Performance Schema Insights</a></li>
            <li><a href="#user-details">9. User Details</a></li>
            <li><a href="#recommendations">10. Optimization Recommendations</a></li>
        </ul>
    </div>

    <!-- 1. System Summary & Engine Metrics -->
    <h2 id="summary">1. System Summary</h2>
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
            <td><strong style="color: #c53030;">{{.Summary.ClusterFlowControl}}</strong></td>
        </tr>
    </table>

    <h3>📊 mysqladmin status Metrics</h3>
    <table style="max-width: 800px; margin-bottom: 20px;">
        <tr>
            <th width="25%">Threads Connected</th>
            <td>{{.Summary.Threads}}</td>
            <th width="20%">Total Questions</th>
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

    <h3>📊 InnoDB Core Engine Metrics</h3>
    <table style="max-width: 750px;">
        <thead>
            <tr>
                <th width="65%">Metric Identifier</th>
                <th>Telemetry Value</th>
            </tr>
        </thead>
        <tbody>
            {{range .EngineMetrics}}
            <tr>
                <td><strong>{{.Key}}</strong></td>
                <td>{{.Value}}</td>
            </tr>
            {{else}}
            <tr><td colspan="2">No engine metrics collected.</td></tr>
            {{end}}
        </tbody>
    </table>

    <!-- 2. Critical Configurations -->
    <h2 id="config">2. Critical Configurations (SHOW GLOBAL VARIABLES)</h2>
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
    <h2 id="status">3. Performance Metrics (SHOW GLOBAL STATUS)</h2>
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
    <h2 id="innodb">4. Storage Engine State (SHOW ENGINE INNODB STATUS)</h2>
    <pre>{{.InnodbStatus}}</pre>

    <!-- 5. HA / Replication Topology Consolidated -->
    <h2 id="replication">5. HA &amp; Replication Topology</h2>
    
    <h3>🧬 Replication Slave / Replica Channels Status</h3>
    <table>
        <thead>
            <tr>
                <th>Channel Name</th>
                <th>IO Running</th>
                <th>SQL Running</th>
                <th>Source Host</th>
                <th>Seconds Behind</th>
                <th>Last IO Error</th>
                <th>Last SQL Error</th>
            </tr>
        </thead>
        <tbody>
            {{range .ReplicationStates}}
            <tr>
                <td><strong>{{.ChannelName}}</strong></td>
                <td>{{.ReplicaIORunning}}</td>
                <td>{{.ReplicaSQLRunning}}</td>
                <td>{{.SourceHost}}</td>
                <td><strong>{{.SecondsBehind}}</strong></td>
                <td><small>{{.LastIOError}}</small></td>
                <td><small>{{.LastSQLError}}</small></td>
            </tr>
            {{else}}
            <tr><td colspan="7">No active replica replication states are configured.</td></tr>
            {{end}}
        </tbody>
    </table>

    <h3>👥 Group Replication Members</h3>
    <table>
        <thead>
            <tr>
                <th>Channel Name</th>
                <th>Member ID UUID</th>
                <th>Member Host</th>
                <th>Port</th>
                <th>State</th>
                <th>Role</th>
                <th>Version</th>
            </tr>
        </thead>
        <tbody>
            {{range .GroupMembers}}
            <tr>
                <td>{{.ChannelName}}</td>
                <td><small>{{.MemberID}}</small></td>
                <td><strong>{{.MemberHost}}</strong></td>
                <td>{{.MemberPort}}</td>
                <td>
                    {{if eq .MemberState "ONLINE"}}
                        <span class="badge badge-ok">ONLINE</span>
                    {{else}}
                        <span class="badge badge-critical">{{.MemberState}}</span>
                    </td>
                {{end}}
                <td><strong>{{.MemberRole}}</strong></td>
                <td>{{.Version}}</td>
            </tr>
            {{else}}
            <tr><td colspan="7">No group replication members detected.</td></tr>
            {{end}}
        </tbody>
    </table>

    {{if or .GRFlowControlLimit .GRQueues}}
    <h3>📊 Group Replication Queues</h3>
    {{if .GRFlowControlLimit}}
    <p><strong>{{.GRFlowControlLimit}}</strong></p>
    {{end}}
    <table>
        <thead>
            <tr>
                <th>Member ID UUID</th>
                <th>Certifier Queue Size (cert_queue)</th>
                <th>Applier Queue Size (applier_queue)</th>
            </tr>
        </thead>
        <tbody>
            {{range .GRQueues}}
            <tr>
                <td><code>{{.MemberID}}</code></td>
                <td><strong>{{.CertQueue}}</strong></td>
                <td><strong>{{.ApplierQueue}}</strong></td>
            </tr>
            {{else}}
            <tr><td colspan="3">No active member stats queues registered.</td></tr>
            {{end}}
        </tbody>
    </table>
    {{end}}

    {{if or .GaleraFlowControl .GaleraStatus}}
    <h3>🛡️ Galera / Percona XtraDB Cluster (PXC) Flow Control &amp; Status Checks</h3>
    {{if .GaleraFlowControl}}
    <p><strong>wsrep_flow_control_status:</strong> <code>{{.GaleraFlowControl}}</code></p>
    {{end}}
    
    {{if .GaleraQueues.RecvQueue}}
    <table style="max-width:650px; margin-bottom:15px;">
        <thead>
            <tr>
                <th>Galera Recv Queue (local_recv_queue)</th>
                <th>Galera Send Queue (local_send_queue)</th>
                <th>Flow Control Paused Fraction (flow_control_paused)</th>
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

    <table>
        <thead>
            <tr>
                <th>Galera Cluster Status Parameter</th>
                <th>State Value</th>
            </tr>
        </thead>
        <tbody>
            {{range .GaleraStatus}}
            <tr>
                <td><strong>{{.Key}}</strong></td>
                <td><code>{{.Value}}</code></td>
            </tr>
            {{else}}
            <tr><td colspan="2">No live Galera status variables found.</td></tr>
            {{end}}
        </tbody>
    </table>
    {{end}}

    {{if or .ClusterStatus .ClusterSetStatus .Routers}}
    <h3>🛡️ InnoDB Clusters &amp; ClusterSet Metadata Details</h3>
    {{if .ClusterStatus}}
    <p>InnoDB Cluster Status (cluster.status):</p>
    <pre>{{.ClusterStatus}}</pre>
    {{end}}
    
    {{if .ClusterSetStatus}}
    <p>InnoDB ClusterSet Status (myclusterset.status):</p>
    <pre>{{.ClusterSetStatus}}</pre>
    {{end}}

    {{if .Routers}}
    <h3>🛡️ Registered MySQLRouter</h3>
    <table>
        <thead>
            <tr>
                <th>Router ID</th>
                <th>Router Name</th>
                <th>Label</th>
                <th>Address</th>
                <th>Version</th>
                <th>Last Check-In</th>
                <th>Endpoints (RW/RO/RWX/ROX/Split)</th>
                <th>Metadata User</th>
                <th>Targets</th>
            </tr>
        </thead>
        <tbody>
            {{range .Routers}}
            <tr>
                <td><strong>{{.RouterID}}</strong></td>
                <td>{{.RouterName}}</td>
                <td><small>{{.RouterLabel}}</small></td>
                <td><code>{{.Address}}</code></td>
                <td>{{.Version}}</td>
                <td><small>{{.LastCheckIn}}</small></td>
                <td>
                    <ul style="margin:0; padding-left:15px; font-family:monospace; font-size:10px;">
                        <li>RW: {{.RWPort}}</li>
                        <li>RO: {{.ROPort}}</li>
                        <li>RWX: {{.RWXPort}}</li>
                        <li>ROX: {{.ROXPort}}</li>
                        <li>Split: {{.RWSplitPort}}</li>
                    </ul>
                </td>
                <td><code>{{.MetadataUser}}</code></td>
                <td><span class="badge badge-ok">{{.ReadOnlyTargets}}</span></td>
            </tr>
            {{end}}
        </tbody>
    </table>
    {{end}}
    {{end}}

    <!-- 6. Current Binary Log Status -->
    <h2 id="replica-source">6. Current Binary Log Status</h2>
    <table style="max-width: 600px;">
        <thead>
            <tr>
                <th>Coordinate Name</th>
                <th>Current Position / File</th>
            </tr>
        </thead>
        <tbody>
            {{range .MasterStatus}}
            <tr>
                <td><strong>{{.Key}}</strong></td>
                <td>{{.Value}}</td>
            </tr>
            {{else}}
            <tr><td colspan="2">Local binary logging parameters are inactive or master status is empty.</td></tr>
            {{end}}
        </tbody>
    </table>

    <!-- 7. Process List & Query History -->
    <h2 id="process">7. Process List &amp; Query History</h2>
    
    <h3>🖥️ Active Connections Thread Status (SHOW FULL PROCESSLIST)</h3>
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

    <h3>📈 Historical Statement Summary Digests (Top Query Stats)</h3>
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

    <h3>🔒 Lock Waits &amp; Blocking Transactions</h3>
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

    <h3>🔒 MDL &amp; DDL Lock Waits</h3>
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
    <h3>🔒 Blocking Session SQL History</h3>
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

    <h3>⚡ Active Transactions (information_schema.innodb_trx)</h3>
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
    <h2 id="perf-schema">8. Performance Schema Insights</h2>

    <h3>🧵 Performance Schema Threads</h3>
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
    <h2 id="user-details">9. User Details</h2>
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
    <h2 id="recommendations">10. Optimization Recommendations</h2>
    <p>Automated database diagnostic evaluations assessed against current active metrics:</p>
    
    {{range .Recommendations}}
    <div class="recommendation-card rec-{{.Type}}">
        <p><strong>[{{.Type}}] {{.Parameter}}</strong></p>
        <p style="margin: 0;">{{.Description}}</p>
    </div>
    {{else}}
    <p>No operational mismatches or threshold flags detected on this collection run.</p>
    {{end}}

    <footer>
        <p>MySQL Gather Diagnostic Report | Inspired by the pg_gather philosophy for clean, rapid DB checks.</p>
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
</body>
</html>`
