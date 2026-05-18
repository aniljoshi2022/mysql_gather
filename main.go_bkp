package main

import (
	"database/sql"
	"flag"
	"fmt"
	"html/template"
	"log"
	"os"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// ─── Data Structures ────────────────────────────────────────────────────────

type ServerSummary struct {
	CollectedAt    string
	Hostname       string
	Version        string
	VersionComment string
	Uptime         string
	UptimeHuman    string
	CurrentUser    string
	DataDir        string
	Port           string
	OS             string
	Arch           string
	CharSet        string
	Timezone       string
}

type GlobalVariable struct {
	Name  string
	Value string
	Note  string
	Alert bool
}

type GlobalStatus struct {
	Name  string
	Value string
	Note  string
	Alert bool
}

type ProcessListRow struct {
	ID      string
	User    string
	Host    string
	DB      string
	Command string
	Time    string
	State   string
	Info    string
	Alert   bool
}

type ReplicaStatus struct {
	Available          bool
	IORunning          string
	SQLRunning         string
	SecondsBehind      string
	MasterHost         string
	MasterPort         string
	ReplicateDoDBs     string
	LastIOError        string
	LastSQLError       string
	IOAlert            bool
	SQLAlert           bool
	LagAlert           bool
	ReplicaIOState     string
	ReplicaSQLState    string
	MasterLogFile      string
	ReadMasterLogPos   string
	RelayLogFile       string
	ExecMasterLogPos   string
	AutoPosition       string
}

type MasterStatus struct {
	Available bool
	File      string
	Position  string
	Binlog    string
	GTIDSet   string
}

type InnoDBStatus struct {
	Raw      string
	Sections []InnoDBSection
}

type InnoDBSection struct {
	Title string
	KVs   []KV
}

type KV struct {
	Key   string
	Value string
	Alert bool
}

type WaitEvent struct {
	Event     string
	Count     string
	TotalSec  string
	AvgMS     string
	Alert     bool
}

type FileIORow struct {
	Event         string
	Reads         string
	Writes        string
	MBRead        string
	MBWritten     string
	ReadLatency   string
	WriteLatency  string
}

type CPUQueryRow struct {
	Query      string
	Executions string
	TotalCPU   string
	AvgCPUMS   string
}

type MemoryRow struct {
	EventName    string
	CurrentAlloc string
	Alert        bool
}

type LockRow struct {
	WaitingQuery    string
	WaitingThread   string
	BlockingThread   string
	BlockingQuery   string
	LockType        string
	LockMode        string
}

type TableIORow struct {
	Schema        string
	Table         string
	Reads         string
	Writes        string
	LatencyRead   string
	LatencyWrite  string
}

type GatherData struct {
	Summary       ServerSummary
	Variables     []GlobalVariable
	Status        []GlobalStatus
	InnoDB        InnoDBStatus
	Replica       ReplicaStatus
	Master        MasterStatus
	ProcessList   []ProcessListRow
	WaitEvents    []WaitEvent
	FileIO        []FileIORow
	CPUQueries    []CPUQueryRow
	Memory        []MemoryRow
	Locks         []LockRow
	TableIO       []TableIORow
	CollectErrors []string
}

// ─── Main ────────────────────────────────────────────────────────────────────

func main() {
	host := flag.String("host", "127.0.0.1", "MySQL host")
	port := flag.String("port", "3306", "MySQL port")
	user := flag.String("user", "root", "MySQL user")
	password := flag.String("password", "", "MySQL password")
	output := flag.String("output", "mysql_gather.html", "Output HTML file")
	flag.Parse()

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/?timeout=10s&readTimeout=30s&parseTime=true",
		*user, *password, *host, *port)

	db2, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("Failed to open DB: %v", err)
	}
	defer db2.Close()
	db2.SetMaxOpenConns(5)
	db2.SetConnMaxLifetime(60 * time.Second)

	if err := db2.Ping(); err != nil {
		log.Fatalf("Cannot connect to MySQL: %v\n\nCheck your connection parameters.", err)
	}

	fmt.Println("Connected to MySQL. Collecting diagnostics...")

	data := GatherData{}

	data.Summary = collectSummary(db2, *host, *port)
	data.Variables = collectVariables(db2, &data)
	data.Status = collectStatus(db2, &data)
	data.InnoDB = collectInnoDB(db2, &data)
	data.Replica = collectReplica(db2, &data)
	data.Master = collectMaster(db2, &data)
	data.ProcessList = collectProcessList(db2, &data)
	data.WaitEvents = collectWaitEvents(db2, &data)
	data.FileIO = collectFileIO(db2, &data)
	data.CPUQueries = collectCPUQueries(db2, &data)
	data.Memory = collectMemory(db2, &data)
	data.Locks = collectLocks(db2, &data)
	data.TableIO = collectTableIO(db2, &data)

	fmt.Println("Data collected. Generating HTML report...")

	f, err := os.Create(*output)
	if err != nil {
		log.Fatalf("Cannot create output file: %v", err)
	}
	defer f.Close()

	tmpl, err := template.New("report").Funcs(template.FuncMap{
		"safeHTML": func(s string) template.HTML { return template.HTML(s) },
		"truncate": func(s string, n int) string {
			if len(s) <= n {
				return s
			}
			return s[:n] + "…"
		},
	}).Parse(htmlTemplate)
	if err != nil {
		log.Fatalf("Template parse error: %v", err)
	}

	if err := tmpl.Execute(f, data); err != nil {
		log.Fatalf("Template execute error: %v", err)
	}

	fmt.Printf("\n✓ Report written to: %s\n", *output)
}

// ─── Collectors ──────────────────────────────────────────────────────────────

func collectSummary(db *sql.DB, host, port string) ServerSummary {
	s := ServerSummary{
		CollectedAt: time.Now().Format("2006-01-02 15:04:05 MST"),
		Hostname:    host,
		Port:        port,
	}
	vars := queryKV(db, "SHOW GLOBAL VARIABLES")
	status := queryKV(db, "SHOW GLOBAL STATUS")

	s.Version = vars["version"]
	s.VersionComment = vars["version_comment"]
	s.DataDir = vars["datadir"]
	s.OS = vars["version_compile_os"]
	s.Arch = vars["version_compile_machine"]
	s.CharSet = vars["character_set_server"]
	s.Timezone = vars["time_zone"]

	if uptimeSec, ok := status["Uptime"]; ok {
		s.Uptime = uptimeSec + "s"
		s.UptimeHuman = formatUptime(uptimeSec)
	}

	_ = db.QueryRow("SELECT CURRENT_USER()").Scan(&s.CurrentUser)
	return s
}

func collectVariables(db *sql.DB, data *GatherData) []GlobalVariable {
	vars := queryKV(db, "SHOW GLOBAL VARIABLES")
	tracked := []struct {
		key   string
		label string
	}{
		{"max_connections", "Max Connections"},
		{"innodb_buffer_pool_size", "InnoDB Buffer Pool Size"},
		{"innodb_buffer_pool_instances", "Buffer Pool Instances"},
		{"innodb_log_file_size", "InnoDB Log File Size"},
		{"innodb_flush_log_at_trx_commit", "Flush Log at Trx Commit"},
		{"innodb_flush_method", "InnoDB Flush Method"},
		{"query_cache_size", "Query Cache Size"},
		{"query_cache_type", "Query Cache Type"},
		{"wait_timeout", "Wait Timeout"},
		{"interactive_timeout", "Interactive Timeout"},
		{"tmp_table_size", "Tmp Table Size"},
		{"max_heap_table_size", "Max Heap Table Size"},
		{"thread_cache_size", "Thread Cache Size"},
		{"table_open_cache", "Table Open Cache"},
		{"sort_buffer_size", "Sort Buffer Size"},
		{"join_buffer_size", "Join Buffer Size"},
		{"read_buffer_size", "Read Buffer Size"},
		{"read_rnd_buffer_size", "Read RND Buffer Size"},
		{"key_buffer_size", "Key Buffer Size"},
		{"slow_query_log", "Slow Query Log"},
		{"long_query_time", "Long Query Time"},
		{"general_log", "General Log"},
		{"binlog_format", "Binlog Format"},
		{"sync_binlog", "Sync Binlog"},
		{"expire_logs_days", "Expire Logs Days"},
		{"binlog_expire_logs_seconds", "Binlog Expire Seconds"},
		{"log_bin", "Binary Logging"},
		{"gtid_mode", "GTID Mode"},
		{"sql_mode", "SQL Mode"},
		{"max_allowed_packet", "Max Allowed Packet"},
	}

	var result []GlobalVariable
	for _, t := range tracked {
		val, ok := vars[t.key]
		if !ok {
			val = "N/A"
		}
		gv := GlobalVariable{Name: t.label, Value: formatVarValue(t.key, val)}
		switch t.key {
		case "query_cache_size":
			if val != "0" && val != "N/A" {
				gv.Note = "Query cache deprecated; consider disabling"
				gv.Alert = true
			}
		case "innodb_flush_log_at_trx_commit":
			if val == "0" || val == "2" {
				gv.Note = "Non-durable setting; risk of data loss on crash"
				gv.Alert = true
			}
		case "sync_binlog":
			if val == "0" {
				gv.Note = "sync_binlog=0 risks binlog loss on crash"
				gv.Alert = true
			}
		case "slow_query_log":
			if val == "OFF" || val == "0" {
				gv.Note = "Slow query log is disabled"
			}
		}
		result = append(result, gv)
	}
	return result
}

func collectStatus(db *sql.DB, data *GatherData) []GlobalStatus {
	status := queryKV(db, "SHOW GLOBAL STATUS")
	vars := queryKV(db, "SHOW GLOBAL VARIABLES")

	tracked := []struct {
		key   string
		label string
	}{
		{"Threads_connected", "Threads Connected"},
		{"Threads_running", "Threads Running"},
		{"Threads_created", "Threads Created"},
		{"Threads_cached", "Threads Cached"},
		{"Max_used_connections", "Max Used Connections"},
		{"Connections", "Total Connections"},
		{"Aborted_connects", "Aborted Connects"},
		{"Aborted_clients", "Aborted Clients"},
		{"Innodb_buffer_pool_pages_total", "InnoDB BP Pages Total"},
		{"Innodb_buffer_pool_pages_free", "InnoDB BP Pages Free"},
		{"Innodb_buffer_pool_pages_dirty", "InnoDB BP Pages Dirty"},
		{"Innodb_buffer_pool_read_requests", "InnoDB BP Read Requests"},
		{"Innodb_buffer_pool_reads", "InnoDB BP Disk Reads"},
		{"Innodb_row_lock_waits", "InnoDB Row Lock Waits"},
		{"Innodb_row_lock_time_avg", "InnoDB Row Lock Avg (ms)"},
		{"Innodb_deadlocks", "InnoDB Deadlocks"},
		{"Innodb_os_log_written", "InnoDB OS Log Written"},
		{"Bytes_received", "Bytes Received"},
		{"Bytes_sent", "Bytes Sent"},
		{"Questions", "Questions"},
		{"Queries", "Queries"},
		{"Slow_queries", "Slow Queries"},
		{"Select_full_join", "Select Full Join"},
		{"Select_scan", "Select Scan"},
		{"Sort_merge_passes", "Sort Merge Passes"},
		{"Created_tmp_disk_tables", "Tmp Disk Tables"},
		{"Created_tmp_tables", "Tmp Tables"},
		{"Open_tables", "Open Tables"},
		{"Opened_tables", "Opened Tables"},
		{"Table_locks_waited", "Table Locks Waited"},
		{"Uptime", "Uptime (sec)"},
		{"Key_read_requests", "Key Read Requests"},
		{"Key_reads", "Key Reads"},
	}

	maxConn := vars["max_connections"]
	var result []GlobalStatus
	for _, t := range tracked {
		val, ok := status[t.key]
		if !ok {
			val = "N/A"
		}
		gs := GlobalStatus{Name: t.label, Value: formatStatusValue(t.key, val)}
		switch t.key {
		case "Threads_connected":
			if maxConn != "" && pct(val, maxConn) > 80 {
				gs.Note = fmt.Sprintf("%.0f%% of max_connections used", pct(val, maxConn))
				gs.Alert = true
			}
		case "Threads_running":
			if toInt(val) > 10 {
				gs.Note = "High running threads; possible contention"
				gs.Alert = true
			}
		case "Innodb_row_lock_waits":
			if toInt(val) > 0 {
				gs.Note = "Row lock contention detected"
				gs.Alert = toInt(val) > 100
			}
		case "Innodb_deadlocks":
			if toInt(val) > 0 {
				gs.Note = "Deadlocks have occurred"
				gs.Alert = true
			}
		case "Slow_queries":
			if toInt(val) > 0 {
				gs.Note = "Slow queries logged"
			}
		case "Select_full_join":
			if toInt(val) > 0 {
				gs.Note = "Full joins without index detected"
				gs.Alert = toInt(val) > 100
			}
		case "Created_tmp_disk_tables":
			if toInt(val) > 0 {
				gs.Note = "Tmp tables spilled to disk"
				gs.Alert = toInt(val) > 1000
			}
		case "Aborted_connects":
			if toInt(val) > 0 {
				gs.Note = "Some connections were aborted"
				gs.Alert = toInt(val) > 50
			}
		}
		result = append(result, gs)
	}
	return result
}

func collectInnoDB(db *sql.DB, data *GatherData) InnoDBStatus {
	result := InnoDBStatus{}
	rows, err := db.Query("SHOW ENGINE INNODB STATUS")
	if err != nil {
		data.CollectErrors = append(data.CollectErrors, "InnoDB Status: "+err.Error())
		return result
	}
	defer rows.Close()
	for rows.Next() {
		var typ, name, status string
		if err := rows.Scan(&typ, &name, &status); err == nil {
			result.Raw = status
		}
	}
	result.Sections = parseInnoDBStatus(result.Raw)
	return result
}

func collectReplica(db *sql.DB, data *GatherData) ReplicaStatus {
	r := ReplicaStatus{}
	// Try SHOW REPLICA STATUS first (MySQL 8.0.22+)
	rows, err := db.Query("SHOW REPLICA STATUS")
	if err != nil {
		// Fallback to SHOW SLAVE STATUS
		rows, err = db.Query("SHOW SLAVE STATUS")
		if err != nil {
			data.CollectErrors = append(data.CollectErrors, "Replica Status: "+err.Error())
			return r
		}
	}
	defer rows.Close()

	cols, _ := rows.Columns()
	if len(cols) == 0 {
		return r
	}
	vals := make([]interface{}, len(cols))
	valPtrs := make([]interface{}, len(cols))
	for i := range vals {
		valPtrs[i] = &vals[i]
	}
	if rows.Next() {
		r.Available = true
		rows.Scan(valPtrs...)
		m := make(map[string]string)
		for i, col := range cols {
			if vals[i] != nil {
				switch v := vals[i].(type) {
				case []byte:
					m[col] = string(v)
				default:
					m[col] = fmt.Sprintf("%v", v)
				}
			}
		}
		r.IORunning = coalesce(m, "Replica_IO_Running", "Slave_IO_Running")
		r.SQLRunning = coalesce(m, "Replica_SQL_Running", "Slave_SQL_Running")
		r.SecondsBehind = coalesce(m, "Seconds_Behind_Source", "Seconds_Behind_Master")
		r.MasterHost = coalesce(m, "Source_Host", "Master_Host")
		r.MasterPort = coalesce(m, "Source_Port", "Master_Port")
		r.ReplicateDoDBs = coalesce(m, "Replicate_Do_DB", "")
		r.LastIOError = coalesce(m, "Last_IO_Error", "")
		r.LastSQLError = coalesce(m, "Last_SQL_Error", "")
		r.MasterLogFile = coalesce(m, "Source_Log_File", "Master_Log_File")
		r.ReadMasterLogPos = coalesce(m, "Read_Source_Log_Pos", "Read_Master_Log_Pos")
		r.RelayLogFile = coalesce(m, "Relay_Log_File", "")
		r.ExecMasterLogPos = coalesce(m, "Exec_Source_Log_Pos", "Exec_Master_Log_Pos")
		r.AutoPosition = coalesce(m, "Auto_Position", "")
		r.ReplicaIOState = coalesce(m, "Replica_IO_State", "Slave_IO_State")
		r.ReplicaSQLState = coalesce(m, "Replica_SQL_Running_State", "")

		r.IOAlert = r.IORunning != "Yes"
		r.SQLAlert = r.SQLRunning != "Yes"
		lag := toInt(r.SecondsBehind)
		r.LagAlert = lag > 30
	}
	return r
}

func collectMaster(db *sql.DB, data *GatherData) MasterStatus {
	m := MasterStatus{}
	// Try SHOW BINARY LOG STATUS (8.4+) then SHOW MASTER STATUS
	tryMaster := func(query string) bool {
		rows, err := db.Query(query)
		if err != nil {
			return false
		}
		defer rows.Close()
		cols, _ := rows.Columns()
		if len(cols) == 0 {
			return false
		}
		vals := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if rows.Next() {
			m.Available = true
			rows.Scan(ptrs...)
			mp := make(map[string]string)
			for i, col := range cols {
				if vals[i] != nil {
					switch v := vals[i].(type) {
					case []byte:
						mp[col] = string(v)
					default:
						mp[col] = fmt.Sprintf("%v", v)
					}
				}
			}
			m.File = mp["File"]
			m.Position = mp["Position"]
			m.Binlog = mp["Binlog_Do_DB"]
			m.GTIDSet = coalesce(mp, "Executed_Gtid_Set", "")
			return true
		}
		return false
	}
	if !tryMaster("SHOW BINARY LOG STATUS") {
		tryMaster("SHOW MASTER STATUS")
	}
	return m
}

func collectProcessList(db *sql.DB, data *GatherData) []ProcessListRow {
	rows, err := db.Query("SHOW FULL PROCESSLIST")
	if err != nil {
		data.CollectErrors = append(data.CollectErrors, "ProcessList: "+err.Error())
		return nil
	}
	defer rows.Close()
	var result []ProcessListRow
	for rows.Next() {
		var id, user, host, db2, command, state sql.NullString
		var timeSec sql.NullInt64
		var info sql.NullString
		rows.Scan(&id, &user, &host, &db2, &command, &timeSec, &state, &info)
		p := ProcessListRow{
			ID:      nullStr(id),
			User:    nullStr(user),
			Host:    nullStr(host),
			DB:      nullStr(db2),
			Command: nullStr(command),
			Time:    fmt.Sprintf("%d", timeSec.Int64),
			State:   nullStr(state),
			Info:    nullStr(info),
		}
		if timeSec.Int64 > 30 || strings.Contains(strings.ToLower(p.State), "lock") {
			p.Alert = true
		}
		result = append(result, p)
	}
	return result
}

func collectWaitEvents(db *sql.DB, data *GatherData) []WaitEvent {
	q := `SELECT EVENT_NAME, COUNT_STAR,
		ROUND(SUM_TIMER_WAIT/1000000000000,4),
		ROUND(AVG_TIMER_WAIT/1000000000,4)
		FROM performance_schema.events_waits_summary_global_by_event_name
		WHERE COUNT_STAR > 0 AND EVENT_NAME NOT LIKE '%idle%'
		ORDER BY SUM_TIMER_WAIT DESC LIMIT 15`
	rows, err := db.Query(q)
	if err != nil {
		data.CollectErrors = append(data.CollectErrors, "Wait Events: "+err.Error())
		return nil
	}
	defer rows.Close()
	var result []WaitEvent
	for rows.Next() {
		var w WaitEvent
		rows.Scan(&w.Event, &w.Count, &w.TotalSec, &w.AvgMS)
		w.Alert = toFloat(w.TotalSec) > 1.0
		result = append(result, w)
	}
	return result
}

func collectFileIO(db *sql.DB, data *GatherData) []FileIORow {
	q := `SELECT EVENT_NAME, COUNT_READ, COUNT_WRITE,
		ROUND(SUM_NUMBER_OF_BYTES_READ/1024/1024,2),
		ROUND(SUM_NUMBER_OF_BYTES_WRITE/1024/1024,2),
		ROUND(SUM_TIMER_READ/1000000000000,2),
		ROUND(SUM_TIMER_WRITE/1000000000000,2)
		FROM performance_schema.file_summary_by_event_name
		WHERE COUNT_READ > 0 OR COUNT_WRITE > 0
		ORDER BY (SUM_TIMER_READ+SUM_TIMER_WRITE) DESC LIMIT 15`
	rows, err := db.Query(q)
	if err != nil {
		data.CollectErrors = append(data.CollectErrors, "File IO: "+err.Error())
		return nil
	}
	defer rows.Close()
	var result []FileIORow
	for rows.Next() {
		var f FileIORow
		rows.Scan(&f.Event, &f.Reads, &f.Writes, &f.MBRead, &f.MBWritten, &f.ReadLatency, &f.WriteLatency)
		result = append(result, f)
	}
	return result
}

func collectCPUQueries(db *sql.DB, data *GatherData) []CPUQueryRow {
	q := `SELECT IFNULL(DIGEST_TEXT,'(unknown)'), COUNT_STAR,
		ROUND(SUM_CPU_TIME/1000000000000,4),
		ROUND(SUM_CPU_TIME/COUNT_STAR/100000000,2)
		FROM performance_schema.events_statements_summary_by_digest
		WHERE SUM_CPU_TIME > 0
		ORDER BY SUM_CPU_TIME DESC LIMIT 15`
	rows, err := db.Query(q)
	if err != nil {
		data.CollectErrors = append(data.CollectErrors, "CPU Queries: "+err.Error())
		return nil
	}
	defer rows.Close()
	var result []CPUQueryRow
	for rows.Next() {
		var c CPUQueryRow
		rows.Scan(&c.Query, &c.Executions, &c.TotalCPU, &c.AvgCPUMS)
		result = append(result, c)
	}
	return result
}

func collectMemory(db *sql.DB, data *GatherData) []MemoryRow {
	q := `SELECT event_name, current_alloc
		FROM sys.memory_global_by_current_bytes LIMIT 15`
	rows, err := db.Query(q)
	if err != nil {
		// Try without sys schema
		q2 := `SELECT EVENT_NAME, CURRENT_NUMBER_OF_BYTES_USED
			FROM performance_schema.memory_summary_global_by_event_name
			WHERE CURRENT_NUMBER_OF_BYTES_USED > 0
			ORDER BY CURRENT_NUMBER_OF_BYTES_USED DESC LIMIT 15`
		rows, err = db.Query(q2)
		if err != nil {
			data.CollectErrors = append(data.CollectErrors, "Memory: "+err.Error())
			return nil
		}
	}
	defer rows.Close()
	var result []MemoryRow
	for rows.Next() {
		var m MemoryRow
		rows.Scan(&m.EventName, &m.CurrentAlloc)
		m.Alert = strings.Contains(m.CurrentAlloc, "GiB")
		result = append(result, m)
	}
	return result
}

func collectLocks(db *sql.DB, data *GatherData) []LockRow {
	// Try sys schema first
	q := `SELECT 
		r.trx_query AS waiting_query,
		r.trx_id AS waiting_thread,
		b.trx_id AS blocking_thread,
		b.trx_query AS blocking_query,
		'InnoDB' as lock_type,
		'ROW LOCK' as lock_mode
		FROM information_schema.innodb_trx b
		JOIN information_schema.innodb_trx r ON r.trx_id != b.trx_id
		WHERE b.trx_wait_started IS NULL AND r.trx_wait_started IS NOT NULL
		LIMIT 10`
	rows, err := db.Query(q)
	if err != nil {
		data.CollectErrors = append(data.CollectErrors, "Locks: "+err.Error())
		return nil
	}
	defer rows.Close()
	var result []LockRow
	for rows.Next() {
		var l LockRow
		rows.Scan(&l.WaitingQuery, &l.WaitingThread, &l.BlockingThread, &l.BlockingQuery, &l.LockType, &l.LockMode)
		result = append(result, l)
	}
	return result
}

func collectTableIO(db *sql.DB, data *GatherData) []TableIORow {
	q := `SELECT OBJECT_SCHEMA, OBJECT_NAME,
		COUNT_READ, COUNT_WRITE,
		ROUND(SUM_TIMER_READ/1000000000000,4),
		ROUND(SUM_TIMER_WRITE/1000000000000,4)
		FROM performance_schema.table_io_waits_summary_by_table
		WHERE COUNT_READ+COUNT_WRITE > 0
		ORDER BY SUM_TIMER_READ+SUM_TIMER_WRITE DESC LIMIT 15`
	rows, err := db.Query(q)
	if err != nil {
		data.CollectErrors = append(data.CollectErrors, "Table IO: "+err.Error())
		return nil
	}
	defer rows.Close()
	var result []TableIORow
	for rows.Next() {
		var t TableIORow
		rows.Scan(&t.Schema, &t.Table, &t.Reads, &t.Writes, &t.LatencyRead, &t.LatencyWrite)
		result = append(result, t)
	}
	return result
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

func queryKV(db *sql.DB, query string) map[string]string {
	m := make(map[string]string)
	rows, err := db.Query(query)
	if err != nil {
		return m
	}
	defer rows.Close()
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err == nil {
			m[k] = v
		}
	}
	return m
}

func coalesce(m map[string]string, keys ...string) string {
	for _, k := range keys {
		if v, ok := m[k]; ok && v != "" {
			return v
		}
	}
	return ""
}

func nullStr(n sql.NullString) string {
	if n.Valid {
		return n.String
	}
	return ""
}

func toInt(s string) int {
	var n int
	fmt.Sscanf(s, "%d", &n)
	return n
}

func toFloat(s string) float64 {
	var f float64
	fmt.Sscanf(s, "%f", &f)
	return f
}

func pct(val, max string) float64 {
	v := toFloat(val)
	m := toFloat(max)
	if m == 0 {
		return 0
	}
	return v / m * 100
}

func formatUptime(secs string) string {
	s := toInt(secs)
	d := s / 86400
	h := (s % 86400) / 3600
	m := (s % 3600) / 60
	if d > 0 {
		return fmt.Sprintf("%dd %dh %dm", d, h, m)
	}
	return fmt.Sprintf("%dh %dm", h, m)
}

func formatVarValue(key, val string) string {
	bytesKeys := []string{"innodb_buffer_pool_size", "innodb_log_file_size",
		"query_cache_size", "tmp_table_size", "max_heap_table_size",
		"sort_buffer_size", "join_buffer_size", "read_buffer_size",
		"read_rnd_buffer_size", "key_buffer_size", "max_allowed_packet"}
	for _, k := range bytesKeys {
		if key == k {
			return formatBytes(val)
		}
	}
	return val
}

func formatStatusValue(key, val string) string {
	bytesKeys := []string{"Bytes_received", "Bytes_sent", "Innodb_os_log_written"}
	for _, k := range bytesKeys {
		if key == k {
			return formatBytes(val)
		}
	}
	return val
}

func formatBytes(s string) string {
	n := toFloat(s)
	if n == 0 {
		return "0 B"
	}
	units := []string{"B", "KiB", "MiB", "GiB", "TiB"}
	i := 0
	for n >= 1024 && i < len(units)-1 {
		n /= 1024
		i++
	}
	return fmt.Sprintf("%.2f %s", n, units[i])
}

func parseInnoDBStatus(raw string) []InnoDBSection {
	var sections []InnoDBSection
	lines := strings.Split(raw, "\n")
	var current *InnoDBSection
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "---") || strings.HasPrefix(line, "===") {
			continue
		}
		if strings.ToUpper(line) == line && len(line) > 5 && !strings.HasPrefix(line, "-") {
			if current != nil && len(current.KVs) > 0 {
				sections = append(sections, *current)
			}
			current = &InnoDBSection{Title: line}
			continue
		}
		if current != nil && strings.Contains(line, ":") {
			parts := strings.SplitN(line, ":", 2)
			if len(parts) == 2 {
				k := strings.TrimSpace(parts[0])
				v := strings.TrimSpace(parts[1])
				if k != "" && v != "" && len(k) < 60 {
					current.KVs = append(current.KVs, KV{Key: k, Value: v})
				}
			}
		}
	}
	if current != nil && len(current.KVs) > 0 {
		sections = append(sections, *current)
	}
	return sections
}

// ─── HTML Template ───────────────────────────────────────────────────────────

const htmlTemplate = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>MySQL Gather — {{.Summary.Hostname}}:{{.Summary.Port}}</title>
<style>
  :root {
    --bg: #0d1117;
    --bg2: #161b22;
    --bg3: #21262d;
    --bg4: #2d333b;
    --border: #30363d;
    --text: #e6edf3;
    --text2: #8b949e;
    --text3: #6e7681;
    --accent: #58a6ff;
    --accent2: #79c0ff;
    --green: #3fb950;
    --green-bg: rgba(63,185,80,0.1);
    --yellow: #d29922;
    --yellow-bg: rgba(210,153,34,0.12);
    --red: #f85149;
    --red-bg: rgba(248,81,73,0.12);
    --orange: #e3b341;
    --purple: #bc8cff;
    --teal: #56d364;
    --font-mono: 'SFMono-Regular', Consolas, 'Liberation Mono', Menlo, monospace;
    --font-sans: -apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif;
    --radius: 6px;
    --shadow: 0 1px 3px rgba(0,0,0,0.4);
  }
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  html { scroll-behavior: smooth; }
  body {
    background: var(--bg);
    color: var(--text);
    font-family: var(--font-sans);
    font-size: 14px;
    line-height: 1.5;
    display: flex;
    min-height: 100vh;
  }

  /* ── Sidebar ── */
  #sidebar {
    width: 220px;
    min-width: 220px;
    background: var(--bg2);
    border-right: 1px solid var(--border);
    position: fixed;
    top: 0; left: 0; bottom: 0;
    overflow-y: auto;
    z-index: 100;
    display: flex;
    flex-direction: column;
  }
  #sidebar .logo {
    padding: 18px 16px 12px;
    border-bottom: 1px solid var(--border);
  }
  #sidebar .logo h1 {
    font-size: 16px;
    font-weight: 700;
    color: var(--accent);
    letter-spacing: 0.04em;
  }
  #sidebar .logo p {
    font-size: 11px;
    color: var(--text3);
    margin-top: 2px;
  }
  #sidebar nav { flex: 1; padding: 8px 0; }
  #sidebar nav a {
    display: block;
    padding: 7px 16px;
    color: var(--text2);
    text-decoration: none;
    font-size: 13px;
    border-left: 3px solid transparent;
    transition: all 0.15s;
  }
  #sidebar nav a:hover, #sidebar nav a.active {
    color: var(--text);
    background: var(--bg3);
    border-left-color: var(--accent);
  }
  #sidebar nav .section-title {
    padding: 12px 16px 4px;
    font-size: 11px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: var(--text3);
  }
  #sidebar .collected-at {
    padding: 12px 16px;
    border-top: 1px solid var(--border);
    font-size: 11px;
    color: var(--text3);
  }

  /* ── Main Content ── */
  #main {
    margin-left: 220px;
    flex: 1;
    min-width: 0;
    padding: 24px 28px;
    max-width: 1400px;
  }

  /* ── Sections ── */
  .section {
    margin-bottom: 32px;
    scroll-margin-top: 16px;
  }
  .section-header {
    display: flex;
    align-items: center;
    gap: 8px;
    margin-bottom: 14px;
    padding-bottom: 8px;
    border-bottom: 1px solid var(--border);
  }
  .section-header h2 {
    font-size: 16px;
    font-weight: 600;
    color: var(--text);
  }
  .section-header .badge {
    font-size: 11px;
    padding: 2px 7px;
    border-radius: 10px;
    font-weight: 500;
  }
  .badge-blue { background: rgba(88,166,255,0.15); color: var(--accent2); }
  .badge-red  { background: var(--red-bg); color: var(--red); }
  .badge-green{ background: var(--green-bg); color: var(--green); }
  .badge-yellow{ background: var(--yellow-bg); color: var(--orange); }

  /* ── Summary Cards ── */
  .summary-grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
    gap: 12px;
    margin-bottom: 16px;
  }
  .summary-card {
    background: var(--bg2);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 14px 16px;
  }
  .summary-card .label {
    font-size: 11px;
    color: var(--text3);
    text-transform: uppercase;
    letter-spacing: 0.06em;
    margin-bottom: 4px;
  }
  .summary-card .value {
    font-size: 15px;
    font-weight: 600;
    color: var(--text);
    word-break: break-all;
  }
  .summary-card .value.accent { color: var(--accent); }
  .summary-card .value.green  { color: var(--green); }

  /* ── KV Table ── */
  .kv-table {
    width: 100%;
    border-collapse: collapse;
    font-size: 13px;
    background: var(--bg2);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    overflow: hidden;
  }
  .kv-table th {
    background: var(--bg3);
    color: var(--text2);
    font-size: 11px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    padding: 8px 12px;
    text-align: left;
    border-bottom: 1px solid var(--border);
  }
  .kv-table td {
    padding: 7px 12px;
    border-bottom: 1px solid var(--border);
    vertical-align: top;
    word-break: break-word;
  }
  .kv-table tr:last-child td { border-bottom: none; }
  .kv-table tr:hover td { background: var(--bg3); }
  .kv-table .key-col { color: var(--text2); width: 260px; font-family: var(--font-mono); font-size: 12px; }
  .kv-table .val-col { color: var(--text); font-family: var(--font-mono); font-size: 12px; }
  .kv-table .note-col { color: var(--text3); font-size: 11px; max-width: 300px; }
  .row-alert td { background: var(--red-bg) !important; }
  .row-alert .key-col { color: var(--red); }
  .row-alert .val-col { color: var(--orange); font-weight: 600; }
  .row-warn td { background: var(--yellow-bg) !important; }

  /* ── Process List ── */
  .proc-table {
    width: 100%;
    border-collapse: collapse;
    font-size: 12px;
    background: var(--bg2);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    overflow: hidden;
  }
  .proc-table th {
    background: var(--bg3);
    color: var(--text2);
    font-size: 11px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    padding: 7px 10px;
    text-align: left;
    border-bottom: 1px solid var(--border);
    white-space: nowrap;
  }
  .proc-table td {
    padding: 6px 10px;
    border-bottom: 1px solid var(--border);
    vertical-align: top;
    font-family: var(--font-mono);
    max-width: 0;
  }
  .proc-table .sql-col {
    max-width: 380px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    cursor: pointer;
  }
  .proc-table .sql-col:hover { white-space: normal; word-break: break-all; }
  .proc-table tr:last-child td { border-bottom: none; }
  .proc-table tr:hover td { background: rgba(255,255,255,0.03); }
  .proc-table .alert-row td { background: var(--red-bg) !important; }
  .proc-table .time-col { color: var(--accent2); }
  .proc-table .alert-row .time-col { color: var(--red); font-weight: 700; }

  /* ── Replica Status ── */
  .replica-grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(220px, 1fr));
    gap: 12px;
    margin-bottom: 16px;
  }
  .replica-card {
    background: var(--bg2);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 14px 16px;
  }
  .replica-card.ok { border-color: var(--green); }
  .replica-card.warn { border-color: var(--red); }
  .replica-card .label { font-size: 11px; color: var(--text3); text-transform: uppercase; letter-spacing: 0.06em; margin-bottom: 4px; }
  .replica-card .value { font-size: 16px; font-weight: 700; }
  .replica-card .value.ok { color: var(--green); }
  .replica-card .value.warn { color: var(--red); }
  .replica-card .value.neutral { color: var(--text2); }

  /* ── InnoDB Raw ── */
  .innodb-raw {
    background: var(--bg2);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    overflow: hidden;
  }
  .innodb-raw-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 8px 14px;
    background: var(--bg3);
    border-bottom: 1px solid var(--border);
  }
  .innodb-raw-header span { font-size: 12px; color: var(--text2); }
  .innodb-search {
    background: var(--bg);
    border: 1px solid var(--border);
    border-radius: 4px;
    color: var(--text);
    padding: 4px 8px;
    font-size: 12px;
    font-family: var(--font-mono);
    width: 200px;
    outline: none;
  }
  .innodb-search:focus { border-color: var(--accent); }
  .innodb-pre {
    font-family: var(--font-mono);
    font-size: 12px;
    line-height: 1.6;
    padding: 14px;
    white-space: pre-wrap;
    word-break: break-all;
    max-height: 500px;
    overflow-y: auto;
    color: var(--text2);
  }
  .innodb-pre mark { background: rgba(210,153,34,0.35); color: var(--orange); border-radius: 2px; }

  /* ── Alert Banner ── */
  .alert-banner {
    background: var(--red-bg);
    border: 1px solid var(--red);
    border-radius: var(--radius);
    padding: 10px 14px;
    margin-bottom: 16px;
    font-size: 13px;
    color: var(--red);
  }
  .alert-banner ul { margin: 4px 0 0 16px; }
  .alert-banner li { margin-top: 2px; }

  /* ── No Data ── */
  .no-data {
    padding: 20px;
    text-align: center;
    color: var(--text3);
    font-size: 13px;
    background: var(--bg2);
    border: 1px solid var(--border);
    border-radius: var(--radius);
  }

  /* ── Tabs ── */
  .tab-bar {
    display: flex;
    gap: 4px;
    margin-bottom: 14px;
    border-bottom: 1px solid var(--border);
  }
  .tab-btn {
    padding: 7px 14px;
    background: none;
    border: none;
    border-bottom: 2px solid transparent;
    color: var(--text2);
    cursor: pointer;
    font-size: 13px;
    margin-bottom: -1px;
    transition: all 0.15s;
  }
  .tab-btn:hover { color: var(--text); }
  .tab-btn.active { color: var(--accent); border-bottom-color: var(--accent); font-weight: 600; }
  .tab-pane { display: none; }
  .tab-pane.active { display: block; }

  /* ── Scrollbar ── */
  ::-webkit-scrollbar { width: 6px; height: 6px; }
  ::-webkit-scrollbar-track { background: var(--bg2); }
  ::-webkit-scrollbar-thumb { background: var(--bg4); border-radius: 3px; }
  ::-webkit-scrollbar-thumb:hover { background: var(--text3); }

  /* ── Misc ── */
  .text-mono { font-family: var(--font-mono); font-size: 12px; }
  .text-dim { color: var(--text3); }
  .text-red { color: var(--red); }
  .text-green { color: var(--green); }
  .text-yellow { color: var(--orange); }
  .text-blue { color: var(--accent); }
  .mb8 { margin-bottom: 8px; }
  .mb16 { margin-bottom: 16px; }
  .status-dot { display: inline-block; width: 8px; height: 8px; border-radius: 50%; margin-right: 6px; }
  .dot-green { background: var(--green); box-shadow: 0 0 4px var(--green); }
  .dot-red   { background: var(--red); box-shadow: 0 0 4px var(--red); }
  .dot-yellow{ background: var(--orange); }
  .section-desc { font-size: 12px; color: var(--text3); margin-bottom: 12px; }

  @media (max-width: 900px) {
    #sidebar { width: 180px; min-width: 180px; }
    #main { margin-left: 180px; padding: 16px; }
  }
</style>
</head>
<body>

<!-- Sidebar -->
<nav id="sidebar">
  <div class="logo">
    <h1>⚡ mysql_gather</h1>
    <p>Performance Diagnostic Report</p>
  </div>
  <nav>
    <div class="section-title">Overview</div>
    <a href="#summary">System Summary</a>
    <a href="#replica">Replication</a>
    <div class="section-title">Configuration</div>
    <a href="#variables">Global Variables</a>
    <a href="#status">Global Status</a>
    <div class="section-title">Activity</div>
    <a href="#processlist">Process List</a>
    <a href="#innodb">InnoDB Status</a>
    <div class="section-title">Performance Schema</div>
    <a href="#waits">Wait Events</a>
    <a href="#fileio">File I/O</a>
    <a href="#memory">Memory Usage</a>
    <a href="#cpu-queries">Top CPU Queries</a>
    <a href="#tableio">Table I/O</a>
    <a href="#locks">Lock Activity</a>
  </nav>
  <div class="collected-at">Collected: {{.Summary.CollectedAt}}</div>
</nav>

<!-- Main -->
<main id="main">

<!-- ── System Summary ── -->
<section class="section" id="summary">
  <div class="section-header">
    <h2>System Summary</h2>
    <span class="badge badge-blue">{{.Summary.Version}}</span>
  </div>
  <div class="summary-grid">
    <div class="summary-card">
      <div class="label">Host</div>
      <div class="value accent">{{.Summary.Hostname}}:{{.Summary.Port}}</div>
    </div>
    <div class="summary-card">
      <div class="label">Version</div>
      <div class="value">{{.Summary.Version}}</div>
    </div>
    <div class="summary-card">
      <div class="label">Uptime</div>
      <div class="value green">{{.Summary.UptimeHuman}}</div>
    </div>
    <div class="summary-card">
      <div class="label">Current User</div>
      <div class="value">{{.Summary.CurrentUser}}</div>
    </div>
    <div class="summary-card">
      <div class="label">Data Directory</div>
      <div class="value text-mono" style="font-size:12px">{{.Summary.DataDir}}</div>
    </div>
    <div class="summary-card">
      <div class="label">OS / Arch</div>
      <div class="value">{{.Summary.OS}} / {{.Summary.Arch}}</div>
    </div>
    <div class="summary-card">
      <div class="label">Character Set</div>
      <div class="value">{{.Summary.CharSet}}</div>
    </div>
    <div class="summary-card">
      <div class="label">Timezone</div>
      <div class="value">{{.Summary.Timezone}}</div>
    </div>
  </div>
  <p class="text-mono text-dim" style="font-size:11px">{{.Summary.VersionComment}}</p>
</section>

<!-- ── Collection Errors ── -->
{{if .CollectErrors}}
<div class="alert-banner">
  <strong>⚠ Collection Warnings</strong> — some queries could not be executed:
  <ul>
    {{range .CollectErrors}}<li>{{.}}</li>{{end}}
  </ul>
</div>
{{end}}

<!-- ── Replication ── -->
<section class="section" id="replica">
  <div class="section-header">
    <h2>HA / Replication</h2>
    {{if .Replica.Available}}
      {{if or .Replica.IOAlert .Replica.SQLAlert .Replica.LagAlert}}
        <span class="badge badge-red">⚠ Issues Detected</span>
      {{else}}
        <span class="badge badge-green">✓ Healthy</span>
      {{end}}
    {{else}}
      <span class="badge badge-blue">Standalone / Source</span>
    {{end}}
  </div>

  {{if .Master.Available}}
  <p class="section-desc">This server is a <strong>replication source</strong> (binary logging enabled).</p>
  <div class="replica-grid mb16">
    <div class="replica-card ok">
      <div class="label">Binary Log File</div>
      <div class="value text-mono" style="font-size:13px;color:var(--teal)">{{.Master.File}}</div>
    </div>
    <div class="replica-card">
      <div class="label">Binlog Position</div>
      <div class="value" style="color:var(--accent)">{{.Master.Position}}</div>
    </div>
    {{if .Master.GTIDSet}}
    <div class="replica-card">
      <div class="label">Executed GTID Set</div>
      <div class="value text-mono" style="font-size:11px;color:var(--text2)">{{.Master.GTIDSet}}</div>
    </div>
    {{end}}
  </div>
  {{end}}

  {{if .Replica.Available}}
  <div class="replica-grid">
    <div class="replica-card {{if .Replica.IOAlert}}warn{{else}}ok{{end}}">
      <div class="label"><span class="status-dot {{if .Replica.IOAlert}}dot-red{{else}}dot-green{{end}}"></span>IO Thread</div>
      <div class="value {{if .Replica.IOAlert}}warn{{else}}ok{{end}}">{{.Replica.IORunning}}</div>
    </div>
    <div class="replica-card {{if .Replica.SQLAlert}}warn{{else}}ok{{end}}">
      <div class="label"><span class="status-dot {{if .Replica.SQLAlert}}dot-red{{else}}dot-green{{end}}"></span>SQL Thread</div>
      <div class="value {{if .Replica.SQLAlert}}warn{{else}}ok{{end}}">{{.Replica.SQLRunning}}</div>
    </div>
    <div class="replica-card {{if .Replica.LagAlert}}warn{{else}}ok{{end}}">
      <div class="label">Seconds Behind Source</div>
      <div class="value {{if .Replica.LagAlert}}warn{{else}}ok{{end}}">{{.Replica.SecondsBehind}}s</div>
    </div>
    <div class="replica-card">
      <div class="label">Source Host</div>
      <div class="value neutral text-mono" style="font-size:13px">{{.Replica.MasterHost}}:{{.Replica.MasterPort}}</div>
    </div>
  </div>
  {{if .Replica.ReplicaIOState}}
  <p class="section-desc" style="margin-top:10px">IO State: <span class="text-mono" style="color:var(--text)">{{.Replica.ReplicaIOState}}</span></p>
  {{end}}
  <table class="kv-table" style="margin-top:12px">
    <tr><th class="key-col">Attribute</th><th class="val-col">Value</th></tr>
    <tr><td class="key-col">Master Log File</td><td class="val-col">{{.Replica.MasterLogFile}}</td></tr>
    <tr><td class="key-col">Read Master Log Pos</td><td class="val-col">{{.Replica.ReadMasterLogPos}}</td></tr>
    <tr><td class="key-col">Relay Log File</td><td class="val-col">{{.Replica.RelayLogFile}}</td></tr>
    <tr><td class="key-col">Exec Master Log Pos</td><td class="val-col">{{.Replica.ExecMasterLogPos}}</td></tr>
    <tr><td class="key-col">Auto Position (GTID)</td><td class="val-col">{{.Replica.AutoPosition}}</td></tr>
    {{if .Replica.LastIOError}}<tr class="row-alert"><td class="key-col">Last IO Error</td><td class="val-col">{{.Replica.LastIOError}}</td></tr>{{end}}
    {{if .Replica.LastSQLError}}<tr class="row-alert"><td class="key-col">Last SQL Error</td><td class="val-col">{{.Replica.LastSQLError}}</td></tr>{{end}}
  </table>
  {{else if not .Master.Available}}
  <div class="no-data">No replica status found — this server is not configured as a replica.</div>
  {{end}}
</section>

<!-- ── Global Variables ── -->
<section class="section" id="variables">
  <div class="section-header">
    <h2>Critical Configurations</h2>
    <span class="badge badge-blue">GLOBAL VARIABLES</span>
  </div>
  <p class="section-desc">Key configuration variables. Highlighted rows indicate potential issues.</p>
  <table class="kv-table">
    <thead><tr>
      <th class="key-col">Variable</th>
      <th class="val-col">Value</th>
      <th class="note-col">Note</th>
    </tr></thead>
    <tbody>
    {{range .Variables}}
    <tr {{if .Alert}}class="row-alert"{{end}}>
      <td class="key-col">{{.Name}}</td>
      <td class="val-col">{{.Value}}</td>
      <td class="note-col">{{if .Note}}<span style="color:{{if .Alert}}var(--red){{else}}var(--text3){{end}}">{{.Note}}</span>{{end}}</td>
    </tr>
    {{end}}
    </tbody>
  </table>
</section>

<!-- ── Global Status ── -->
<section class="section" id="status">
  <div class="section-header">
    <h2>Performance Metrics</h2>
    <span class="badge badge-blue">GLOBAL STATUS</span>
  </div>
  <p class="section-desc">Runtime counters. Alert rows indicate potential performance issues.</p>
  <table class="kv-table">
    <thead><tr>
      <th class="key-col">Metric</th>
      <th class="val-col">Value</th>
      <th class="note-col">Note</th>
    </tr></thead>
    <tbody>
    {{range .Status}}
    <tr {{if .Alert}}class="row-alert"{{end}}>
      <td class="key-col">{{.Name}}</td>
      <td class="val-col">{{.Value}}</td>
      <td class="note-col">{{if .Note}}<span style="color:{{if .Alert}}var(--red){{else}}var(--text3){{end}}">{{.Note}}</span>{{end}}</td>
    </tr>
    {{end}}
    </tbody>
  </table>
</section>

<!-- ── Process List ── -->
<section class="section" id="processlist">
  <div class="section-header">
    <h2>Process List</h2>
    <span class="badge badge-blue">FULL PROCESSLIST</span>
  </div>
  <p class="section-desc">Active queries at collection time. Red rows = long-running or locked queries (&gt;30s or lock state).</p>
  {{if .ProcessList}}
  <div style="overflow-x:auto">
  <table class="proc-table">
    <thead><tr>
      <th>ID</th><th>User</th><th>Host</th><th>DB</th>
      <th>Command</th><th>Time (s)</th><th>State</th><th>Query</th>
    </tr></thead>
    <tbody>
    {{range .ProcessList}}
    <tr {{if .Alert}}class="alert-row"{{end}}>
      <td>{{.ID}}</td>
      <td>{{.User}}</td>
      <td>{{.Host}}</td>
      <td>{{.DB}}</td>
      <td>{{.Command}}</td>
      <td class="time-col">{{.Time}}</td>
      <td>{{.State}}</td>
      <td class="sql-col" title="{{.Info}}">{{.Info}}</td>
    </tr>
    {{end}}
    </tbody>
  </table>
  </div>
  {{else}}
  <div class="no-data">No active processes (other than this connection).</div>
  {{end}}
</section>

<!-- ── InnoDB Status ── -->
<section class="section" id="innodb">
  <div class="section-header">
    <h2>InnoDB Engine Status</h2>
    <span class="badge badge-blue">SHOW ENGINE INNODB STATUS</span>
  </div>

  <div class="tab-bar">
    <button class="tab-btn active" onclick="switchTab(event,'innodb-kv')">Key Metrics</button>
    <button class="tab-btn" onclick="switchTab(event,'innodb-raw-tab')">Raw Output</button>
  </div>

  <div id="innodb-kv" class="tab-pane active">
    {{if .InnoDB.Sections}}
    {{range .InnoDB.Sections}}
    <div style="margin-bottom:16px">
      <p style="font-size:12px;font-weight:600;color:var(--accent2);margin-bottom:6px;text-transform:uppercase;letter-spacing:0.06em">{{.Title}}</p>
      <table class="kv-table">
        {{range .KVs}}
        <tr {{if .Alert}}class="row-alert"{{end}}>
          <td class="key-col">{{.Key}}</td>
          <td class="val-col">{{.Value}}</td>
        </tr>
        {{end}}
      </table>
    </div>
    {{end}}
    {{else}}
    <div class="no-data">No structured sections extracted from InnoDB status.</div>
    {{end}}
  </div>

  <div id="innodb-raw-tab" class="tab-pane">
    <div class="innodb-raw">
      <div class="innodb-raw-header">
        <span>Raw INNODB STATUS output</span>
        <input class="innodb-search" type="text" placeholder="Search..." id="innodbSearch" oninput="highlightSearch()">
      </div>
      <pre class="innodb-pre" id="innodbPre">{{.InnoDB.Raw}}</pre>
    </div>
  </div>
</section>

<!-- ── Wait Events ── -->
<section class="section" id="waits">
  <div class="section-header">
    <h2>Wait Events</h2>
    <span class="badge badge-blue">performance_schema</span>
  </div>
  <p class="section-desc">Top wait events by total wait time. High total_wait_sec may indicate I/O pressure or lock contention.</p>
  {{if .WaitEvents}}
  <table class="kv-table">
    <thead><tr>
      <th>Wait Event</th>
      <th style="text-align:right">Occurrences</th>
      <th style="text-align:right">Total Wait (s)</th>
      <th style="text-align:right">Avg Wait (ms)</th>
    </tr></thead>
    <tbody>
    {{range .WaitEvents}}
    <tr {{if .Alert}}class="row-alert"{{end}}>
      <td class="text-mono" style="font-size:12px">{{.Event}}</td>
      <td style="text-align:right;font-family:var(--font-mono)">{{.Count}}</td>
      <td style="text-align:right;font-family:var(--font-mono);{{if .Alert}}color:var(--red){{end}}">{{.TotalSec}}</td>
      <td style="text-align:right;font-family:var(--font-mono)">{{.AvgMS}}</td>
    </tr>
    {{end}}
    </tbody>
  </table>
  {{else}}
  <div class="no-data">No wait event data available (performance_schema may be disabled).</div>
  {{end}}
</section>

<!-- ── File I/O ── -->
<section class="section" id="fileio">
  <div class="section-header">
    <h2>File I/O Summary</h2>
    <span class="badge badge-blue">performance_schema</span>
  </div>
  {{if .FileIO}}
  <div style="overflow-x:auto">
  <table class="kv-table">
    <thead><tr>
      <th>I/O Event</th>
      <th style="text-align:right">Reads</th>
      <th style="text-align:right">Writes</th>
      <th style="text-align:right">MB Read</th>
      <th style="text-align:right">MB Written</th>
      <th style="text-align:right">Read Lat (s)</th>
      <th style="text-align:right">Write Lat (s)</th>
    </tr></thead>
    <tbody>
    {{range .FileIO}}
    <tr>
      <td class="text-mono" style="font-size:12px">{{.Event}}</td>
      <td style="text-align:right;font-family:var(--font-mono)">{{.Reads}}</td>
      <td style="text-align:right;font-family:var(--font-mono)">{{.Writes}}</td>
      <td style="text-align:right;font-family:var(--font-mono)">{{.MBRead}}</td>
      <td style="text-align:right;font-family:var(--font-mono)">{{.MBWritten}}</td>
      <td style="text-align:right;font-family:var(--font-mono)">{{.ReadLatency}}</td>
      <td style="text-align:right;font-family:var(--font-mono)">{{.WriteLatency}}</td>
    </tr>
    {{end}}
    </tbody>
  </table>
  </div>
  {{else}}
  <div class="no-data">No file I/O data available.</div>
  {{end}}
</section>

<!-- ── Memory ── -->
<section class="section" id="memory">
  <div class="section-header">
    <h2>Memory Usage</h2>
    <span class="badge badge-blue">sys.memory_global_by_current_bytes</span>
  </div>
  <p class="section-desc">Current memory allocation by component. GiB-level allocations are highlighted.</p>
  {{if .Memory}}
  <table class="kv-table">
    <thead><tr>
      <th>Event / Component</th>
      <th style="text-align:right">Current Allocation</th>
    </tr></thead>
    <tbody>
    {{range .Memory}}
    <tr {{if .Alert}}class="row-warn"{{end}}>
      <td class="text-mono" style="font-size:12px">{{.EventName}}</td>
      <td style="text-align:right;font-family:var(--font-mono);{{if .Alert}}color:var(--orange);font-weight:600{{end}}">{{.CurrentAlloc}}</td>
    </tr>
    {{end}}
    </tbody>
  </table>
  {{else}}
  <div class="no-data">Memory data unavailable (sys schema may be missing or performance_schema disabled).</div>
  {{end}}
</section>

<!-- ── CPU Queries ── -->
<section class="section" id="cpu-queries">
  <div class="section-header">
    <h2>Top CPU Queries</h2>
    <span class="badge badge-blue">events_statements_summary_by_digest</span>
  </div>
  <p class="section-desc">Queries ordered by total CPU time consumed since server start.</p>
  {{if .CPUQueries}}
  <table class="kv-table">
    <thead><tr>
      <th>Query Digest</th>
      <th style="text-align:right">Executions</th>
      <th style="text-align:right">Total CPU (s)</th>
      <th style="text-align:right">Avg CPU (ms)</th>
    </tr></thead>
    <tbody>
    {{range .CPUQueries}}
    <tr>
      <td class="text-mono" style="font-size:11px;max-width:500px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="{{.Query}}">{{.Query}}</td>
      <td style="text-align:right;font-family:var(--font-mono)">{{.Executions}}</td>
      <td style="text-align:right;font-family:var(--font-mono)">{{.TotalCPU}}</td>
      <td style="text-align:right;font-family:var(--font-mono)">{{.AvgCPUMS}}</td>
    </tr>
    {{end}}
    </tbody>
  </table>
  {{else}}
  <div class="no-data">No CPU query data available.</div>
  {{end}}
</section>

<!-- ── Table I/O ── -->
<section class="section" id="tableio">
  <div class="section-header">
    <h2>Table I/O Waits</h2>
    <span class="badge badge-blue">table_io_waits_summary_by_table</span>
  </div>
  {{if .TableIO}}
  <table class="kv-table">
    <thead><tr>
      <th>Schema</th>
      <th>Table</th>
      <th style="text-align:right">Reads</th>
      <th style="text-align:right">Writes</th>
      <th style="text-align:right">Read Lat (s)</th>
      <th style="text-align:right">Write Lat (s)</th>
    </tr></thead>
    <tbody>
    {{range .TableIO}}
    <tr>
      <td class="text-mono" style="font-size:12px;color:var(--text3)">{{.Schema}}</td>
      <td class="text-mono" style="font-size:12px">{{.Table}}</td>
      <td style="text-align:right;font-family:var(--font-mono)">{{.Reads}}</td>
      <td style="text-align:right;font-family:var(--font-mono)">{{.Writes}}</td>
      <td style="text-align:right;font-family:var(--font-mono)">{{.LatencyRead}}</td>
      <td style="text-align:right;font-family:var(--font-mono)">{{.LatencyWrite}}</td>
    </tr>
    {{end}}
    </tbody>
  </table>
  {{else}}
  <div class="no-data">No table I/O data available.</div>
  {{end}}
</section>

<!-- ── Lock Activity ── -->
<section class="section" id="locks">
  <div class="section-header">
    <h2>Lock Activity</h2>
    <span class="badge badge-blue">information_schema.innodb_trx</span>
  </div>
  {{if .Locks}}
  <div class="alert-banner">⚠ Active lock waits detected!</div>
  <table class="kv-table">
    <thead><tr>
      <th>Waiting Query</th>
      <th>Waiting Thread</th>
      <th>Blocking Thread</th>
      <th>Blocking Query</th>
      <th>Lock Type</th>
    </tr></thead>
    <tbody>
    {{range .Locks}}
    <tr class="row-alert">
      <td class="text-mono" style="font-size:11px;max-width:200px;overflow:hidden;text-overflow:ellipsis">{{.WaitingQuery}}</td>
      <td class="text-mono">{{.WaitingThread}}</td>
      <td class="text-mono">{{.BlockingThread}}</td>
      <td class="text-mono" style="font-size:11px;max-width:200px;overflow:hidden;text-overflow:ellipsis">{{.BlockingQuery}}</td>
      <td class="text-mono">{{.LockMode}}</td>
    </tr>
    {{end}}
    </tbody>
  </table>
  {{else}}
  <div class="no-data"><span class="status-dot dot-green"></span>No active lock waits detected.</div>
  {{end}}
</section>

</main>

<script>
// Tab switching
function switchTab(e, id) {
  const section = e.target.closest('section') || document.body;
  section.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
  section.querySelectorAll('.tab-pane').forEach(p => p.classList.remove('active'));
  e.target.classList.add('active');
  document.getElementById(id).classList.add('active');
}

// InnoDB search highlight
function highlightSearch() {
  const q = document.getElementById('innodbSearch').value;
  const pre = document.getElementById('innodbPre');
  const raw = pre.textContent;
  if (!q) { pre.innerHTML = escapeHtml(raw); return; }
  const re = new RegExp(escapeRe(q), 'gi');
  pre.innerHTML = escapeHtml(raw).replace(re.source, m => '<mark>' + m + '</mark>');
}
function escapeHtml(s) {
  return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}
function escapeRe(s) {
  return s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

// Sidebar active link
const links = document.querySelectorAll('#sidebar nav a');
const sections = document.querySelectorAll('.section');
const obs = new IntersectionObserver(entries => {
  entries.forEach(e => {
    if (e.isIntersecting) {
      links.forEach(l => l.classList.toggle('active', l.getAttribute('href') === '#' + e.target.id));
    }
  });
}, { threshold: 0.2, rootMargin: '-80px 0px -80px 0px' });
sections.forEach(s => obs.observe(s));

// Store raw InnoDB text after DOM load for search
window.addEventListener('DOMContentLoaded', () => {
  const pre = document.getElementById('innodbPre');
  if (pre) pre.dataset.raw = pre.textContent;
});
document.getElementById('innodbSearch').addEventListener('input', function() {
  const q = this.value.trim();
  const pre = document.getElementById('innodbPre');
  const raw = pre.dataset.raw || pre.textContent;
  if (!q) { pre.innerHTML = escapeHtml(raw); return; }
  const re = new RegExp(escapeRe(q), 'gi');
  pre.innerHTML = escapeHtml(raw).replace(re, m => '<mark>' + m + '</mark>');
});
</script>
</body>
</html>`
