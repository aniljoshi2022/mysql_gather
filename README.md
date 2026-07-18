# 🐬 MySQL Gather

A Go-based tool to gather MySQL insight and performance metrics in a single HTML file.

## ⌛️How to Run

### Option 1: Download Pre-compiled Binaries (Easiest)

Go to the **Releases** tab, download the binary for your OS, and run it via terminal/command prompt:

**Linux:** `./mysql_gather_linux-<ARCH> --user=<USER> --password=<PASSWORD> --host=<HOST> --port=<PORT>`

**Windows:** `mysql_gather_win.exe --user=<USER> --password=<PASSWORD> --host=<HOST>  --port=<PORT>`

**Mac:** `./mysql_gather_mac_Darwin_<ARCH> --user=<USER> --password=<PASSWORD> --host=<HOST>  --port=<PORT>`

### Option 2: Build from Source (Requires Go installed)

```bash
git clone https://github.com/aniljoshi2022/mysql_gather.git
cd mysql_gather
go mod tidy
go build -o mysql_gather main.go
chmod +x mysql_gather
./mysql_gather --user=<USER> --password=<PASSWORD> --host=<HOST> --port=<PORT> --db=<database>
```

To build this project from source, you need to have Go (Golang) installed on your machine.

- Mac (Homebrew): `brew install go`
- Linux (Ubuntu/Debian): `sudo apt install golang-go`
- Linux (Centos/Rhel): `sudo yum/dnf install go`
- Windows: Download the installer from `https://go.dev/dl/`

Verify your installation by running `go version` in your terminal or command prompt.

**Build the code**

```
go build -o mysql_gather main.go
```

**Make it executable (usually required on Linux/Mac)**

```
chmod +x mysql_gather
```

**Run the tool**

```
./mysql_gather --user=root --password=Root@1234 --host=127.0.0.1 --port=3306 --db=test
```

---



## 📸 Screenshots


|                     |                             |
| ------------------- | --------------------------- |
| **mysql_gather UI** | **HA/Replication Topology** |




## **🔖Usage**

```bash
./mysql_gather --help
Usage of ./mysql_gather:
  -db string
    	Target database name for Tables & Indexes analysis (required)
  -host string
    	MySQL host address (default "127.0.0.1")
  -output string
    	Path to write the standalone HTML report (default "mysql_gather.html")
  -password string
    	MySQL database password
  -port int
    	MySQL host port (default 3306)
  -user string
    	MySQL database user (default "root")
```



## **🧑‍💻Monitoring Previleges required:**

```bash
CREATE USER '<USERNAME>'@localhost identified by '<PASSWORD>';
GRANT PROCESS ON . TO <USERNAME>'@localhost;
GRANT REPLICATION CLIENT ON . TO <USERNAME>'@localhost;
GRANT SELECT ON performance_schema.* TO <USERNAME>'@localhost;
GRANT SELECT ON sys.* TO <USERNAME>'@localhost;
GRANT SELECT ON mysql.* TO <USERNAME>'@localhost;
```

