# MySQL Gather

A Go-based tool to gather MySQL insight and performance metrics and output it  in an HTML report.

## How to Run

### Option 1: Download Pre-compiled Binaries (Easiest)
Go to the **Releases** tab, download the binary for your OS, and run it via terminal/command prompt:

**Linux:** `./mysql_gather_linux-<ARCH> --user=<USER> --password=<PASSWORD> --host=<HOST> --port=<PORT>`

**Windows:** `mysql_gather_win.exe --user=<USER> --password=<PASSWORD> --host=<HOST>  --port=<PORT>`

**Mac:** `./mysql_gather_mac_Darwin_<ARCH> --user=<USER> --password=<PASSWORD> --host=<HOST>  --port=<PORT>`

### Option 2: Build from Source (Requires Go installed)


```bash
git clone https://github.com/aniljoshi2022/mysql_gather.git
cd mysql-gather
go mod tidy
go build -o mysql_gather main.go
chmod +x mysql_gather
./mysql_gather --user=<USER> --password=<PASSWORD --host=<HOST> --port=<PORT


To build this project from source, you need to have Go (Golang) installed on your machine.

- Mac (Homebrew): `brew install go`
- Linux (Ubuntu/Debian): `sudo apt install golang-go`
- Windows: Download the installer from https://go.dev/dl/

Verify your installation by running `go version` in your terminal or command prompt.

# Compile the code
go build -o mysql_gather main.go

# Make it executable (usually required on Linux/Mac)
chmod +x mysql_gather

# Run the tool
./mysql_gather --user=root --password=Root@1234 --host=127.0.0.1 --port=3306
