# 🛡️ storecrypt

**storecrypt** is a pluggable storage abstraction for secure file storage.
It transparently applies compression and encryption when storing and retrieving files,
with support for **local**, **S3**, and **SFTP** backends.

---

## ✨ Features

- 🔐 **Encryption** for data confidentiality and integrity
- 📦 **Compression** to reduce storage size
- 🔁 Unified `Storage` interface across:
    - Local filesystem
    - Amazon S3
    - SFTP servers
- ✅ Comprehensive integration tests across all backends

---

## 🧪 Usage Example

See [`main.go`](./main.go) for a simple working example that:

- Initializes all backends: **local**, **S3**, and **SFTP**
- Compresses + encrypts a file (`Put`)
- Retrieves it (`Get`) and prints the content
- Checks file existence (`Exists`)

```bash
make run-demo
go run main.go
