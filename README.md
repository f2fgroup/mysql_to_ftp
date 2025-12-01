# MySQL to SFTP Extraction

A bash script that executes SQL queries from `.sql` files on a MySQL server (local or remote), generates CSV files locally, and uploads them to an SFTP server using atomic operations.

## Features

- ✅ Executes SQL queries from `.sql` files on local or remote MySQL server
- ✅ Generates CSV files locally (no FILE privilege required on remote MySQL)
- ✅ Works with remote MySQL servers without filesystem access
- ✅ Atomic SFTP upload (upload as `.part` then rename)
- ✅ UTF-8 encoding support
- ✅ Configurable CSV format
- ✅ CSV headers included automatically
- ✅ Comprehensive logging
- ✅ Error handling
- ✅ No Python or additional dependencies required (bash + mysql client + sftp only)

## Requirements

### System Requirements
- Bash shell
- MySQL client (`mysql` command)
- SFTP client (`sftp` command)
- Optional: `sshpass` (for SFTP password authentication)

### MySQL Requirements
- MySQL user with SELECT privilege on the target database
- No FILE privilege required (CSV generation is done locally)
- Works with local or remote MySQL servers

### SFTP Requirements
- Valid SFTP credentials (password or SSH key)
- Write permissions on the remote directory

## Installation

1. Clone this repository:
```bash
git clone https://github.com/f2f-apps/mysql_to_ftp.git
cd mysql_to_ftp
```

2. Make the script executable:
```bash
chmod +x mysql_to_sftp.sh
```

3. Create your configuration file:
```bash
cp config.env.example .env
# Edit .env with your settings
vim .env
```

### Optional: Quick local MySQL setup

If you don't have MySQL installed locally, you can use the helper script to install and configure it using your `.env`/`config.env` values (database, user, password, and secure_file_priv):

```bash
chmod +x install_mysql_local.sh
# Uses .env if present, otherwise config.env
./install_mysql_local.sh

# Or specify a custom env file
./install_mysql_local.sh --config /path/to/your.env
```

What this does:
- Installs `mysql-server` and `mysql-client` via apt
- Ensures `secure_file_priv` equals your `OUTPUT_DIR`
- Creates the database and user, and grants `FILE` + full access to the DB
- Verifies a basic connection using your credentials

### Optional: Seed test data

You can populate the database with simple test data (tables will be created if missing). This is intended for local testing only.

```bash
chmod +x seed_database.sh
./seed_database.sh

# Or specify a custom env file
./seed_database.sh --config /path/to/your.env
```

What this does:
- Warns and asks for confirmation since it will alter the database
- Creates four tables if needed: `users`, `categories`, `products`, `orders`
- Inserts at least 100 random rows in each table (skips if already >= 100)
- Uses MySQL 8 features (recursive CTEs)


## Configuration

### Environment Variables

The script can be configured using environment variables. You can:
- Set them directly in your shell
- Use a `.env` file and source it
- Pass them when running the script

#### Required Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `MYSQL_USER` | MySQL username | `myuser` |
| `MYSQL_DATABASE` | MySQL database name | `mydatabase` |
| `SFTP_HOST` | SFTP server hostname | `sftp.example.com` |
| `SFTP_USER` | SFTP username | `sftpuser` |
| `SFTP_PASSWORD` or `SFTP_KEY_FILE` | SFTP authentication | `password123` or `/path/to/key` |

#### Optional Variables

| Variable | Description | Default |
|----------|-------------|---------||
| `SQL_DIR` | Directory containing .sql files | `sql/queries` |
| `OUTPUT_DIR` | Local directory for CSV generation | `/tmp/mysql_exports` |
| `MYSQL_HOST` | MySQL server hostname (local or remote) | `localhost` |
| `MYSQL_PORT` | MySQL server port | `3306` |
| `MYSQL_PASSWORD` | MySQL password | (empty) |
| `SFTP_PORT` | SFTP server port | `22` |
| `SFTP_REMOTE_DIR` | Remote directory on SFTP server | `/upload` |
| `LOG_FILE` | Path to log file | `/tmp/mysql_to_sftp.log` |
| `CSV_INCLUDE_HEADERS` | Prepend a header row with column names | `true` |

### MySQL Configuration

#### Remote MySQL Server

To connect to a remote MySQL server, simply configure the `MYSQL_HOST`, `MYSQL_PORT`, `MYSQL_USER`, and `MYSQL_PASSWORD` variables in your `.env` file.

#### Grant SELECT Privilege

```sql
GRANT SELECT ON your_database.* TO 'your_mysql_user'@'%';
FLUSH PRIVILEGES;
```

#### Local Output Directory

The local output directory (`OUTPUT_DIR`) will be created automatically if it doesn't exist. Make sure the user running the script has write permissions.

```bash
# Create the directory if needed
mkdir -p /tmp/mysql_exports
chmod 755 /tmp/mysql_exports
```

### CSV Format

The script generates CSV files with the following format:
- **Field Terminator**: `,` (comma)
- **Field Enclosure**: `"` (double quote)
- **Escape Character**: `"` (double quote - doubled for escaping)
- **Line Terminator**: `\n` (newline)
- **Encoding**: UTF-8 (utf8mb4)
- **Headers**: Included automatically with column names

All fields are properly quoted and escaped according to RFC 4180 CSV standard.

## Usage

### Basic Usage

```bash
# Run the script (auto-loads .env if present, else config.env)
./mysql_to_sftp.sh

# Or specify a custom env file
./mysql_to_sftp.sh --config /path/to/your.env
```

### Using Environment Variables Directly

```bash
MYSQL_USER=myuser \
MYSQL_PASSWORD=mypass \
MYSQL_DATABASE=mydb \
SFTP_HOST=sftp.example.com \
SFTP_USER=sftpuser \
SFTP_PASSWORD=sftppass \
./mysql_to_sftp.sh
```

### Using SSH Key for SFTP

```bash
# Set SFTP_KEY_FILE instead of SFTP_PASSWORD
export SFTP_KEY_FILE=/home/user/.ssh/id_rsa
./mysql_to_sftp.sh
```

## SQL Query Files

### File Location

Place your SQL query files in the directory specified by `SQL_DIR` (default: `sql/queries/`).

### File Format

- Each `.sql` file should contain a single `SELECT` query
- Do **NOT** include `INTO OUTFILE` in your queries (not used anymore)
- Trailing semicolons are optional (they will be removed if present)
- UTF-8 encoding is recommended

### Example Query Files

**sql/queries/users_export.sql:**
```sql
SELECT 
    id,
    name,
    email,
    created_at
FROM users
WHERE active = 1
ORDER BY created_at DESC
```

**sql/queries/orders_export.sql:**
```sql
SELECT 
    o.order_id,
    o.customer_id,
    o.order_date,
    o.total_amount,
    o.status
FROM orders o
WHERE o.order_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
ORDER BY o.order_date DESC
```

## How It Works

1. **Discovery**: The script finds all `.sql` files in the `SQL_DIR` directory
2. **Processing**: For each `.sql` file:
   - Reads and cleans the query (removes trailing semicolons)
   - Verifies the query doesn't already contain `INTO OUTFILE`
   - Executes the query on MySQL (local or remote) with UTF-8 encoding (`SET NAMES utf8mb4`)
   - Retrieves the data and generates a CSV file locally in the `OUTPUT_DIR` directory
   - Includes column headers in the first row
   - Properly escapes and quotes all fields
3. **Upload**: For each generated CSV file:
   - Uploads to SFTP as `filename.csv.part`
   - Renames to `filename.csv` (atomic operation)
4. **Logging**: All operations are logged to the specified log file

## Error Handling

The script includes comprehensive error handling:
- Configuration validation before execution
- Checks for existing output files (removes them before query execution)
- Verifies CSV file creation after query execution
- SFTP connection and upload error handling
- Detailed error logging

## Logging

All operations are logged to the file specified by `LOG_FILE` (default: `/tmp/mysql_to_sftp.log`).

Log entries include:
- Timestamp
- Operation being performed
- Success/failure status
- Error messages

## Atomic SFTP Upload

The script uses atomic uploads to prevent incomplete files on the SFTP server:

1. Upload file as `filename.csv.part`
2. Rename to `filename.csv` only after complete upload
3. This ensures consumers never see partially uploaded files

## Security Considerations

1. **Credentials**: Store credentials securely
   - Use environment variables or a protected `.env` file
   - Consider using SSH keys instead of passwords for SFTP
   - Set appropriate file permissions: `chmod 600 .env`

2. **MySQL FILE Privilege**: The `FILE` privilege is powerful
   - Only grant it to trusted users
   - Consider using a dedicated MySQL user for this script

3. **File Permissions**: Ensure proper permissions on:
   - The script file
   - Configuration files
   - Log files
   - MySQL output directory

## Troubleshooting

### MySQL: "Access denied for user"

**Solution**: Verify your MySQL credentials and ensure the user has SELECT privilege:
```sql
GRANT SELECT ON your_database.* TO 'your_user'@'%';
FLUSH PRIVILEGES;
```

### Cannot connect to remote MySQL server

**Solution**: Check:
1. MySQL server accepts remote connections (bind-address in my.cnf)
2. Firewall rules allow connection on port 3306
3. User has remote access permission (not just @localhost)

### SFTP: "Permission denied"

**Solution**: Verify SFTP credentials and remote directory permissions.

### SFTP: "sshpass: command not found"

**Solution**: Install sshpass or use SSH key authentication:
```bash
# Ubuntu/Debian
sudo apt-get install sshpass

# Or use SSH key
export SFTP_KEY_FILE=/path/to/your/key
```

### No CSV files generated

**Solution**: Check:
1. MySQL user has SELECT privilege on the database
2. Local output directory exists and is writable
3. Query executes successfully in MySQL
4. Log file for specific error messages

## Examples

### Scheduled Execution with Cron

```bash
# Run every day at 2 AM
0 2 * * * cd /path/to/mysql_to_ftp && source .env && ./mysql_to_sftp.sh >> /var/log/mysql_to_sftp_cron.log 2>&1
```

### Testing Individual Components

```bash
# Test MySQL connection (local or remote)
mysql -h$MYSQL_HOST -P$MYSQL_PORT -u$MYSQL_USER -p$MYSQL_PASSWORD $MYSQL_DATABASE -e "SELECT 1"

# Test SFTP connection
sftp -P $SFTP_PORT $SFTP_USER@$SFTP_HOST

# Test a simple query
mysql -h$MYSQL_HOST -P$MYSQL_PORT -u$MYSQL_USER -p$MYSQL_PASSWORD $MYSQL_DATABASE -e "SELECT * FROM your_table LIMIT 5"
```

## License

This project is open source and available under the MIT License.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Support

For issues and questions, please open an issue on GitHub.