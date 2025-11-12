# MySQL to SFTP Extraction

A bash script that executes SQL queries from `.sql` files, generates CSV files using MySQL's `INTO OUTFILE`, and uploads them to an SFTP server using atomic operations.

## Features

- ✅ Executes SQL queries from `.sql` files on MySQL server
- ✅ Generates CSV files directly on MySQL server using `SELECT ... INTO OUTFILE`
- ✅ Atomic SFTP upload (upload as `.part` then rename)
- ✅ UTF-8 encoding support
- ✅ Configurable CSV format
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
- MySQL user with `FILE` privilege
- `secure_file_priv` configured (typically `/var/lib/mysql-files/`)
- The directory specified in `secure_file_priv` must:
  - Exist
  - Be writable by the MySQL server process
  - Be readable by the user running the script

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
|----------|-------------|---------|
| `SQL_DIR` | Directory containing .sql files | `sql/queries` |
| `OUTPUT_DIR` | MySQL output directory (secure_file_priv) | `/var/lib/mysql-files` |
| `MYSQL_HOST` | MySQL server hostname | `localhost` |
| `MYSQL_PORT` | MySQL server port | `3306` |
| `MYSQL_PASSWORD` | MySQL password | (empty) |
| `SFTP_PORT` | SFTP server port | `22` |
| `SFTP_REMOTE_DIR` | Remote directory on SFTP server | `/upload` |
| `LOG_FILE` | Path to log file | `/tmp/mysql_to_sftp.log` |

### MySQL Configuration

#### Check secure_file_priv Setting

```sql
SHOW VARIABLES LIKE 'secure_file_priv';
```

This will show the directory where MySQL can write files. Update your `OUTPUT_DIR` environment variable to match this path.

#### Grant FILE Privilege

```sql
GRANT FILE ON *.* TO 'your_mysql_user'@'localhost';
FLUSH PRIVILEGES;
```

#### Verify Directory Permissions

```bash
# The directory must exist and be writable by MySQL
sudo mkdir -p /var/lib/mysql-files
sudo chown mysql:mysql /var/lib/mysql-files
sudo chmod 755 /var/lib/mysql-files
```

### CSV Format

The script generates CSV files with the following format (as per specification):
- **Field Terminator**: `,` (comma)
- **Field Enclosure**: `"` (double quote, optional)
- **Escape Character**: `"` (double quote)
- **Line Terminator**: `\n` (newline)
- **Encoding**: UTF-8 (utf8mb4)

## Usage

### Basic Usage

```bash
# Source your configuration
source .env

# Run the script
./mysql_to_sftp.sh
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
- Do **NOT** include `INTO OUTFILE` in your queries (the script adds it automatically)
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
   - Adds the `INTO OUTFILE` clause with the specified CSV format
   - Executes the query on MySQL with UTF-8 encoding (`SET NAMES utf8mb4`)
   - Generates a CSV file in the `OUTPUT_DIR` directory
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

### MySQL: "The MySQL server is running with the --secure-file-priv option"

**Solution**: Set `OUTPUT_DIR` to match MySQL's `secure_file_priv` value:
```bash
mysql -e "SHOW VARIABLES LIKE 'secure_file_priv';"
```

### MySQL: "Access denied; you need (at least one of) the FILE privilege(s)"

**Solution**: Grant FILE privilege to your MySQL user:
```sql
GRANT FILE ON *.* TO 'your_user'@'localhost';
FLUSH PRIVILEGES;
```

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
1. MySQL has FILE privilege
2. `secure_file_priv` is configured correctly
3. Output directory exists and is writable by MySQL
4. Log file for specific error messages

## Examples

### Scheduled Execution with Cron

```bash
# Run every day at 2 AM
0 2 * * * cd /path/to/mysql_to_ftp && source .env && ./mysql_to_sftp.sh >> /var/log/mysql_to_sftp_cron.log 2>&1
```

### Testing Individual Components

```bash
# Test MySQL connection
mysql -h$MYSQL_HOST -P$MYSQL_PORT -u$MYSQL_USER -p$MYSQL_PASSWORD $MYSQL_DATABASE -e "SELECT 1"

# Test SFTP connection
sftp -P $SFTP_PORT $SFTP_USER@$SFTP_HOST

# Check secure_file_priv
mysql -h$MYSQL_HOST -P$MYSQL_PORT -u$MYSQL_USER -p$MYSQL_PASSWORD -e "SHOW VARIABLES LIKE 'secure_file_priv';"
```

## License

This project is open source and available under the MIT License.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Support

For issues and questions, please open an issue on GitHub.