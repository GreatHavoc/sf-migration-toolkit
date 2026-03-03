# Snowflake Migration Utility

[![Python](https://img.shields.io/badge/Python-3.8%2B-blue.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

> A utility for migrating and managing Snowflake database objects, agents, and semantic views, with backup and restore support.

---

## Table of Contents
- Features
- Project Structure
- Installation
- Usage
- Backup Folder
- Contributing
- License

## Features
- Command-line and Streamlit‑based migration tools (`main.py` and `mainv2.py`)
- Backup and restore Snowflake schemas, agents, semantic views, Streamlit apps, notebooks, and table data
- Migrate notebooks automatically via internal stages
- Support for Azure external stages with configurable storage integration
- Local backup/restore independent of cloud stages
- Environment variable support and optional MFA passcodes
- Easy-to-use Python/Streamlit scripts with interactive UI

## Project Structure
```
main.py, mainv2.py         # Migration scripts
pyproject.toml             # Project dependencies
sf_backup/                 # Database backups (ignored by git)
```

## Installation
1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/snowflake_migrate.git
   cd snowflake_migrate
   ```
2. (Optional) Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Usage
There are two entry points:

* `main.py` – a simpler migration script with a Streamlit UI for cross-account Snowflake migrations.
* `mainv2.py` – an enhanced version offering additional features such as notebook migration, local backup/restore, Azure stage configuration, and more interactive controls.

### Command-line
You can invoke either script directly from the shell. Connection parameters are taken from flags or environment variables:
```bash
python main.py [--account <account>] [--user <user>] [--password <pwd>]
python mainv2.py [options]
```
Supported environment variables:
- `SNOWFLAKE_ACCOUNT`
- `SNOWFLAKE_USER`
- `SNOWFLAKE_PASSWORD`
- `SNOWFLAKE_ROLE` (optional)
- `SNOWFLAKE_WAREHOUSE` (optional)
- `SNOWFLAKE_PASSCODE` (for MFA, mainv2 only)

Run with `--help` to see available flags:
```bash
python mainv2.py --help
```

### Streamlit UI
Both scripts can also be launched as a web application:
```bash
streamlit run main.py
# or
streamlit run mainv2.py
```

The UI allows you to:
- Enter source/target credentials and optional MFA passcodes
- Configure utility database/schema and Azure external stage settings
- Perform migrations of schemas, semantic views, agents, Streamlit apps, notebooks, and table data
- Execute local backups to the filesystem and restore from them
- Run copy operations via Azure stages for cross-account data movement

The `mainv2.py` UI adds tabs for **Local Backup & Restore**, **Migration Copy**, and more detailed controls.

### Examples
```bash
export SNOWFLAKE_ACCOUNT=myacct
export SNOWFLAKE_USER=admin
export SNOWFLAKE_PASSWORD=secret
python main.py              # basic migration
python mainv2.py            # starts interactive script
streamlit run mainv2.py     # full UI with backups & notebooks
```

Refer to the form help text in the UI for additional hints on fields and operations. You can also inspect the source code to see all configurable options.


## Backup Folder
- All backups are stored in the sf_backup directory.
- This folder is excluded from version control via .gitignore.
- Contains schemas, agents, data, and semantic views for each database.

## Contributing
Pull requests are welcome! For major changes, please open an issue first to discuss what you would like to change.

## License
This project is licensed under the MIT License.

---

If you want me to try a different approach to automate this update, let me know!