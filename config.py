# config.py

import os
from dotenv import load_dotenv

load_dotenv()  # reads .env in your project root

# ── Database connection settings ───────────────────────────────────────────────
DB_SERVER = os.getenv("DB_SERVER")
DB_NAME   = os.getenv("DB_NAME")
DB_USER   = os.getenv("DB_USER")
DB_PASS   = os.getenv("DB_PASS")

DRIVER    = "{ODBC Driver 17 for SQL Server}"
CONN_STR  = (
    f"DRIVER={DRIVER};"
    f"SERVER={DB_SERVER},1433;"
    f"DATABASE={DB_NAME};"
    f"UID={DB_USER};PWD={DB_PASS};"
    "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
)

# ── Flask secret ────────────────────────────────────────────────────────────────
# You can override SECRET_KEY in .env if you like, otherwise a random one is generated.
SECRET_KEY = os.getenv("SECRET_KEY", os.urandom(24))
