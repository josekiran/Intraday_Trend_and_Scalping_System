#==================================#
### 1.0     Imports for the Project
#==================================#
import pandas as pd
from dhanhq import dhanhq
from dhanhq.marketfeed import DhanFeed
from datetime import datetime, timedelta, time
import asyncio
import pytz
import os, glob, json
import requests
import logging
import sys, io
import shutil
import threading
import tempfile

#========================================#
### 2.0 Setting Time Zone and Date  
#========================================#
##  2.1         Time and Date Variables
kolkata_tz = pytz.timezone('Asia/Kolkata')
current_date = datetime.now(kolkata_tz).strftime("%Y-%m-%d")  # fixed for day

#========================================#
### 3.0 Base directories for data and files
#========================================#

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "Data and Files")
PREVIOUS_RECORDS_DIR = os.path.join(BASE_DIR, "Previous_Records")
LOGS_DIR = os.path.join(BASE_DIR, "Logs")

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(PREVIOUS_RECORDS_DIR, exist_ok=True)
os.makedirs(LOGS_DIR, exist_ok=True)

# --- 🧩 Runtime and versioned data directories ---
RUNTIME_DIR = os.path.join(DATA_DIR, "runtime")
VERSIONS_DIR = os.path.join(DATA_DIR, "versions")

os.makedirs(RUNTIME_DIR, exist_ok=True)
os.makedirs(VERSIONS_DIR, exist_ok=True)

# --- 🧩 Create dated log filenames ---
debug_log_path = os.path.join(LOGS_DIR, f"debug_{current_date}.log")
app_log_path = os.path.join(LOGS_DIR, f"app_{current_date}.log")

# ==============================================================#
#  GLOBAL LOCK for POSITION MANAGEMENT
# ==============================================================#
POSITION_LOCK = threading.Lock()

# ==============================================================#
#  GLOBAL ASYNC LOCK for SMA Computation
# ==============================================================#
SMA_LOCK = asyncio.Lock()

#========================================#
### 2.0 Loggin Config 
#========================================#

# Force console to UTF-8
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# --- 🧩 Set up logging handlers ---
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

logging.Formatter.converter = lambda *args: datetime.now(kolkata_tz).timetuple()
debug_handler = logging.FileHandler(debug_log_path, encoding='utf-8')
debug_handler.setLevel(logging.DEBUG)
debug_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

info_handler = logging.FileHandler(app_log_path, encoding='utf-8')
info_handler.setLevel(logging.INFO)
info_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

logger.handlers.clear()
logger.addHandler(debug_handler)
logger.addHandler(info_handler)
logger.addHandler(console_handler)

# Optional: mute noisy libraries
logging.getLogger('websockets.protocol').setLevel(logging.INFO)
logging.getLogger('websockets.client').setLevel(logging.INFO)

# --- 📘 Separate handler for Position Management ---
position_log_path = os.path.join(LOGS_DIR, f"position_manager_{current_date}.log")
position_handler = logging.FileHandler(position_log_path, encoding='utf-8')
position_handler.setLevel(logging.INFO)
position_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

# Create a dedicated logger for position manager
position_logger = logging.getLogger("position_manager")
position_logger.setLevel(logging.INFO)
position_logger.addHandler(position_handler)
polog = logging.getLogger("position_manager")


#========================================#
### x.0 Snapshot File Save - Helper 
#========================================#
def save_with_snapshot(df, base_filename):
    """
    Save DataFrame atomically to runtime and also create
    a timestamped snapshot copy for audit and Excel review.
    """
    try:
        # 1️⃣ Runtime save (atomic write)
        runtime_path = os.path.join(RUNTIME_DIR, base_filename)
        tmpfile = tempfile.NamedTemporaryFile(dir=RUNTIME_DIR, delete=False, suffix=".tmp")
        tmpfile.close()
        df.to_csv(tmpfile.name, index=False, encoding="utf-8-sig")
        os.replace(tmpfile.name, runtime_path)
        logging.debug(f"💾 Runtime file saved → {runtime_path}")

        # 2️⃣ Timestamped snapshot copy
        ts = datetime.now(kolkata_tz).strftime("%Y-%m-%d_%H-%M-%S")
        snapshot_name = f"{os.path.splitext(base_filename)[0]}_{ts}.csv"
        snapshot_path = os.path.join(VERSIONS_DIR, snapshot_name)
        shutil.copy2(runtime_path, snapshot_path)
        logging.debug(f"📑 Snapshot created → {snapshot_path}")

        return True

    except Exception as e:
        logging.exception(f"❌ Error saving snapshot {base_filename}: {e}")
        return False

#========================================#
### 3.0    Client Code and Access Token
#========================================#
client_id = "1101823113"
api_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzY0Mzk2Nzg2LCJpYXQiOjE3NjQzMTAzODYsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTAxODIzMTEzIn0.sWjItwGyJy_dl1g6Q6cOoVCkrWu05Hb9oXKKLMyBquWlRIupcFb4iTuE8w16DY4_zOw_Uxdzm8bgSRL3G8TvPQ"
dhan = dhanhq(client_id, api_token)
version = "v2"

#================================================================================#
### 4.0    Global  Constants and Variables                      
#================================================================================#
# 4.2           Resampling and Signal Generation Variables
interval = 5
ssma_window = 5
lsma_window = 10
min_period = 2

# 4.3       Indicator and Candle Values 
ssma_Value = None               # updated every five minutes from get_intraday_data() and used to check_entry_conditions() 
lsma_Value = None               # updated every five minutes from get_intraday_data() and used to check_entry_conditions()  
close_value = None              # updated every five minutes from get_intraday_data() and used to check_entry_conditions() find_required_strikes(), buy_ce_position(), buy_pe_position(), check_entry_conditions() 
last_candle_time = None         # updated every five minutes from get_intraday_data() and used to check_entry_conditions()  
security_id_to_name = {}        # empty dict at startup

# ============================================================ #
# Phase 2 — Exit Logic Activation Parameters 
# ============================================================ #

base_req_fav_move = 0.0040        # 0.40% favorable movement
decay_factor = 0.72              # per-bucket decay multiplier
bucket_size = 5000               # underlying price bucket size
min_buckets = 1                  # never allow zero buckets
timeout_minutes = 15             # activation timeout

# ------------------------------------------------ #
# 🧭 Position Management 
# ------------------------------------------------ #
# 4.8       Position  
previous_close_values_map = {}    # {security_id: [list of closes]}             
subscribed_instruments = pd.DataFrame(columns=['SECURITY_ID', 'DISPLAY_NAME', 'STRIKE_PRICE', 'OPTION_TYPE', 'UNDERLYING_SECURITY_ID'])
LTP_subscribed_instruments = {}   # Saves latest price of subscibed instriments (incl tracked instriment). To be used to caculate limit price for entry/ exit order                                                
tradable_df = None                # will be filled after script_list()          
ltp_update_condition = asyncio.Condition()  
sl_exit_buffer = 0.50  # safe adjustment to avoid Dhan rejection


# ==============================================================
#  🧭 Position Manager: Parent Dictionary Structure
# ==============================================================

# GLOBAL / RUNTIME STATE INITIALIZATION
# (1) Define Position State Structure for CE/PE legs Tracking
def _init_position_state():
    """
    Initialize a clean position state for CE/PE legs.
    Focuses purely on position and order leg statuses, not prices.
    """
    return {
        "position": "Unknown",                     # Ready for entry | Entering | Open | Triggered | Exiting
        "securityId": None,
        "orderId": None,
        "quantity": None,
        "remainingQuantity": None,
        "orderStatus": None,                       # entry leg textual status (e.g. TRADED / OPEN / CANCELLED)
        "STOP_LOSS_LEG_remainingQuantity": None,
        "TARGET_LEG_remainingQuantity": None,
        "STOP_LOSS_LEG_status": None,
        "TARGET_LEG_status": None,
        "last_updated": None,
        "note": None,
        # --- Phase 2 fields ---
        "exit_logic_active": False,                # SSMA-based exit monitoring is OFF initially
        "entry_timestamp": None,                   # Time of entry
        "entry_underlying_price": None             # Underlying LTP at entry
    }

# (2) Initialize runtime position status for both legs
position_status = {
    "CE": _init_position_state(),
    "PE": _init_position_state()
}

# 4.5    Order Management Parameters
quantity = 1    # Default trade quantity per order (configurable)

#================================================================================#
### 4.0    User Config for System Autoconfiguration
#================================================================================#
## 4.1   Exchange and Underlying Instrument
exchange = "MCX"        # "NSE" or "MCX"
underlying = "CRUDEOILM"    # NIFTY, BANKNIFTY, GOLD, NATURALGAS etc.

#================================================================================#
### 4.0    System Autoconfiguration and Global  Constants and Variables                      
#================================================================================#

## 4.3 System Autoconfiguration
def auto_config(exchange, underlying, current_date):
    master_file = os.path.join(DATA_DIR, f"api-scrip-master-detailed_{current_date}.csv")

    if os.path.exists(master_file):
        df = pd.read_csv(master_file, low_memory=False)
        # print(f"Using cached master file: {master_file}")
        logging.info("Using cached master file: %s", master_file)
    else:
        master_url = 'https://images.dhan.co/api-data/api-scrip-master-detailed.csv'
        df = pd.read_csv(master_url, low_memory=False)
        df.to_csv(master_file, index=False)
        # print(f"Downloaded and saved master file: {master_file}")
        logging.info("Downloaded and saved master file: %s", master_file)

    Exchange_to_Trade = exchange.upper()
    Underlying_Symbol = underlying.upper()

    if Exchange_to_Trade == "NSE" and Underlying_Symbol == "NIFTY":
        security_id_tracked = "13"
        instrument_type = "INDEX"
        exchange_segment = "IDX_I"
    else:
        df = df[(df['EXCH_ID'] == Exchange_to_Trade) &
                (df['UNDERLYING_SYMBOL'].str.upper() == Underlying_Symbol)]
        if df.empty:
            raise ValueError(f"No instrument found for {Exchange_to_Trade}/{Underlying_Symbol}.")
        row = df.head(1).iloc[0]
        instr_type_col = str(row.get('INSTRUMENT_TYPE') or row.get('INSTRUMENT') or '').upper()

        if 'FUTCOM' in instr_type_col:
            instrument_type = 'FUTCOM'
            exchange_segment = 'MCX_COMM'
        elif 'FUTIDX' in instr_type_col:
            instrument_type = 'FUTIDX'
            exchange_segment = 'NSE_FNO'
        elif 'INDEX' in instr_type_col:
            instrument_type = 'INDEX'
            exchange_segment = 'IDX_I'
        else:
            instrument_type = instr_type_col
            exchange_segment = row['EXCH_ID']

        security_id_tracked = str(row['SECURITY_ID'])

    instrument = [(exchange_segment, security_id_tracked)]

    if Exchange_to_Trade == "NSE":
        market_times = (9, 15, 15, 30, 14, 45, 15, 15)
        exchange_segment_tradable = "NSE_FNO"
    else:
        market_times = (9, 0, 23, 30, 23, 30, 23, 15)
        exchange_segment_tradable = "MCX_COMM"

    return {
        "security_id_tracked": security_id_tracked,
        "exchange_segment": exchange_segment,
        "instrument_type": instrument_type,
        "instrument": instrument,
        "Exchange_to_Trade": Exchange_to_Trade,
        "Underlying_Symbol": Underlying_Symbol,
        "exchange_segment_tradable": exchange_segment_tradable,
        "market_times": market_times
    }

cfg = auto_config(exchange, underlying, current_date)

## 4.3      Tracked Instrument  
security_id_tracked    = cfg["security_id_tracked"]                
exchange_segment       = cfg["exchange_segment"]                    
instrument_type        = cfg["instrument_type"]                     
instrument             = cfg["instrument"]

# 🟢 initialise subscribed_instruments and LTP_subscribed_instruments here
subscribed_instruments.loc[len(subscribed_instruments)] = [int(security_id_tracked), '', 0, '', '']
LTP_subscribed_instruments[int(security_id_tracked)] = {'LTP': None}

# print(subscribed_instruments)
logging.info("\n%s", subscribed_instruments)

## 4.3      Tradable Instruments  
Exchange_to_Trade      = cfg["Exchange_to_Trade"]                   
Underlying_Symbol      = cfg["Underlying_Symbol"]                   
exchange_segment_tradable = cfg["exchange_segment_tradable"]        

# 4.4       Market Hours
startH,startM,closeH,closeM,entryEndH,entryEndM,exitH,exitM = cfg["market_times"]

#========================================#
### 5.0    Clean up / archive of base directories
#========================================#
# 4.2   Clean up old cached files (master + tradable list)
def cleanup_old_files(current_date):
    """
    Moves previous day's data and log files into a dated folder inside 'Previous_Records'.
    Keeps only current day's files in 'Data and Files' and 'Logs'.
    """
    # 🟢 Move data files
    patterns = [
        'api-scrip-master-detailed_*.csv',
        'Tradable_Instruments_List_*.csv',
        'Intraday_Data_*.csv',
        'Positions_*.csv',                # ✅ new
        'Super_Order_List_*.csv'            # ✅ new
    ]

    for pattern in patterns:
        for file_path in glob.glob(os.path.join(DATA_DIR, pattern)):
            filename = os.path.basename(file_path)
            parts = filename.rsplit('_', 1)
            if len(parts) == 2:
                file_date = parts[1].replace(".csv", "")
                if file_date != current_date:
                    archive_dir = os.path.join(PREVIOUS_RECORDS_DIR, f"Archived_{file_date}")
                    os.makedirs(archive_dir, exist_ok=True)
                    shutil.move(file_path, os.path.join(archive_dir, filename))
                    # print(f"Moved old file to {archive_dir}/{filename}")
                    logging.info("Moved old file to %s/%s", archive_dir, filename)

    # 🟢 Move log files (from Logs/ folder)
    for log_path in glob.glob(os.path.join(LOGS_DIR, "*.log")):
        filename = os.path.basename(log_path)
        # e.g., app_2025-10-06.log
        if "_" in filename:
            log_date = filename.split("_")[-1].replace(".log", "")
            if log_date != current_date:
                archive_dir = os.path.join(PREVIOUS_RECORDS_DIR, f"Archived_{log_date}")
                os.makedirs(archive_dir, exist_ok=True)
                shutil.move(log_path, os.path.join(archive_dir, filename))
                # print(f"Moved old log file to {archive_dir}/{filename}")
                logging.info("Moved old log file to %s/%s", archive_dir, filename)

    logging.info("Previous data and log files moved to %s.", PREVIOUS_RECORDS_DIR)

# cleanup_old_files(current_date)

def archive_previous_snapshots():
    """
    Move existing runtime and versioned CSVs into Previous_Records/<date>/.
    Logs every step with file counts and destination path.
    """
    try:
        today = datetime.now(kolkata_tz).strftime("%Y-%m-%d")
        archive_dir = os.path.join(PREVIOUS_RECORDS_DIR, f"Archived_{today}")
        os.makedirs(archive_dir, exist_ok=True)

        total_moved = 0
        skipped = 0

        logging.info("🧹 Starting daily archive cleanup for runtime/version snapshots...")

        for subdir in [RUNTIME_DIR, VERSIONS_DIR]:
            if not os.path.exists(subdir):
                logging.info(f"ℹ️ Directory not found — skipping: {subdir}")
                continue

            moved_count = 0
            files = os.listdir(subdir)
            if not files:
                logging.info(f"✅ No files to archive in {subdir}")
                continue

            for fname in files:
                src = os.path.join(subdir, fname)
                dst = os.path.join(archive_dir, fname)
                try:
                    shutil.move(src, dst)
                    moved_count += 1
                    total_moved += 1
                except Exception as e:
                    skipped += 1
                    logging.warning(f"⚠️ Could not move {src}: {e}")

            logging.info(f"📦 Moved {moved_count} files from {subdir} → {archive_dir}")

        if total_moved > 0:
            logging.info(f"✅ Archived total {total_moved} files → {archive_dir}")
        else:
            logging.info("🟢 No runtime/version files found for archival today.")

        if skipped > 0:
            logging.warning(f"⚠️ Skipped {skipped} files due to errors or locks.")

    except Exception as e:
        logging.exception(f"❌ Error during archive_previous_snapshots(): {e}")

#===============================================================#
### STATE VARIABLES CLEARING FUNCTION (for runtime memory only)
#===============================================================#
def clear_state_variables():
    """
    Clears all runtime and computed state and in-memory variables only (does NOT delete CSVs).
    Uses _init_position_state() to ensure structural consistency for CE/PE legs.
    Thread-safe via POSITION_LOCK.
    """
    with POSITION_LOCK:
        global ssma_Value, lsma_Value, close_value, last_candle_time
        global previous_close_values_map
        global subscribed_instruments, LTP_subscribed_instruments
        global position_status, security_id_to_name, tradable_df
        global security_id_tracked

        logging.info("🧹 Clearing runtime Algo state variables (no files)...")

        # 1️⃣ Reset indicator values
        ssma_Value = None
        lsma_Value = None
        close_value = None
        last_candle_time = None

        # 2️⃣ Clear rolling data / indicators
        try:
            previous_close_values_map.clear()
        except Exception:
            previous_close_values_map = {}

        # 3️⃣ Reset CE/PE position states (fresh init)
        position_status = {
            "CE": _init_position_state(),
            "PE": _init_position_state()
        }
        position_status["CE"]["position"] = "Ready for entry"
        position_status["PE"]["position"] = "Ready for entry"

        # 4️⃣ Reset subscribed instruments (keep tracked instrument only)
        try:
            subscribed_instruments = pd.DataFrame([{
                "SECURITY_ID": int(security_id_tracked),
                "DISPLAY_NAME": "",
                "STRIKE_PRICE": 0,
                "OPTION_TYPE": "",
                "UNDERLYING_SECURITY_ID": ""
            }])
        except Exception as e:
            logging.warning("Error rebuilding subscribed_instruments: %s", e)
            subscribed_instruments = pd.DataFrame(columns=[
                "SECURITY_ID", "DISPLAY_NAME", "STRIKE_PRICE",
                "OPTION_TYPE", "UNDERLYING_SECURITY_ID"
            ])

        # 5️⃣ Reset LTP cache for tracked instrument
        try:
            LTP_subscribed_instruments.clear()
        except Exception:
            LTP_subscribed_instruments = {}
        LTP_subscribed_instruments[int(security_id_tracked)] = {'LTP': None}

        # 6️⃣ Clear symbol name map and tradable_df
        try:
            security_id_to_name.clear()
        except Exception:
            security_id_to_name = {}
        tradable_df = None

        logging.info("✅ Runtime variables, caches, and state cleared successfully.")

#========================================#
### 5.0    Tradeable Instruments 
#========================================#
def script_list(exchange, underlying, current_date):
    """Build a fresh tradable options list for the given exchange/underlying."""
    # 1. delete old tradable list files
    for file in os.listdir(DATA_DIR):
        if file.startswith("Tradable_Instruments_List_") and file.endswith(".csv"):
            try:
                os.remove(os.path.join(DATA_DIR, file))
            except Exception as e:
                # print(f"Could not delete {file}: {e}")
                logging.warning("Could not delete %s: %s", file, e)

    # 2. prepare new file name
    file_name = f"Tradable_Instruments_List_{current_date}.csv"
    file_path = os.path.join(DATA_DIR, file_name)

    # 3. load master file
    master_file = os.path.join(DATA_DIR, f"api-scrip-master-detailed_{current_date}.csv")
    if os.path.exists(master_file):
        script_data = pd.read_csv(master_file, low_memory=False)
        # print(f"Using cached master file: {master_file}")
        logging.info("Using cached master file: %s", master_file)
    else:
        url = 'https://images.dhan.co/api-data/api-scrip-master-detailed.csv'
        script_data = pd.read_csv(url, low_memory=False)
        script_data.to_csv(master_file, index=False)
        # print(f"Downloaded and saved master file: {master_file}")
        logging.info("Downloaded and saved master file: %s", master_file)

    Exchange_to_Trade = exchange.upper()
    Underlying_Symbol = underlying.upper()

    # 4. filter by exchange + underlying
    script_data = script_data[script_data['EXCH_ID'] == Exchange_to_Trade]
    script_data = script_data[script_data['UNDERLYING_SYMBOL'].str.upper() == Underlying_Symbol]

    # 5. keep only options (CE/PE)
    if 'INSTRUMENT' in script_data.columns:
        instr_col = 'INSTRUMENT'
    elif 'INSTRUMENT_TYPE' in script_data.columns:
        instr_col = 'INSTRUMENT_TYPE'
    else:
        instr_col = None

    if instr_col:
        script_data = script_data[
            script_data[instr_col].str.contains('OPT', case=False, na=False)
            & script_data['OPTION_TYPE'].isin(['CE', 'PE'])
        ]

    # 6. filter ONLY the nearest expiry (but skip today's expiry)
    now = datetime.now(kolkata_tz)
    today = pd.to_datetime(now.date())

    # Normalize expiry column
    script_data['SM_EXPIRY_DATE'] = pd.to_datetime(script_data['SM_EXPIRY_DATE'], errors='coerce')

    # Keep only future expiries (strictly greater than today)
    future_expiries = script_data[script_data['SM_EXPIRY_DATE'].dt.date > today.date()]

    # If no future expiries exist, fail gracefully
    if future_expiries.empty:
        logging.error("No future expiries found for %s %s", exchange, underlying)
        script_data = future_expiries  # becomes empty
    else:
        # Pick the nearest expiry date
        nearest_expiry = future_expiries['SM_EXPIRY_DATE'].min()

        # Filter only that expiry
        script_data = future_expiries[
            future_expiries['SM_EXPIRY_DATE'] == nearest_expiry
        ]

    logging.info("Using nearest expiry: %s", nearest_expiry.date() if 'nearest_expiry' in locals() else "None")

    # 7. trim columns and save
    script_data = script_data[['SECURITY_ID','DISPLAY_NAME','STRIKE_PRICE','OPTION_TYPE','UNDERLYING_SECURITY_ID','LOT_SIZE']]

    # 🔹 NEW: fix underlying IDs for NSE index options
    if Exchange_to_Trade == "NSE":
        if Underlying_Symbol == "NIFTY":
            script_data['UNDERLYING_SECURITY_ID'] = 13
        elif Underlying_Symbol == "BANKNIFTY":
            script_data['UNDERLYING_SECURITY_ID'] = 25

    script_data.to_csv(file_path, index=False)
    # print(f"Saved new tradable instruments list: {file_path}")
    logging.info("Saved new tradable instruments list: %s", file_path)

# script_list()

#========================================#
### 6.0    Intraday Data and SMA Values (Async, Dhan SDK)
#========================================#
async def get_intraday_data():
    """
    Asynchronously fetches latest intraday data for the tracked instrument
    using the existing Dhan SDK (executed in a background thread),
    computes SMAs, updates globals, and saves to CSV.

    This version avoids blocking the async event loop.
    
    Handles all possible response formats from Dhan API:
    - list of dicts ✅
    - dict of lists ✅
    - single dict of scalar values ✅

    """
    global ssma_Value, lsma_Value, close_value, last_candle_time, previous_close_values_map
    
    previous_values = None

    try:
        #---------------------------------------------------------------#
        # 🕒 1️⃣  Define time window for fetching intraday data
        #---------------------------------------------------------------#
        now = datetime.now(kolkata_tz)
        from_date = f"{current_date} {startH:02}:{startM:02}:00"
        to_date = now.strftime("%Y-%m-%d %H:%M:%S")

        #---------------------------------------------------------------#
        # 📡 2️⃣  Fetch intraday data via Dhan SDK (in executor)
        #---------------------------------------------------------------#
        loop = asyncio.get_running_loop()
        data = await loop.run_in_executor(
            None,
            lambda: dhan.intraday_minute_data(
                security_id_tracked,
                exchange_segment,
                instrument_type,
                from_date,
                to_date,
                interval
            )
        )

        #---------------------------------------------------------------#
        # 🔍 3️⃣  Extract and validate the data payload
        #---------------------------------------------------------------#
        rows = data.get('data') or []
        if not rows:
            logging.warning("No intraday data returned yet.")
            return

        if isinstance(rows, dict):
            if all(not isinstance(v, (list, tuple)) for v in rows.values()):
                df = pd.DataFrame([rows])
            else:
                df = pd.DataFrame.from_dict(rows)
        elif isinstance(rows, list):
            df = pd.DataFrame(rows)
        else:
            logging.error("Unexpected intraday data format: %s", type(rows))
            return

        # Log a small sample for diagnostics
        logging.debug("Raw intraday data sample: %s", str(rows)[:500])

        if df.empty:
            logging.warning("DataFrame empty after parsing.")
            return

        #---------------------------------------------------------------#
        # 🕓 4️⃣  Convert timestamp and clean dataframe
        #---------------------------------------------------------------#
        if 'timestamp' not in df.columns:
            logging.error("Missing 'timestamp' in intraday data.")
            return

        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s', errors='coerce')
        df = df.dropna(subset=['timestamp', 'close'])
        df = df.rename(columns={'timestamp': 'Date'})
        df['Date'] = df['Date'].dt.tz_localize('UTC').dt.tz_convert('Asia/Kolkata')
        df = df.set_index('Date').sort_index()

        #---------------------------------------------------------------#
        # 📈 5️⃣  Compute rolling SMA indicators
        #---------------------------------------------------------------#
        df['ssma'] = df['close'].rolling(window=ssma_window, min_periods=min_period).mean()
        df['lsma'] = df['close'].rolling(window=lsma_window, min_periods=min_period).mean()

        # ✅ Acquire SMA_LOCK before updating globals
        async with SMA_LOCK:
            logging.debug("🔒 SMA_LOCK acquired in get_intraday_data()")

            # ✅ Always set close_value, even if SMA unavailable
            close_value = round(df['close'].iloc[-1], 2)
            last_candle_time = df.index[-1].to_pydatetime()
            logging.info("Updated close_value=%s (even without SMA) for strike subscription readiness.", close_value)

            # 🧮 Try to compute SMA if possible, else fallback
            if df['ssma'].isna().all() or df['lsma'].isna().all():
                logging.warning("Not enough candles for SMA — using close_value only for strike subscription.")
                ssma_Value = None
                lsma_Value = None
            else:
                ssma_Value = round(df['ssma'].iloc[-1], 2)
                lsma_Value = round(df['lsma'].iloc[-1], 2)

            #---------------------------------------------------------------#
            # 🧮 7️⃣  Save last N close values (including latest)
            #---------------------------------------------------------------#
            N = lsma_window  # ✅ ensure enough values for long SMA
            num_prev = N

            if len(df) >= 1:
                # Take the last N rows including the latest close
                prev_df = df['close'].iloc[-num_prev:]
                prev_df = prev_df.round(2).reset_index()
                previous_values = dict(
                    zip(prev_df['Date'].dt.strftime('%Y-%m-%d %H:%M:%S'), prev_df['close'])
                )
            else:
                previous_values = None

            previous_close_values_map[security_id_tracked] = previous_values

        logging.debug("🔓 SMA_LOCK released in get_intraday_data()")

        #---------------------------------------------------------------#
        # 🧾 8️⃣  Logging
        #---------------------------------------------------------------#
        if previous_values:
            logging.info(
                "Stored %s previous close values for %s: %s",
                len(previous_values),
                security_id_tracked,
                previous_values
            )
        else:
            logging.info("No previous close values available for %s.", security_id_tracked)

        #---------------------------------------------------------------#
        # 🔎 9️⃣  Display recent SMA snapshot for verification
        #---------------------------------------------------------------#
        logging.debug("\n%s", df[['close', 'ssma', 'lsma']].tail(5))
        logging.info(
            "SSMA: %s LSMA: %s Close: %s Last Candle: %s",
            ssma_Value, lsma_Value, close_value, last_candle_time
        )

        #---------------------------------------------------------------#
        # 💾 🔚 10️⃣  Save complete intraday dataframe to CSV
        #---------------------------------------------------------------#
        save_with_snapshot(df.reset_index(), "Intraday_Data.csv")
        logging.info("💾 Intraday data saved to runtime + version snapshot.")

        logging.info("Intraday file rowcount=%s last index=%s", len(df), df.index[-1])

    except Exception as e:
        logging.exception("Error in get_intraday_data(): %s", str(e))

    ## Need to add entry signals in this to check if trades are taken correctly (and later comment and keep it)

# get_intraday_data()

#========================================#
### 6.1    Get Positions from Dhan (REST)
#========================================#
def get_positions():
    """
    Fetches all positions (open + closed) from Dhan API.
    Saves the full DataFrame to 'Data and Files' directory.
    Returns only *open* positions (LONG / SHORT) for live logic.
    """
    try:
        positions = dhan.get_positions()

        if not isinstance(positions, dict) or "data" not in positions:
            logging.error("❌ Invalid response from Dhan API: %s", positions)
            df = pd.DataFrame()  # ensure df is defined even on invalid response
            save_with_snapshot(df, "Positions.csv")  # save empty snapshot for audit
            return df

        data_list = positions.get("data", [])
        if not data_list:
            logging.info("⚠️ No positions available from Dhan.")
            df = pd.DataFrame()  # still define df
            save_with_snapshot(df, "Positions.csv")  # ✅ always snapshot, even if empty
            logging.info("📁 Empty Positions snapshot saved for audit.")
            return df

        # Convert to DataFrame
        df = pd.DataFrame(data_list)
        logging.info("✅ Retrieved %d positions from Dhan.", len(df))

        # Normalize case
        if "positionType" in df.columns:
            df["positionType"] = df["positionType"].astype(str).str.upper().str.strip()

        # ============================================================
        #  FILTER OUT CLOSED POSITIONS
        # ============================================================
        before = len(df)
        if "positionType" in df.columns:
            live_df = df[df["positionType"].isin(["LONG", "SHORT"])].copy()
            removed = before - len(live_df)
            logging.info(
                f"🧹 Filtered {removed} CLOSED positions. "
                f"Active positions: {len(live_df)}"
            )
        else:
            logging.warning("⚠️ positionType column missing — skipping filter.")
            live_df = df

        # Save complete (unfiltered) data for auditing
        os.makedirs(DATA_DIR, exist_ok=True)
        save_with_snapshot(df, "Positions.csv")
        logging.info("📁 Positions saved to runtime + version snapshot.")

        logging.info("Positions file rowcount=%s", len(df))

        # Optionally preview the active subset
        logging.debug("Active Positions:\n%s", live_df.head())

        return live_df

    except Exception as e:
        logging.exception("❌ Error while fetching or saving positions: %s", e)
        df = pd.DataFrame()
        save_with_snapshot(df, "Positions.csv")  # snapshot even on exception
        return df

# get_positions()

#===============================================#
### 6.2    Get Super Order List from Dhan (REST)
#===============================================#
def get_super_order_list():
    """
    Fetches the list of Super Orders from Dhan API.
    Saves the file to 'Data and Files' directory and logs a summary.
    Returns only *active/live* orders (filters out CLOSED / REJECTED / CANCELLED).
    """
    url = "https://api.dhan.co/v2/super/orders"
    headers = {
        "Content-Type": "application/json",
        "access-token": api_token
    }

    try:
        response = requests.get(url, headers=headers, timeout=10)

        if response.status_code != 200:
            logging.error("❌ API Error: %s — %s", response.status_code, response.text)
            df = pd.DataFrame()
            save_with_snapshot(df, "Super_Order_List.csv")
            return df


        data = response.json()
        if not isinstance(data, list) or not data:
            logging.warning("⚠️ No Super Order data returned.")
            df = pd.DataFrame()
            save_with_snapshot(df, "Super_Order_List.csv")
            return df

        # Convert to DataFrame
        df = pd.DataFrame(data)
        logging.info("✅ Retrieved %d Super Orders from Dhan.", len(df))

        # Convert all ID-like columns to string to avoid precision loss
        id_columns = [col for col in df.columns if "orderId" in col or "Id" in col]
        for col in id_columns:
            df[col] = df[col].astype(str)

        # --- Parse legDetails into separate columns ---
        if "legDetails" in df.columns:
            def extract_leg_value(legs, leg_name, field):
                for leg in legs:
                    if leg.get("legName") == leg_name:
                        return leg.get(field)
                return None

            # Safely parse legDetails JSON
            df["legDetails_parsed"] = df["legDetails"].apply(
                lambda x: x if isinstance(x, list)
                else (json.loads(x) if isinstance(x, str) else [])
            )

            # Extract fields for both legs
            for leg_name in ["STOP_LOSS_LEG", "TARGET_LEG"]:
                for field in ["orderId", "price", "orderStatus", "remainingQuantity", "transactionType"]:
                    col_name = f"{leg_name}_{field}"
                    df[col_name] = df["legDetails_parsed"].apply(
                        lambda legs: extract_leg_value(legs, leg_name, field)
                    )

            # Drop parsed intermediates
            df.drop(columns=["legDetails_parsed", "legDetails"], inplace=True, errors="ignore")

        # ============================================================
        #  FILTER OUT TERMINAL / CLOSED / DEAD ORDERS
        # ============================================================
        if "orderStatus" in df.columns:
            TERMINAL_STATUSES = {"CLOSED", "REJECTED", "CANCELLED"}
            df["orderStatus"] = df["orderStatus"].astype(str).str.upper().str.strip()

            # Identify filtered (dead) orders for logging
            dead_orders = df[df["orderStatus"].isin(TERMINAL_STATUSES)]
            if not dead_orders.empty:
                logging.info(
                    f"🧹 Ignoring {len(dead_orders)} closed/dead orders: "
                    f"{dead_orders['orderStatus'].value_counts().to_dict()}"
                )

            # Keep only live orders
            df = df[~df["orderStatus"].isin(TERMINAL_STATUSES)].copy()
            logging.info(f"Filtered live orders: {len(df)} remain after cleanup")
        else:
            logging.warning("⚠️ orderStatus column not found — skipping filter.")

        # --- Save to Data and Files directory ---
        os.makedirs(DATA_DIR, exist_ok=True)
        save_with_snapshot(df, "Super_Order_List.csv")
        logging.info("📁 Super Order List saved to runtime + version snapshot.")

        logging.info("Super Order file rowcount=%s", len(df))

        # --- Log summary counts ---
        if "orderStatus" in df.columns:
            summary = df["orderStatus"].value_counts()
            logging.info("📊 Super Order Status Summary:\n%s", summary)
        else:
            logging.warning("orderStatus column not found in Super Order list.")

        leg_cols = [col for col in df.columns if "LEG_orderStatus" in col]
        if leg_cols:
            for col in leg_cols:
                counts = df[col].value_counts()
                logging.info("▶ %s:\n%s", col, counts.to_string())
        else:
            logging.warning("No leg detail columns found for summary.")

        logging.info("✅ Super Order list fetch and save completed successfully.")
        return df

    except requests.exceptions.RequestException as e:
        logging.exception("🌐 Network Error while fetching Super Order List: %s", e)
        return pd.DataFrame()
    
    except Exception as e:
        logging.exception("❌ Unexpected Error while fetching Super Order List: %s", e)
        df = pd.DataFrame()
        save_with_snapshot(df, "Super_Order_List.csv")
        return df

# get_super_order_list()

# ==============================================================#
#  Helper Functions for Reconciliation of Orders and Positions
# ==============================================================#

def _safe_col_choice(df, candidates):
    """Return first candidate col present in df or None."""
    if df is None or df.empty:
        return None
    for c in candidates:
        if c in df.columns:
            return c
    return None


def cancel_super_order_leg(order_id, order_leg):
    """
    Cancel a specific leg of a Dhan Super Order.
    Used for:
      - Startup cleanup: orphan SL/TG legs
      - Mid/End candle cleanup: pending entry legs
    """
    global api_token, kolkata_tz

    if not order_id or not order_leg:
        logging.warning("⚠️ cancel_super_order_leg() called without order_id or order_leg.")
        return False, "invalid_parameters"

    allowed_legs = ["ENTRY_LEG", "STOP_LOSS_LEG", "TARGET_LEG"]
    if order_leg not in allowed_legs:
        logging.warning("⚠️ Invalid leg '%s' passed to cancel_super_order_leg()", order_leg)
        return False, f"invalid_leg: {order_leg}"

    url = f"https://api.dhan.co/v2/super/orders/{order_id}/{order_leg}"
    headers = {"accept": "application/json", "access-token": api_token}

    logging.info("🟡 Attempting cancel: orderId=%s | leg=%s", order_id, order_leg)

    try:
        response = requests.delete(url, headers=headers, timeout=8)
        if response.status_code == 200:
            try:
                resp_json = response.json()
            except ValueError:
                resp_json = {"status": "cancelled_no_json"}
            logging.info("🟢 Cancel successful — orderId=%s | leg=%s", order_id, order_leg)
            return True, resp_json

        elif response.status_code in (400, 404):
            logging.warning("⚠️ Cancel failed — orderId=%s | leg=%s | %s",
                            order_id, order_leg, response.text)
            return False, response.text

        else:
            logging.error("❌ Cancel failed — orderId=%s | leg=%s | Status=%s | Response=%s",
                          order_id, order_leg, response.status_code, response.text)
            return False, response.text

    except Exception as e:
        logging.exception("❌ Exception in cancel_super_order_leg(%s, %s): %s", order_id, order_leg, e)
        return False, f"exception: {e}"


def _find_order_row_by_orderid(orders_df, order_id):
    """Locate a specific order row by orderId in a dataframe."""
    if orders_df is None or orders_df.empty or not order_id:
        return None
    order_str = str(order_id)
    for col in orders_df.columns:
        try:
            if orders_df[col].astype(str).str.contains(order_str, na=False).any():
                return orders_df[orders_df[col].astype(str).str.contains(order_str, na=False)].iloc[0]
        except Exception:
            continue
    return None


def _rows_for_option_type(df, tradable_df, option_type):
    """Return rows from df matching SECURITY_IDs in tradable_df of given option_type."""
    if df is None or df.empty:
        return pd.DataFrame()
    try:
        ids = tradable_df[tradable_df['OPTION_TYPE'] == option_type]['SECURITY_ID'].astype(str).tolist()
        if not ids:
            return pd.DataFrame()
    except Exception:
        return pd.DataFrame()
    sec_col = _safe_col_choice(df, ["securityId", "SECURITY_ID", "security_id", "SecurityId", "SecurityID"])
    if sec_col is None:
        return pd.DataFrame()
    return df[df[sec_col].astype(str).isin(ids)].copy()


def _safe_str_from_df(df, col_candidates):
    """Return the first string value from first available candidate column."""
    if df is None or df.empty:
        return None
    col = _safe_col_choice(df, col_candidates)
    if col is None:
        return None
    try:
        v = df[col].astype(str).iloc[0]
        return v
    except Exception:
        return None


def _filter_leg_positions(positions_df, tradable_df, option_type):
    """Return position rows corresponding to CE/PE using SECURITY_IDs from tradable_df."""
    try:
        return _rows_for_option_type(positions_df, tradable_df, option_type)
    except Exception:
        return pd.DataFrame()


def _filter_leg_orders(orders_df, tradable_df, option_type):
    """Return order rows corresponding to CE/PE using SECURITY_IDs from tradable_df."""
    try:
        return _rows_for_option_type(orders_df, tradable_df, option_type)
    except Exception:
        return pd.DataFrame()


def _extract_order_leg_statuses(order_row):
    """
    Extract order leg statuses from a Dhan super order row (Series or dict).
    Returns dict with keys: orderId, orderStatus, STOP_LOSS_LEG_orderStatus, TARGET_LEG_orderStatus.
    """
    od = {}
    try:
        od["orderId"] = order_row.get("orderId") or order_row.get("ORDER_ID") or order_row.get("order_id") or None
        od["orderStatus"] = order_row.get("orderStatus") or order_row.get("ORDER_STATUS") or None
        od["STOP_LOSS_LEG_orderStatus"] = order_row.get("STOP_LOSS_LEG_orderStatus") or order_row.get("STOP_LOSS_LEG_status") or None
        od["TARGET_LEG_orderStatus"] = order_row.get("TARGET_LEG_orderStatus") or order_row.get("TARGET_LEG_status") or None
    except Exception:
        pass
    return od


def _is_order_stale(order_row, cutoff_seconds):
    """
    Determine if an order is stale using create/update/exchange timestamps.
    Robust for both dict and pd.Series rows.
    Returns True if:
      - Creation time older than cutoff_seconds, AND
      - Last update older than 60 seconds.
    """
    try:
        # --- Candidate field sets ---
        create_keys = ["createTime", "createdAt", "orderCreatedTime", "create_time", "create_time_str"]
        update_keys = ["updateTime", "updatedAt", "modifiedAt", "exchangeTime"]

        def _get_value(row, keys):
            """Try each key across dict/Series index."""
            for k in keys:
                if isinstance(row, dict) and k in row:
                    return row[k]
                elif isinstance(row, pd.Series) and k in row.index:
                    return row[k]
            return None

        def _to_dt(val):
            """Parse timestamp robustly; supports multiple date formats."""
            if val is None or val == "":
                return None
            try:
                dt = pd.to_datetime(val, dayfirst=True, errors='coerce')
                if pd.isna(dt):
                    dt = pd.to_datetime(val, errors='coerce')
                return dt
            except Exception:
                return None

        # --- Extract timestamps ---
        create_val = _get_value(order_row, create_keys)
        update_val = _get_value(order_row, update_keys) or create_val

        create_dt = _to_dt(create_val)
        update_dt = _to_dt(update_val) or create_dt

        if create_dt is None or pd.isna(create_dt):
            return False

        # --- Normalize timezone ---
        now = datetime.now(kolkata_tz)
        if create_dt.tzinfo is None:
            create_dt = kolkata_tz.localize(create_dt)
        if update_dt and update_dt.tzinfo is None:
            update_dt = kolkata_tz.localize(update_dt)

        # --- Compute elapsed seconds ---
        age_create = (now - create_dt).total_seconds()
        age_update = (now - update_dt).total_seconds() if update_dt is not None else age_create

        # --- Decision ---
        is_stale = (age_create > cutoff_seconds) and (age_update > 60)
        if is_stale:
            logging.info("⏳ Stale order detected (create_age=%.1fs, update_age=%.1fs)", age_create, age_update)
        return is_stale

    except Exception as e:
        logging.exception("Error in _is_order_stale(): %s", e)
        return False


def _classify_state_by_qty(net_qty, rem_entry, rem_sl, rem_tg, order_present):
    """
    Classify numeric state based on position/order quantities.
    Returns one of:
      - Ready_for_entry
      - Entering
      - Partial_Entry
      - Open
      - SL_Triggered / TG_Triggered
      - Orphan_SL_TG / True_Orphan / Unknown
    """
    def safe_f(v):
        try:
            if v in (None, "", "NaN", "nan"): 
                return 0.0
            return float(pd.to_numeric(v, errors='coerce') or 0.0)
        except Exception:
            return 0.0

    net, re, rs, rt = map(safe_f, [net_qty, rem_entry, rem_sl, rem_tg])

    # All zeros → ready
    if net == 0 and re == 0 and rs == 0 and rt == 0:
        return "Ready_for_entry"

    # Net == 0 but entry pending
    if net == 0 and re > 0:
        return "Entering"

    # Net > 0
    if net > 0:
        if re > 0:
            return "Partial_Entry"
        if re == 0 and (rs > 0 or rt > 0):
            if rs > 0 and rt > 0:
                return "Open"
            if rt > 0:
                return "TG_Triggered"
            if rs > 0:
                return "SL_Triggered"
        if re == 0 and rs == 0 and rt == 0:
            return "Open" if order_present else "True_Orphan"

    # Orphan SL/TG
    if net == 0 and (rs > 0 or rt > 0):
        return "Orphan_SL_TG"

    return "Unknown"

# -----------------------------
# Main reconcile function
# -----------------------------
def reconcile_orders_and_positions(mode='startup', minutes_pending_cutoff=2.5):
    """
    Reconcile positions and super orders using explicit numeric logic.
    Modes: startup, mid, end
    Cancels stale entries and orphan SL/TG automatically.
    """
    global position_status, tradable_df
    tag = mode.upper()
    logging.info(f"🔹 Starting {tag} reconciliation cycle")

    with POSITION_LOCK:
        logging.info(f"🔒 POSITION_LOCK acquired for {tag} reconciliation.")

        # Candle timing
        try:
            candle_interval_sec = interval * 60
        except Exception:
            candle_interval_sec = 300

        now = datetime.now(kolkata_tz)
        seconds_since_candle = (now.minute * 60 + now.second) % candle_interval_sec
        remaining_to_next = candle_interval_sec - seconds_since_candle
        remaining_to_mid = (candle_interval_sec / 2) - seconds_since_candle
        next_candle_time = now + timedelta(seconds=remaining_to_next)
        mid_candle_time = (now + timedelta(seconds=remaining_to_mid)
                           if remaining_to_mid > 0
                           else next_candle_time - timedelta(seconds=candle_interval_sec / 2))

        logging.info(
            "🕒 Candle timing — mid: %s | next: %s | Polling stops 10s before either.",
            mid_candle_time.strftime("%H:%M:%S"),
            next_candle_time.strftime("%H:%M:%S"),
        )

        # Fetch data
        try:
            positions_df = get_positions()
            if positions_df is None or positions_df.empty:
                positions_df = pd.DataFrame()
            pos_success = True
        except Exception as e:
            logging.exception("get_positions() failed: %s", e)
            positions_df = pd.DataFrame()
            pos_success = False

        try:
            orders_df = get_super_order_list()
            if orders_df is None or orders_df.empty:
                orders_df = pd.DataFrame()
            ord_success = True
        except Exception as e:
            logging.exception("get_super_order_list() failed: %s", e)
            orders_df = pd.DataFrame()
            ord_success = False

        if not pos_success or not ord_success:
            logging.warning("⚠️ API failure — cannot reconcile.")
            for leg_type in ["CE", "PE"]:
                position_status[leg_type] = _init_position_state()
                position_status[leg_type].update({
                    "position": "No data available",
                    "note": "API failure — cannot reconcile",
                    "last_updated": datetime.now(kolkata_tz)
                })
            return position_status

        # Empty => Ready
        if positions_df.empty and orders_df.empty:
            for leg_type in ["CE", "PE"]:
                position_status[leg_type] = _init_position_state()
                position_status[leg_type].update({
                    "position": "Ready for entry",
                    "note": "No positions/orders — fresh session",
                    "last_updated": datetime.now(kolkata_tz)
                })
            return position_status

        # tradable_df validation
        if tradable_df is None or tradable_df.empty:
            logging.error("❌ tradable_df missing -> aborting reconciliation.")
            for leg_type in ["CE", "PE"]:
                position_status[leg_type] = _init_position_state()
                position_status[leg_type].update({
                    "position": "Mapping failed — no reconciliation",
                    "note": "tradable_df missing",
                    "last_updated": datetime.now(kolkata_tz)
                })
            return position_status

        valid_ids = tradable_df['SECURITY_ID'].astype(str).unique().tolist()
        try:
            ord_sec_col = _safe_col_choice(orders_df, ["securityId", "SECURITY_ID", "security_id"])
            pos_sec_col = _safe_col_choice(positions_df, ["securityId", "SECURITY_ID", "security_id"])
            if ord_sec_col:
                orders_df = orders_df[orders_df[ord_sec_col].astype(str).isin(valid_ids)]
            if pos_sec_col:
                positions_df = positions_df[positions_df[pos_sec_col].astype(str).isin(valid_ids)]
        except Exception:
            logging.exception("Error filtering to tradable IDs.")

        # Reconcile CE/PE
        for leg_type in ["CE", "PE"]:
            try:
                state = _init_position_state()
                pos_rows = _filter_leg_positions(positions_df, tradable_df, leg_type)
                ord_rows = _filter_leg_orders(orders_df, tradable_df, leg_type)
                logging.info("Processing %s | pos=%d | ord=%d", leg_type, len(pos_rows), len(ord_rows))

                net_qty = float(pos_rows["netQty"].sum()) if "netQty" in pos_rows else 0.0
                rem_entry = float(ord_rows["remainingQuantity"].sum()) if "remainingQuantity" in ord_rows else 0.0
                rem_sl = float(ord_rows["STOP_LOSS_LEG_remainingQuantity"].sum()) if "STOP_LOSS_LEG_remainingQuantity" in ord_rows else 0.0
                rem_tg = float(ord_rows["TARGET_LEG_remainingQuantity"].sum()) if "TARGET_LEG_remainingQuantity" in ord_rows else 0.0

                order_id = _safe_str_from_df(ord_rows, ['orderId', 'ORDER_ID'])
                order_present = bool(order_id)
                classification = _classify_state_by_qty(net_qty, rem_entry, rem_sl, rem_tg, order_present)
                secid = _safe_str_from_df(pos_rows, ['securityId', 'SECURITY_ID']) or _safe_str_from_df(ord_rows, ['securityId', 'SECURITY_ID'])
                logging.info("%s classified as %s (net=%.1f, entry=%.1f, SL=%.1f, TG=%.1f)", leg_type, classification, net_qty, rem_entry, rem_sl, rem_tg)

                # Cancel stale entries in mid/end
                if mode in ('mid', 'end') and classification in ("Entering", "Partial_Entry"):
                    for _, orow in ord_rows.iterrows():
                        try:
                            if _is_order_stale(orow, minutes_pending_cutoff * 60):
                                stale_id = orow.get("orderId")
                                if stale_id:
                                    logging.warning(f"⚠️ {leg_type}: stale entry {stale_id} -> cancelling ENTRY_LEG")
                                    cancel_super_order_leg(stale_id, "ENTRY_LEG")
                        except Exception:
                            continue

                # ----------------------------------------------------------
                # Classification → State Mapping (Hybrid Full Version)
                # ----------------------------------------------------------
                if classification == "Ready_for_entry":
                    state.update({
                        "position": "Ready for entry",
                        "securityId": None,
                        "orderId": None,
                        "orderStatus": None,
                        "quantity": 0.0,
                        "remainingQuantity": 0.0,
                        "STOP_LOSS_LEG_remainingQuantity": 0.0,
                        "TARGET_LEG_remainingQuantity": 0.0,
                        "STOP_LOSS_LEG_status": None,
                        "TARGET_LEG_status": None,
                        "exit_logic_active": False,
                        "entry_timestamp": None,
                        "entry_underlying_price": None,
                        "note": "No net qty / no legs remaining — Ready for entry"
                    })

                elif classification == "Entering":
                    prev_state = position_status.get(leg_type, {})

                    state.update({
                        "position": "Entering",
                        "securityId": secid,
                        "orderId": order_id,
                        "orderStatus": _safe_str_from_df(ord_rows, ['orderStatus', 'ORDER_STATUS']),
                        "quantity": net_qty,
                        "remainingQuantity": rem_entry,
                        "STOP_LOSS_LEG_remainingQuantity": rem_sl,
                        "TARGET_LEG_remainingQuantity": rem_tg,
                        "STOP_LOSS_LEG_status": _safe_str_from_df(ord_rows, ['STOP_LOSS_LEG_orderStatus', 'STOP_LOSS_LEG_status']),
                        "TARGET_LEG_status": _safe_str_from_df(ord_rows, ['TARGET_LEG_orderStatus', 'TARGET_LEG_status']),
                        "note": "Entry order placed — awaiting fill"
                    })
                    # Preserve existing Phase-2 fields
                    state["exit_logic_active"] = prev_state.get("exit_logic_active", False)
                    state["entry_timestamp"] = prev_state.get("entry_timestamp", None)
                    state["entry_underlying_price"] = prev_state.get("entry_underlying_price", None)



                elif classification == "Partial_Entry":
                    prev_state = position_status.get(leg_type, {})

                    state.update({
                        "position": "Partial_Entry",
                        "securityId": secid,
                        "orderId": order_id,
                        "orderStatus": _safe_str_from_df(ord_rows, ['orderStatus', 'ORDER_STATUS']),
                        "quantity": net_qty,
                        "remainingQuantity": rem_entry,
                        "STOP_LOSS_LEG_remainingQuantity": rem_sl,
                        "TARGET_LEG_remainingQuantity": rem_tg,
                        "STOP_LOSS_LEG_status": _safe_str_from_df(ord_rows, ['STOP_LOSS_LEG_orderStatus', 'STOP_LOSS_LEG_status']),
                        "TARGET_LEG_status": _safe_str_from_df(ord_rows, ['TARGET_LEG_orderStatus', 'TARGET_LEG_status']),
                        "note": "Partial entry — some qty pending"
                    })
                    # Preserve existing Phase-2 fields
                    state["exit_logic_active"] = prev_state.get("exit_logic_active", False)
                    state["entry_timestamp"] = prev_state.get("entry_timestamp", None)
                    state["entry_underlying_price"] = prev_state.get("entry_underlying_price", None)


                elif classification == "Open":
                    # Preserve activation & entry info
                    prev_state = position_status.get(leg_type, {})
                    
                    state.update({
                        "position": "Open",
                        "securityId": secid,
                        "orderId": order_id,
                        "orderStatus": _safe_str_from_df(ord_rows, ['orderStatus', 'ORDER_STATUS']),
                        "quantity": net_qty,
                        "remainingQuantity": rem_entry,
                        "STOP_LOSS_LEG_remainingQuantity": rem_sl,
                        "TARGET_LEG_remainingQuantity": rem_tg,
                        "STOP_LOSS_LEG_status": _safe_str_from_df(ord_rows, ['STOP_LOSS_LEG_orderStatus', 'STOP_LOSS_LEG_status']),
                        "TARGET_LEG_status": _safe_str_from_df(ord_rows, ['TARGET_LEG_orderStatus', 'TARGET_LEG_status']),
                        "note": "Open position with SL/TG active"
                    })
                    # Preserve existing Phase-2 fields
                    state["exit_logic_active"] = prev_state.get("exit_logic_active", False)
                    state["entry_timestamp"] = prev_state.get("entry_timestamp", None)
                    state["entry_underlying_price"] = prev_state.get("entry_underlying_price", None)

                    # ---------------------------------------------------------
                    # Phase-2 Simplified Restart Logic: always use SSMA exits
                    # ---------------------------------------------------------
                    if mode == 'startup':
                        state["exit_logic_active"] = True
                        state["entry_timestamp"] = None
                        state["entry_underlying_price"] = None
                        state["note"] += " | Restart detected → SSMA exit regime enabled"

                elif classification in ("SL_Triggered", "TG_Triggered"):
                    prev_state = position_status.get(leg_type, {})

                    state.update({
                        "position": "Triggered",
                        "securityId": secid,
                        "orderId": order_id,
                        "orderStatus": _safe_str_from_df(ord_rows, ['orderStatus', 'ORDER_STATUS']),
                        "quantity": net_qty,
                        "remainingQuantity": rem_entry,
                        "STOP_LOSS_LEG_remainingQuantity": rem_sl,
                        "TARGET_LEG_remainingQuantity": rem_tg,
                        "STOP_LOSS_LEG_status": _safe_str_from_df(ord_rows, ['STOP_LOSS_LEG_orderStatus', 'STOP_LOSS_LEG_status']),
                        "TARGET_LEG_status": _safe_str_from_df(ord_rows, ['TARGET_LEG_orderStatus', 'TARGET_LEG_status']),
                        "note": f"{classification} detected — awaiting broker confirmation"
                    })
                    # Preserve existing Phase-2 fields
                    state["exit_logic_active"] = prev_state.get("exit_logic_active", False)
                    state["entry_timestamp"] = prev_state.get("entry_timestamp", None)
                    state["entry_underlying_price"] = prev_state.get("entry_underlying_price", None)

                elif classification == "Orphan_SL_TG":

                    # Always attempt to cancel all SL/TG legs
                    orphan_cancelled_all = True
                    for _, orow in ord_rows.iterrows():
                        try:
                            # parent super-order id
                            oid = orow.get("orderId") or _safe_str_from_df(
                                pd.DataFrame([orow]), ["orderId", "ORDER_ID"]
                            )
                            if oid:
                                ok_sl, _ = cancel_super_order_leg(oid, "STOP_LOSS_LEG")
                                ok_tg, _ = cancel_super_order_leg(oid, "TARGET_LEG")
                                if not (ok_sl or ok_tg):
                                    orphan_cancelled_all = False
                        except Exception:
                            orphan_cancelled_all = False

                    # Final hard reset — ALWAYS reset Phase-2 fields
                    state.update({
                        "position": "Ready for entry",
                        "securityId": None,
                        "orderId": None,
                        "orderStatus": None,
                        "quantity": 0.0,
                        "remainingQuantity": 0.0,
                        "STOP_LOSS_LEG_remainingQuantity": 0.0,
                        "TARGET_LEG_remainingQuantity": 0.0,
                        "STOP_LOSS_LEG_status": None,
                        "TARGET_LEG_status": None,

                        # Phase-2 reset
                        "exit_logic_active": False,
                        "entry_timestamp": None,
                        "entry_underlying_price": None,

                        "note": (
                            "Cancelled orphan SL/TG orders — reset Ready"
                            if orphan_cancelled_all
                            else "Orphan SL/TG present — cancel attempted or manual review"
                        ),
                    })


                elif classification == "True_Orphan":
                    state.update({
                        "position": "Ready for entry",
                        "securityId": secid,
                        "quantity": net_qty,
                        "orderStatus": None,
                        "exit_logic_active": False,
                        "entry_timestamp": None,
                        "entry_underlying_price": None,
                        "note": "True orphan (pos exists but no order) — marked Ready for entry"
                    })

                else:
                    state.update({
                        "position": "Unknown",
                        "securityId": secid,
                        "orderId": order_id,
                        "quantity": net_qty,
                        "remainingQuantity": rem_entry,
                        "STOP_LOSS_LEG_remainingQuantity": rem_sl,
                        "TARGET_LEG_remainingQuantity": rem_tg,
                        "STOP_LOSS_LEG_status": _safe_str_from_df(ord_rows, ['STOP_LOSS_LEG_orderStatus', 'STOP_LOSS_LEG_status']),
                        "TARGET_LEG_status": _safe_str_from_df(ord_rows, ['TARGET_LEG_orderStatus', 'TARGET_LEG_status']),
                        "exit_logic_active": False,
                        "entry_timestamp": None,
                        "entry_underlying_price": None,
                        "note": f"Unhandled numeric classification: {classification}"
                    })

                # Timestamp
                state["last_updated"] = datetime.now(kolkata_tz)
                position_status[leg_type] = state


            except Exception as e:
                logging.exception("Exception in %s leg: %s", leg_type, e)
                continue

        logging.info(f"🔓 POSITION_LOCK released after {tag} reconciliation.")
        logging.info(f"{tag} reconciliation completed → {position_status}")
        logging.debug(json.dumps(position_status, indent=2, default=str))
        polog.info(f"🔓 POSITION_LOCK released after {tag} reconciliation.")
        polog.info(f"{tag} reconciliation completed → {position_status}")
        polog.debug(json.dumps(position_status, indent=2, default=str))

    return position_status

#========================================#
### 7.0    Live Data Feed - Instruments   
#========================================#
# 7.1       Create a DhanFeed instance
feed = DhanFeed(client_id, api_token, instrument, version)

# Define a callback function to handle incoming ticks
async def on_ticks(tick):
    """
    Handles every live tick update from DhanFeed.
    Safely updates LTP_subscribed_instruments under lock,
    and notifies the monitoring task when the tracked instrument's LTP changes (or even stays same, but new tick).
    """

    global LTP_subscribed_instruments

    try:
        #----------------------------------------------------------#
        # 1️⃣ Accept only real ticker data
        #----------------------------------------------------------#
        tick_type = tick.get("type")
        if tick_type != "Ticker Data":
            logging.debug("Ignoring non-ticker tick type: %s", tick_type)
            return

        #----------------------------------------------------------#
        # 2️⃣ Extract fields
        #----------------------------------------------------------#
        security_id = int(tick.get("security_id", 0))
        ltp_value   = tick.get("LTP")
        tick_ts_raw = tick.get("timestamp")

        if ltp_value is None:
            logging.warning("Missing LTP value for security_id %s", security_id)
            return

        #----------------------------------------------------------#
        # 3️⃣ Ensure entry exists — protected
        #----------------------------------------------------------#
        with POSITION_LOCK:
            entry_missing = (security_id not in LTP_subscribed_instruments)
            if entry_missing:
                LTP_subscribed_instruments[security_id] = {
                    'LTP': None,
                    'timestamp': None
                }

        display_name = security_id_to_name.get(security_id, 'Unknown')

        if entry_missing:
            logging.debug(
                "Added new instrument %s (%s) to LTP store",
                security_id, display_name
            )

        #----------------------------------------------------------#
        # 4️⃣ Convert tick timestamp (UTC epoch → IST)
        #----------------------------------------------------------#
        ts_str   = None
        fixed_ts = None
        log_ts_str = "N/A"

        if tick_ts_raw:
            try:
                raw = int(tick_ts_raw)

                # Detect milliseconds
                if raw > 10**12:
                    raw = raw / 1000.0

                # Reject garbage timestamps
                if raw <= 0:
                    raise ValueError("Invalid epoch timestamp")

                fixed_ts = raw

                # Convert UTC → IST
                ist_dt = datetime.fromtimestamp(fixed_ts - 19800)
                ts_str = ist_dt.strftime("%Y-%m-%d %H:%M:%S")
                log_ts_str = ts_str

            except Exception:
                logging.debug(
                    f"⚠️ Invalid timestamp received for {security_id}: {tick_ts_raw}"
                )

        #----------------------------------------------------------#
        # 5️⃣ Notify tracked instrument BEFORE updating dict
        #----------------------------------------------------------#
        if security_id == int(security_id_tracked):

            with POSITION_LOCK:
                prev_ltp = LTP_subscribed_instruments[security_id].get('LTP')
                prev_ts  = LTP_subscribed_instruments[security_id].get('timestamp')

            snapshot = {
                'security_id': security_id,
                'prev_LTP': prev_ltp,
                'LTP': float(ltp_value),
                'prev_timestamp': prev_ts,
                'timestamp': fixed_ts,
            }

            async with ltp_update_condition:
                ltp_update_condition.snapshot = snapshot
                ltp_update_condition.notify_all()

            logging.debug(
                "📡 [Tracked] tick → SEC_ID=%s (%s) | LTP=%.2f | prev_LTP=%.2f | ts=%s",
                security_id, display_name, float(ltp_value),
                (prev_ltp or 0.0), log_ts_str
            )

        #----------------------------------------------------------#
        # 6️⃣ SAFE UPDATE: LTP & timestamp together under lock
        #----------------------------------------------------------#
        with POSITION_LOCK:
            LTP_subscribed_instruments[security_id]["LTP"] = float(ltp_value)
            LTP_subscribed_instruments[security_id]["timestamp"] = fixed_ts

        #----------------------------------------------------------#
        # 7️⃣ Debug log for non-tracked instruments
        #----------------------------------------------------------#
        if security_id != int(security_id_tracked):
            logging.debug(
                "Updated LTP for %s (%s): %.2f | ts=%s | type=%s",
                security_id, display_name, float(ltp_value),
                log_ts_str, tick_type
            )

    #--------------------------------------------------------------#
    # 8️⃣ Clean exception handling
    #--------------------------------------------------------------#
    except KeyError as e:
        logging.error("⚠️ KeyError in on_ticks: missing key %s", e)

    except ValueError as e:
        logging.error("⚠️ ValueError in on_ticks (bad LTP or field): %s", e)

    except TypeError as e:
        logging.error("⚠️ TypeError in on_ticks: %s", e)

    except Exception as e:
        logging.exception("🔥 Unexpected exception in on_ticks: %s", e)

# Set the on_ticks callback function
feed.on_ticks = on_ticks

#========================================#
### 7.2    Live Data Feed - Connection Start    
#========================================#
async def connect_to_dhan():
    """
    Connect to DhanFeed, handle reconnects, and continuously receive ticks.
    Decodes binary feed packets to include both LTP and Last Trade Time (timestamp).
    Initially subscribes only to the tracked instrument.
    Option subscriptions happen later after the first candle forms.
    """
    import struct
    backoff = 1

    while True:
        try:
            await feed.connect()
            logging.info("Connected to DhanFeed.")
            backoff = 1

            # ✅ Re-subscribe only if there are extra instruments (not just the tracked one)
            if not subscribed_instruments.empty:
                ids = subscribed_instruments['SECURITY_ID'].astype(int).tolist()
                resub_ids = [i for i in ids if i != int(security_id_tracked)]

                if resub_ids:
                    logging.info(
                        "Reconnection detected. Subscribed total=%d, re-subscribing options=%d",
                        len(ids), len(resub_ids)
                    )
                    await subscribe_additional_instruments_v2(feed, resub_ids)
                    logging.info("Re-subscribed %d instruments: %s", len(resub_ids), resub_ids)
            else:
                logging.info("Waiting for first 5-minute candle before option subscription.")

            # 🟢 Continuous tick processing loop
            while True:
                raw = await feed.ws.recv()

                # ========================================================== #
                # 🔍 Decode Dhan Binary Packet Inline (FeedCode 2 = Ticker)
                # ========================================================== #
                tick = None
                try:
                    if isinstance(raw, (bytes, bytearray)) and len(raw) >= 16:
                        # Unpack header (8 bytes): <BHB I = FeedCode, MsgLen, Segment, SecurityID
                        feed_code, msg_len, segment, security_id = struct.unpack('<BHB I', raw[:8])

                        # Handle only Ticker packets (FeedCode 2)
                        if feed_code == 2:
                            # Payload: LTP (float32) + LTT (int32 epoch)
                            ltp, epoch_time = struct.unpack('<fi', raw[8:16])
                            tick = {
                                "type": "Ticker Data",
                                "security_id": int(security_id),
                                "LTP": round(float(ltp), 2),
                                "timestamp": int(epoch_time),
                            }
                        else:
                            # fallback to existing handler for non-ticker packets
                            tick = feed.process_data(raw)
                    else:
                        # fallback to SDK decode if it's JSON/text
                        tick = feed.process_data(raw)

                except Exception as e:
                    logging.debug("⚠️ Binary decode fallback to SDK: %s", e)
                    tick = feed.process_data(raw)

                # ========================================================== #
                # ✅ Forward valid ticks to handler
                # ========================================================== #
                if tick:
                    await on_ticks(tick)

        except Exception as e:
            logging.error("Feed error: %s. Reconnecting in %s s", e, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)

# asyncio.run(main())

#=========================================================================#
### 7.0    Finding  Instruments to be Added to Live Feed (ITM Version)
#=========================================================================#
def find_required_strikes(close_value):
    """
    Build a list of required option strikes around ATM for the current underlying.
    This version selects ATM and five ITM strikes for both CE and PE options.
    Returns a DataFrame of new strikes to subscribe.
    """
    global subscribed_instruments, LTP_subscribed_instruments

    #----------------------------------------#
    # 7.1  Validate input value
    #----------------------------------------#
    if close_value is None:
        logging.warning("close_value is None – cannot select strikes yet.")
        return pd.DataFrame()

    #----------------------------------------#
    # 7.2  Load tradable instruments file
    #----------------------------------------#
    file_name = f'Tradable_Instruments_List_{current_date}.csv'
    file_path = os.path.join(DATA_DIR, file_name)
    df = pd.read_csv(file_path)

    #----------------------------------------#
    # 7.3  Keep only CE/PE option rows
    #----------------------------------------#
    df = df[df['OPTION_TYPE'].isin(['CE', 'PE'])].copy()

    # Ensure STRIKE_PRICE is numeric and drop invalid rows
    df['STRIKE_PRICE'] = pd.to_numeric(df['STRIKE_PRICE'], errors='coerce')
    df = df.dropna(subset=['STRIKE_PRICE'])
    if df.empty:
        raise ValueError("No option rows found in tradable instruments list.")

    #----------------------------------------#
    # 7.4  Identify the ATM strike
    #----------------------------------------#
    atm_index = (df['STRIKE_PRICE'] - close_value).abs().argsort().iloc[0]
    atm_strike = df['STRIKE_PRICE'].iloc[atm_index]

    #----------------------------------------#
    # 7.5  Select ITM strikes (5 each)
    #----------------------------------------#
    # PE (Put Options): ITM means STRIKE > ATM
    # Sort ascending to pick 5 nearest ITM PEs above ATM
    pe_strikes = df[
        (df['STRIKE_PRICE'] > atm_strike) & (df['OPTION_TYPE'] == 'PE')
    ].sort_values(by='STRIKE_PRICE').head(5)

    # CE (Call Options): ITM means STRIKE < ATM
    # Sort descending to pick 5 nearest ITM CEs below ATM
    ce_strikes = df[
        (df['STRIKE_PRICE'] < atm_strike) & (df['OPTION_TYPE'] == 'CE')
    ].sort_values(by='STRIKE_PRICE', ascending=False).head(5)

    #----------------------------------------#
    # 7.6  Include ATM strikes (both CE and PE)
    #----------------------------------------#
    atm_strikes = df[
        (df['STRIKE_PRICE'] == atm_strike) &
        (df['OPTION_TYPE'].isin(['CE', 'PE']))
    ]

    # Combine all: ITM PE + ATM CE/PE + ITM CE
    required_strikes = pd.concat([pe_strikes, atm_strikes, ce_strikes])

    #----------------------------------------#
    # 7.7  Remove already subscribed instruments
    #----------------------------------------#
    if not subscribed_instruments.empty:
        required_strikes = required_strikes[
            ~required_strikes['SECURITY_ID'].isin(subscribed_instruments['SECURITY_ID'])
        ]

    #----------------------------------------#
    # 7.8  Ensure SECURITY_ID is integer for consistency
    #----------------------------------------#
    if required_strikes.empty:
        # Even if nothing new, we still log and keep existing structures
        logging.info("ℹ️ No new strikes required — subscription unchanged.")
        logging.info("Final subscribed_instruments:\n%s", subscribed_instruments)
        return required_strikes

    required_strikes['SECURITY_ID'] = required_strikes['SECURITY_ID'].astype(int)

    #----------------------------------------#
    # 7.9  Track new IDs for logging
    #----------------------------------------#
    new_ids = required_strikes['SECURITY_ID'].tolist()
    if new_ids:
        logging.info("🆕 Newly required strikes: %s", new_ids)
    else:
        logging.info("ℹ️ No new strikes required — subscription unchanged.")

    #----------------------------------------#
    # 7.10–7.12  SAFE UPDATE OF GLOBALS UNDER LOCK
    #----------------------------------------#
    with POSITION_LOCK:
        # 7.10  Append new strikes to subscribed_instruments DataFrame
        new_subscribed = pd.concat([subscribed_instruments, required_strikes]) \
                            .drop_duplicates(subset=['SECURITY_ID'], keep='first') \
                            .reset_index(drop=True)

        # 7.11  Ensure the underlying instrument is always included
        subs_ids_int = new_subscribed['SECURITY_ID'].astype(int).values
        if int(security_id_tracked) not in subs_ids_int:
            new_subscribed = pd.concat([
                new_subscribed,
                pd.DataFrame([{
                    'SECURITY_ID': int(security_id_tracked),
                    'DISPLAY_NAME': '',
                    'STRIKE_PRICE': 0,
                    'OPTION_TYPE': '',
                    'UNDERLYING_SECURITY_ID': ''
                }])
            ], ignore_index=True)

        # 7.12  Rebuild LTP_subscribed_instruments dict to stay in sync,
        #        but KEEP existing LTP/timestamp values where available.
        wanted_ids = new_subscribed['SECURITY_ID'].astype(int).tolist()
        new_ltp_map = {}

        for sid in wanted_ids:
            sid = int(sid)
            prev = LTP_subscribed_instruments.get(sid, {})
            new_ltp_map[sid] = {
                'LTP': prev.get('LTP', None),
                'timestamp': prev.get('timestamp', None)
            }

        # Also ensure underlying is present in the LTP dict
        if int(security_id_tracked) not in new_ltp_map:
            new_ltp_map[int(security_id_tracked)] = {'LTP': None, 'timestamp': None}

        # Atomically replace / sync both structures
        subscribed_instruments = new_subscribed
        LTP_subscribed_instruments.clear()
        LTP_subscribed_instruments.update(new_ltp_map)

        logging.info("Final subscribed_instruments:\n%s", subscribed_instruments)

    return required_strikes

#========================================#
### 7.1.1    Subscribe Additional Instruments to Live Feed  
#========================================#
async def wait_ws_ready(feed, timeout=15):
    """Wait until feed.ws exists and is usable."""
    start = datetime.now()
    while True:
        ws = getattr(feed, "ws", None)
        # check that ws exists and (if it has .closed) it's not closed
        if ws:
            # if .closed exists, require it to be False
            if hasattr(ws, "closed"):
                if not ws.closed:
                    break  # ready
            else:
                # no .closed property? assume ready if ws is non-None
                break
        await asyncio.sleep(0.2)
        if (datetime.now() - start).total_seconds() > timeout:
            raise TimeoutError("WebSocket not ready in time.")

#========================================================================#
### Subscribe Additional Instruments - V2
#========================================================================#
async def subscribe_additional_instruments_v2(feed, security_ids):
    
    """
    Subscribe to additional instruments safely:
    - Ensures LTP_subscribed_instruments has entries BEFORE ticks arrive
    - Protects shared dict using POSITION_LOCK
    - Builds and sends WebSocket payload cleanly
    """

    if not security_ids:
        logging.info("subscribe_additional_instruments_v2: No new security_ids to subscribe.")
        return

    # Ensure WebSocket is ready
    await wait_ws_ready(feed)

    # --------------------------------------------------------
    # 1️⃣ PRE-SAFE: Ensure dict entries exist BEFORE feed sends ticks
    # --------------------------------------------------------
    added_list = []

    with POSITION_LOCK:
        for s in security_ids:
            sid = int(s)
            if sid not in LTP_subscribed_instruments:
                LTP_subscribed_instruments[sid] = {'LTP': None, 'timestamp': None}
                added_list.append(sid)

    if added_list:
        logging.info("🆕 Prepared %d new LTP dict entries: %s", len(added_list), added_list)
    else:
        logging.info("ℹ️ All security_ids already existed in LTP dict. No new entries added.")

    # --------------------------------------------------------
    # 2️⃣ Build instrument subscription payload
    # --------------------------------------------------------
    instrument_list = [
        {"ExchangeSegment": exchange_segment_tradable, "SecurityId": str(int(s))}
        for s in security_ids
    ]

    payload = {
        "RequestCode": 15,
        "InstrumentCount": len(instrument_list),
        "InstrumentList": instrument_list
    }

    logging.info(
        "📡 Subscribing to %d instruments: %s",
        len(instrument_list),
        [i['SecurityId'] for i in instrument_list]
    )

    # --------------------------------------------------------
    # 3️⃣ Send to WebSocket
    # --------------------------------------------------------
    try:
        await feed.ws.send(json.dumps(payload))
        logging.info("✅ Subscription request sent successfully.")
    except Exception as e:
        logging.exception("🔥 Failed to send subscription request: %s", e)

#==================================================#
### 7.1.1    Trade Management - Place Super Order for PE CE Buys
#==================================================#
def place_super_order_long(security_id, leg_type=None):
    global position_status, quantity, exchange_segment_tradable
    """
    Places a Super Order for a long entry (CE_LONG or PE_LONG).
    Handles Dhan API call and updates position_status for the relevant leg.
    """
    global position_status, quantity

    url = "https://api.dhan.co/v2/super/orders"
    headers = {
        "Content-Type": "application/json",
        "access-token": api_token
    }

    # 🟢 Fetch latest LTP from subscribed instruments
    price = LTP_subscribed_instruments.get(security_id, {}).get('LTP')
    if price is None:
        logging.warning("⚠️ No LTP available for %s — aborting Super Order placement.", security_id)
        return {"order_id": None, "status": "LTP unavailable"}

    # 🧮 Calculate target and stop-loss prices
    #  ## Target: +120 points
    target_price = price + 120
    #  ## Stop-loss: 20% max loss, but never below 5
    max_loss_pct = 0.20                     # 20% max loss
    raw_sl = price * (1 - max_loss_pct)     # price × 0.8
    stoploss_price = max(raw_sl, 5)         # Cannot go below 5

    # 🧾 Prepare payload for Dhan API
    payload = {
        "dhanClientId": client_id,
        "transactionType": "BUY",
        "exchangeSegment": exchange_segment_tradable,
        "productType": "INTRADAY",
        "orderType": "LIMIT",
        "securityId": str(security_id),
        "quantity": quantity,
        "price": price,
        "targetPrice": target_price,
        "stopLossPrice": stoploss_price
    }

    order_id = None
    api_status = "FAILED"

    try:
        resp = requests.post(url, headers=headers, data=json.dumps(payload))

        if resp.status_code == 200:
            resp_json = resp.json()
            data = resp_json.get("data", resp_json)

            order_id = data.get("orderId")

            if order_id:
                logging.info(
                    "✅ Super Order placed successfully — SecID=%s | OrderID=%s | TP=%.2f | SL=%.2f",
                    security_id, order_id, target_price, stoploss_price
                )
                api_status = "SUCCESS"
            else:
                logging.warning(
                    "⚠️ Order placed but OrderID missing in response. Response: %s",
                    resp.text
                )
        else:
            logging.error("❌ Super Order API error (%s): %s", resp.status_code, resp.text)

    except Exception as e:
        logging.exception("❌ Exception during Super Order placement for %s: %s", security_id, e)

    if leg_type in ["CE", "PE"]:

        underlying_entry_price = LTP_subscribed_instruments.get(
            int(security_id_tracked), {}
        ).get("LTP")

        with POSITION_LOCK:
            position_status[leg_type].update({
                "position": "Entering",
                "securityId": security_id,
                "orderId": order_id,
                "quantity": quantity,
                "remainingQuantity": quantity,
                "orderStatus": api_status,
                "STOP_LOSS_LEG_remainingQuantity": None,
                "TARGET_LEG_remainingQuantity": None,
                "STOP_LOSS_LEG_status": None,
                "TARGET_LEG_status": None,

                # --- Phase 2 additions ---
                "exit_logic_active": False,
                "entry_timestamp": datetime.now(kolkata_tz),
                "entry_underlying_price": underlying_entry_price,
                # --------------------------

                "last_updated": datetime.now(kolkata_tz),
                "note": "Entry order sent successfully to Dhan"
                    if api_status == "SUCCESS"
                    else "Order placement attempt made — pending confirmation",
            })

        logging.info(
            "📊 %s position status updated — SecID=%s | OrderID=%s | Price=%.2f",
            leg_type,
            security_id,
            order_id or "None",
            price or -1
        )

    # ✅ Return result for optional use
    return {"order_id": order_id, "status": api_status, "price": price}

#====================================================================#
### 8.0    Buying Positions  
#====================================================================#

def buy_ce_position():
    ce_strikes = subscribed_instruments[subscribed_instruments['OPTION_TYPE'] == 'CE']

    # ✅ Safeguard: no CE strikes available
    if ce_strikes.empty:
        logging.warning("⚠️ No CE strikes found — skipping CE buy.")
        return

    # Pick the ATM or next ITM CE strike
    atm_ce_strike = ce_strikes[ce_strikes['STRIKE_PRICE'] >= close_value].sort_values(by='STRIKE_PRICE').head(1)

    if atm_ce_strike.empty:
        atm_ce_strike = ce_strikes.sort_values(by='STRIKE_PRICE', ascending=False).head(1)

    security_id = int(atm_ce_strike['SECURITY_ID'].values[0])

    # Start order execution 
    place_super_order_long(security_id, leg_type="CE")


    logging.info("🟢 CE entry request sent for SECURITY_ID=%s", security_id)


def buy_pe_position():
    pe_strikes = subscribed_instruments[subscribed_instruments['OPTION_TYPE'] == 'PE']

    # ✅ Safeguard: no PE strikes available
    if pe_strikes.empty:
        logging.warning("⚠️ No PE strikes found — skipping PE buy.")
        return

    # Pick the ATM or next ITM PE strike
    atm_pe_strike = pe_strikes[pe_strikes['STRIKE_PRICE'] <= close_value].sort_values(by='STRIKE_PRICE', ascending=False).head(1)

    if atm_pe_strike.empty:
        atm_pe_strike = pe_strikes.sort_values(by='STRIKE_PRICE', ascending=True).head(1)

    security_id = int(atm_pe_strike['SECURITY_ID'].values[0])

    # Start order execution
    place_super_order_long(security_id, leg_type="PE")

    logging.info("🔴 PE entry request sent for SECURITY_ID=%s", security_id)

#====================================================================#
### 9.0    Check Entry Conditions and initiate Buying Options  
#====================================================================#
def check_entry_conditions():
    global position_status, last_candle_time

    now = datetime.now(kolkata_tz)

    # ✅ compute the current expected 5-min candle time
    current_candle_time = now.replace(second=0, microsecond=0)
    # snap to last completed 5-min boundary
    minute = (current_candle_time.minute // interval) * interval
    current_candle_time = current_candle_time.replace(minute=minute)

    if last_candle_time is None:
        # print("No candle time yet.")
        logging.warning("No candle time yet — skipping entry check..")
        return

    # 🟢 skip if last_candle_time < current_candle_time
    if last_candle_time < current_candle_time:
        # print(f"Last candle {last_candle_time} not current {current_candle_time}, skipping entry check.")
        logging.info("Last candle %s not current %s, skipping entry check.", last_candle_time, current_candle_time)
        return

    entry_start = now.replace(hour=9, minute=30, second=0)
    entry_end   = now.replace(hour=entryEndH, minute=entryEndM, second=0)
    is_entry_time = entry_start <= now <= entry_end

    # ✅ Guard: ensure CE/PE instruments and LTPs are ready before evaluating entry
    has_option_rows = False
    try:
        if LTP_subscribed_instruments:
            cepe_ids = [
                sid for sid in LTP_subscribed_instruments.keys()
                if sid != int(security_id_tracked)
            ]
            if cepe_ids:
                # Require at least one CE/PE with a live LTP value
                if any(LTP_subscribed_instruments[sid].get('LTP') for sid in cepe_ids):
                    has_option_rows = True
    except Exception as e:
        logging.error("Error checking LTP_subscribed_instruments readiness: %s", e)
        has_option_rows = False

    if not has_option_rows:
        logging.info("⚠️  Option LTPs not yet populated — skipping entry check this cycle.")
        return

    # 🧮 Core entry logic
    with POSITION_LOCK:
        ce = position_status["CE"].copy()
        pe = position_status["PE"].copy()

    if ce["position"] == "Ready for entry" and pe["position"] == "Ready for entry":
    
        if is_entry_time:
            if ssma_Value is None or lsma_Value is None or close_value is None:
                logging.warning("One of SSMA/LSMA/Close is None — cannot evaluate entry.")
                return

            # --- Dynamic entry distance based on underlying ---
            base_pct = 0.00070     # 0.070%
            step_pct = 0.00005     # reduce 0.005% per 5000
            min_pct  = 0.00035     # 0.035% floor

            if close_value < 5000:
                pct = base_pct
            else:
                blocks = int((close_value - 5000) // 5000) + 1
                pct = base_pct - (blocks * step_pct)

            # enforce lower bound
            pct = max(pct, min_pct)

            entry_distance_from_ssma = close_value * pct
            logging.info(f"Dynamic entry pct={pct*100:.3f}% | distance={entry_distance_from_ssma:.2f}")
            # -------------------------------------------------

            if abs(ssma_Value - close_value) <= entry_distance_from_ssma:
                if ssma_Value > (lsma_Value * 1.0001):
                    logging.info("📈 Long Condition — CE Buy")
                    buy_ce_position()
                elif ssma_Value < (lsma_Value * 0.9999):
                    logging.info("📉 Short Condition — PE Buy")
                    buy_pe_position()
                else:
                    logging.warning("Trend unclear — skipping trade.")
            else:
                logging.warning(f"SSMA not within dynamic band ({pct*100:.3f}%) — skipping trade.")
        else:
            logging.info("Not within entry time window — skipping entry.")
    else:
        logging.info("Position already open — skipping new entry.")

# check_entry_conditions()

#====================================================================#
### X.0    Modify STOP LOSS to Exit Position  
#====================================================================#
def exit_position(order_id, leg):
    """
    Modify the STOP_LOSS_LEG to LTP - buffer to trigger a controlled exit.
    If price reverses again in favor, position remains open (desirable).
    """

    global position_status, LTP_subscribed_instruments, api_token, client_id, sl_exit_buffer

    # 1) Get security ID of the option being monitored (CE or PE)
    with POSITION_LOCK:
        security_id = position_status[leg].get("securityId")

    if not security_id:
        logging.error(f"❌ exit_position(): No securityId found for leg={leg}")
        return

    # 2) Fetch latest LTP of the option
    curr_ltp = LTP_subscribed_instruments.get(int(security_id), {}).get("LTP")
    if curr_ltp is None:
        logging.warning(f"⚠️ exit_position(): LTP unavailable for SEC_ID={security_id}, delaying exit.")
        return

    # 3) Compute a new SL price *below* current LTP (required by Dhan)
    new_stop_loss_price = round(float(curr_ltp) - sl_exit_buffer, 2)

    logging.info(
        "🔁 Exit Request → %s | SEC_ID=%s | LTP=%.2f → New SL=%.2f (LTP - %.2f)",
        leg, security_id, curr_ltp, new_stop_loss_price, sl_exit_buffer
    )

    # 4) Prepare Dhan modify request
    url = f"https://api.dhan.co/v2/super/orders/{order_id}"
    headers = {"Content-Type": "application/json", "access-token": api_token}
    payload = {
        "dhanClientId": client_id,
        "orderId": order_id,
        "legName": "STOP_LOSS_LEG",
        "stopLossPrice": float(new_stop_loss_price)
    }

    # 5) Send PUT request
    try:
        response = requests.put(url, json=payload, headers=headers)
        response.raise_for_status()
        logging.info("✅ SL modified successfully — Exit execution active.")
    except Exception as e:
        logging.exception("❌ exit_position(): SL modify failed: %s", e)
        return

    # 6) Mark position as Exiting to prevent duplicate exit triggers
    with POSITION_LOCK:
        position_status[leg]["position"] = "Exiting"
        position_status[leg]["note"] = f"SL moved to {new_stop_loss_price:.2f} — exit attempt in progress"
        position_status[leg]["last_updated"] = datetime.now(kolkata_tz)

    logging.info("🔚 %s marked as Exiting.", leg)


# ===========================================================================#
# 🧭 LIVE POSITION MONITOR — Real-time Exit & Trend Reversal Watch (Task 4)
# ===========================================================================#
async def live_position_monitor():
    """
    Continuously listens for tick updates via ltp_update_condition.
    When a tick for the tracked instrument arrives, this coroutine:
      • Checks if any position (CE/PE) is currently Open.
      • Computes a live SSMA using latest LTP + previous_close_values_map.
      • Detects trend reversals:
            - CE: live_ssma < LSMA → exit_ce_position()
            - PE: live_ssma > LSMA → exit_pe_position()
      • Logs all computations for audit.
      • But ONLY after Phase-2 activation logic (favourable-move or timeout), unless restart has already enabled it.
    """

    global position_status, previous_close_values_map, lsma_Value
    global security_id_tracked, ssma_window, min_period
    global base_req_fav_move, decay_factor, bucket_size, min_buckets
    global timeout_minutes

    logging.info("🧭 Starting live_position_monitor() coroutine...")

    while True:
        try:
            # ------------------------------------------------------ #
            # 1️⃣ Wait for tick update
            # ------------------------------------------------------ #
            async with ltp_update_condition:
                await ltp_update_condition.wait()
                snapshot = getattr(ltp_update_condition, "snapshot", None)

            if not snapshot:
                continue

            # Only process if underlying tick
            if snapshot.get("security_id") != int(security_id_tracked):
                continue

            curr_underlying = snapshot.get("LTP")
            if curr_underlying is None:
                continue

            # ------------------------------------------------------ #
            # 2️⃣ ATOMIC READ of closes + SMA values (prevent race)
            # ------------------------------------------------------ #
            async with SMA_LOCK:
                closes_dict_copy = dict(
                    previous_close_values_map.get(security_id_tracked, {})
                )
                lsma_val_copy = lsma_Value
                ssma_val_copy = ssma_Value

            if lsma_val_copy is None:
                continue

            if not closes_dict_copy:
                logging.debug("No previous closes available for SSMA calc.")
                continue

            # Clean + sort closes for SSMA calc
            closes_dict_copy = {k: v for k, v in closes_dict_copy.items() if v is not None}
            sorted_items = sorted(closes_dict_copy.items(), key=lambda x: x[0])
            closes = [float(v) for _, v in sorted_items]

            # Add current underlying to close window
            closes.append(float(curr_underlying))
            closes = closes[-ssma_window:]

            if len(closes) < min_period:
                continue

            # Compute live SSMA
            series = pd.Series(closes)
            live_ssma = (
                round(series.rolling(ssma_window, min_periods=min_period).mean().iloc[-1], 2)
                if len(closes) >= ssma_window
                else round(series.mean(), 2)
            )

            # ------------------------------------------------------ #
            # 3️⃣ Determine which leg is open
            # ------------------------------------------------------ #
            with POSITION_LOCK:
                ce_snapshot = position_status["CE"].copy()
                pe_snapshot = position_status["PE"].copy()

            if ce_snapshot["position"] == "Open":
                leg = "CE"
                order_id = ce_snapshot["orderId"]
            elif pe_snapshot["position"] == "Open":
                leg = "PE"
                order_id = pe_snapshot["orderId"]
            else:
                leg = None
                order_id = None

            if not leg or not order_id:
                continue

            # ------------------------------------------------------ #
            # 4️⃣ EXIT-ACTIVATION LOGIC
            # ------------------------------------------------------ #
            with POSITION_LOCK:
                ps = position_status[leg].copy()
            
            logging.debug(
                "🔍 Exit state for %s → pos=%s | active=%s | entry_ts=%s | entry_underlying=%s",
                leg, ps["position"], ps["exit_logic_active"],
                ps["entry_timestamp"], ps["entry_underlying_price"]
            )

            # Skip if urgent exit already triggered
            if ps["position"] == "Exiting":
                continue

            exit_active = ps["exit_logic_active"]
            entry_ts = ps["entry_timestamp"]
            entry_underlying = ps["entry_underlying_price"]

            activated = False

            # A) Favorable move activation
            if not exit_active and entry_underlying is not None:
                buckets = max(min_buckets, int(entry_underlying // bucket_size))
                required_pct = base_req_fav_move * (decay_factor ** (buckets - 1))

                if leg == "CE":
                    pct_move = (curr_underlying - entry_underlying) / entry_underlying
                else:  # PE
                    pct_move = (entry_underlying - curr_underlying) / entry_underlying

                if pct_move >= required_pct:
                    activated = True

            # B) Timeout activation
            if not exit_active and not activated and entry_ts is not None:
                elapsed = (datetime.now(kolkata_tz) - entry_ts).total_seconds()
                if elapsed >= timeout_minutes * 60:
                    activated = True

            # C) If activation triggered → update flag
            if activated and not exit_active:
                with POSITION_LOCK:
                    position_status[leg]["exit_logic_active"] = True
                    position_status[leg]["note"] = "Exit logic activated — SSMA monitoring ON"
                logging.info(f"🔔 Exit logic ACTIVATED for {leg}")
                continue

            # D) Not activated → skip trend checks
            if not exit_active:
                logging.debug(f"🛑 Exit logic inactive for {leg} — skipping SSMA exit.")
                continue

            # ------------------------------------------------------ #
            # 5️⃣ TREND REVERSAL EXIT LOGIC (AFTER ACTIVATION)
            # ------------------------------------------------------ #
            # PHASE-3 — Dynamic Hysteresis around LSMA
            
            # Spread between previous LSMA and SSMA (from candle close)
            spread = abs(lsma_val_copy - ssma_val_copy)

            # LSMA bucket size mapping (adaptive by instrument)
            buckets = max(1, int(lsma_val_copy // 5000))

            # Compressed vs normal regime
            if spread < 1:
                # Compressed regime: strong hysteresis (1 point per bucket)
                shift = buckets * 1.0
            else:
                # Normal regime: moderate hysteresis (0.5 point per bucket)
                shift = buckets * 0.5

            # Compute final LSMA bands
            lsma_lower = float(lsma_val_copy) - shift
            lsma_upper = float(lsma_val_copy) + shift

            logging.debug(
                "🔧 Hysteresis → spread=%.2f | buckets=%d | shift=%.2f | lower=%.2f | upper=%.2f",
                spread, buckets, shift, lsma_lower, lsma_upper
            )

            # ------------------------------------------------------ #
            # 6️⃣ FINAL EXIT CONDITIONS USING HYSTERESIS
            # ------------------------------------------------------ #

            if leg == "CE" and live_ssma < lsma_lower:
                logging.info(
                    "⚠️ [CE EXIT] live_SSMA=%.2f < LSMA_LOWER=%.2f — Trend reversal (HYSTERESIS OK)",
                    live_ssma, lsma_lower
                )
                exit_position(order_id, leg)

            elif leg == "PE" and live_ssma > lsma_upper:
                logging.info(
                    "⚠️ [PE EXIT] live_SSMA=%.2f > LSMA_UPPER=%.2f — Trend reversal (HYSTERESIS OK)",
                    live_ssma, lsma_upper
                )
                exit_position(order_id, leg)

            else:
                logging.debug(
                    "📊 Hysteresis Check → leg=%s | live_SSMA=%.2f | band=[%.2f , %.2f] — No exit.",
                    leg, live_ssma, lsma_lower, lsma_upper
                )

            # ------------------------------------------------------ #
            # 4️⃣ Cooldown to avoid excessive computation
            # ------------------------------------------------------ #
            await asyncio.sleep(0.25)

        except asyncio.CancelledError:
            logging.warning("🛑 live_position_monitor() cancelled — shutting down gracefully.")
            break
        except Exception as e:
            logging.exception("⚠️ Exception in live_position_monitor(): %s", e)
            await asyncio.sleep(1)

#========================================#
### 10.0    Threading and Scheduling      
#========================================#

#----------------------------------------#
#   10.1    Startup 
#----------------------------------------#
async def startup_async():
    global tradable_df, security_id_to_name

    # 🧹 Step 0: Clean up old cached files (before building new data)
    # print("Performing startup cleanup...")
    logging.info("Performing startup cleanup...")
    try:
        archive_previous_snapshots()
        logging.info("Archived previous runtime/version snapshots.")
    except Exception as e:
        logging.exception("Error archiving previous snapshots: %s", e)
    else:
        logging.info("Archived previous runtime/version snapshots successfully.")

    try:
        cleanup_old_files(current_date)
        logging.info("Old data and log files moved to Previous_Records archive.")
    except Exception as e:
        logging.exception("Error cleaning up old files: %s", e)
    logging.info("Clearing state(s) before initialization...")
    clear_state_variables()   

    #  Download instruments list via REST (no live feed)
    # print("Building tradable instruments list...")
    logging.info("Building tradable instruments list...")
    script_list(exchange, underlying, current_date)

    # Read local file and prepare lookup
    tradable_df = pd.read_csv(os.path.join(DATA_DIR, f"Tradable_Instruments_List_{current_date}.csv"))
    security_id_to_name = dict(zip(tradable_df['SECURITY_ID'], tradable_df['DISPLAY_NAME']))

    # 🟢 Step 4: Fetch intraday data (REST only, via dhanhq)
    await get_intraday_data()
    logging.info("Retrieving updated intraday data from Dhan API.")
    
    # Order and position reconciliation at startup
    logging.info("🟢 Startup reconciliation initiated...")
    try:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, reconcile_orders_and_positions, 'startup')
        logging.info("Startup reconciliation completed.")
    except Exception as e:
        logging.exception("❌ Startup reconciliation failed: %s", e)

    # print("Startup complete.")
    logging.info("Startup complete.")

#========================================#
### Candle MidPoint Actions (Task 2)
#========================================#
async def candle_midpoint_actions():
    """
    Runs at the mid-point of every 5-minute candle.
    Purpose:
      • Refresh intraday data
      • Update/subscribe option instruments
      • Refresh open positions
      • Maintain runtime data consistency
    """
    global close_value, ssma_Value, lsma_Value, last_candle_time 

    logging.info("🟡 [MIDPOINT] Candle MidPoint Action Triggered")

    try:
        #-------------------------------------------------------------#
        # Step 1: Refresh intraday data
        #-------------------------------------------------------------#
        logging.info("Step 1️⃣ Refreshing intraday data...")
        await get_intraday_data()
        logging.info("✅ Intraday data refresh complete. close_value=%s", close_value)

        #-------------------------------------------------------------#
        # Step 2: Find required option strikes and subscribe if needed
        #-------------------------------------------------------------#
        if close_value is None:
            logging.warning("close_value is None — skipping strike selection.")
        else:
            logging.info("Step 2️⃣ Finding and subscribing required option strikes...")
            required_strikes = find_required_strikes(close_value)

            if not getattr(required_strikes, "empty", True):
                security_ids = required_strikes["SECURITY_ID"].astype(int).tolist()
                await subscribe_additional_instruments_v2(feed, security_ids)
                logging.info("✅ Subscribed to %d new instruments: %s", len(security_ids), security_ids)
            else:
                logging.info("ℹ️ No new strikes needed this cycle.")

        #-------------------------------------------------------------#
        # Step 3: Refresh Orders and Positions
        #-------------------------------------------------------------#
        logging.info("🟡 Mid-candle reconciliation initiated...")
        try:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, reconcile_orders_and_positions, 'mid')
            logging.info("Mid-candle reconciliation completed.")
        except Exception as e:
            logging.exception("❌ Mid-candle     reconciliation failed: %s", e)
        #-------------------------------------------------------------#
        # Step 4: Wrap-up
        #-------------------------------------------------------------#
        logging.info("🟢 [MIDPOINT] Candle MidPoint Action Completed.\n")

    except Exception as e:
        logging.exception("❌ Error during Candle MidPoint Actions: %s", e)
    
    logging.info("SSMA: %s LSMA: %s Close: %s Last Candle: %s",
            ssma_Value, lsma_Value, close_value, last_candle_time
        )

#========================================#
### Schedule MidPoint Actions (every 5-min midpoint)
#========================================#
async def run_every_5_minutes_midpoint(start_hour, start_minute, close_hour, close_minute):
    """
    Aligns to candle midpoints (2.5 minutes after each candle start).
    Runs candle_midpoint_actions() halfway between candle closes.
    """
    interval_sec = interval * 60
    half_interval = interval_sec / 2  # 2.5 minutes for 5-min candles
    close_time = time(close_hour, close_minute)

    while True:
        now = datetime.now()
        if now.time() > close_time:
            logging.info("Market closed, stopping midpoint job.")
            break

        # compute last candle boundary
        minute = (now.minute // interval) * interval
        candle_start = now.replace(minute=minute, second=0, microsecond=0)
        midpoint_time = candle_start + timedelta(seconds=half_interval)

        if now < midpoint_time:
            sleep_seconds = (midpoint_time - now).total_seconds()
            logging.info("⏳ Waiting %.1f sec until next midpoint at %s …", sleep_seconds, midpoint_time.time())
            await asyncio.sleep(sleep_seconds)
            await candle_midpoint_actions()
        else:
            # already past midpoint, move to next one
            next_candle_start = candle_start + timedelta(minutes=interval)
            midpoint_time = next_candle_start + timedelta(seconds=half_interval)
            sleep_seconds = (midpoint_time - now).total_seconds()
            logging.info("⏳ Waiting %.1f sec until next midpoint at %s …", sleep_seconds, midpoint_time.time())
            await asyncio.sleep(sleep_seconds)
            await candle_midpoint_actions()


async def compute_hybrid_sma_from_live_feed(close_value):
    """
    Computes SSMA and LSMA from live feed using the latest close_value 
    and stored previous_close_values_map.
    Hybrid = uses both historical Dhan data (already in map) and the live tick close.
    Thread-safe using SMA_LOCK.
    """
    global ssma_Value, lsma_Value, previous_close_values_map
    global security_id_tracked, ssma_window, lsma_window, min_period, interval

    async with SMA_LOCK:
        logging.debug("🔒 SMA_LOCK acquired for hybrid SMA computation.")
        try:
            closes_dict = previous_close_values_map.get(security_id_tracked, {})
            if not closes_dict:
                logging.warning("⚠️ previous_close_values_map empty — cannot compute SMA.")
                return

            # ✅ Clean out None or invalid close entries
            closes_dict = {k: v for k, v in closes_dict.items() if v is not None}

            # Sort by time
            sorted_items = sorted(closes_dict.items(), key=lambda x: x[0])
            closes = [v for _, v in sorted_items]

            # Add current close explicitly if not already captured
            if closes and closes[-1] != close_value:
                closes.append(float(close_value))

            # Keep only up to LSMA window size
            closes = closes[-lsma_window:]

            if len(closes) < min_period:
                logging.warning("Not enough closes to compute SMA (have=%d, need=%d)", len(closes), min_period)
                return

            # Compute SMA
            series = pd.Series(closes)
            ssma_Value = (
                round(series.rolling(ssma_window).mean().iloc[-1], 2)
                if len(closes) >= ssma_window
                else round(series.mean(), 2)
            )
            lsma_Value = (
                round(series.rolling(lsma_window).mean().iloc[-1], 2)
                if len(closes) >= lsma_window
                else round(series.mean(), 2)
            )

            logging.info(
                "📈 Recomputed Hybrid SMA → SSMA=%.2f | LSMA=%.2f | (Closes=%d)",
                ssma_Value, lsma_Value, len(closes)
            )
            logging.debug("🔓 SMA_LOCK released after SMA computation.")

        except Exception as e:
            logging.exception("❌ Error in compute_hybrid_sma_from_live_feed(): %s", e)


def update_previous_close_map(security_id, close_price, timestamp_str):
    """
    Updates the global previous_close_values_map with the latest close value.
    """
    global previous_close_values_map
    if security_id not in previous_close_values_map:
        previous_close_values_map[security_id] = {}
    previous_close_values_map[security_id][timestamp_str] = round(float(close_price), 2)

# ===============================================================#
#  🕯️ CANDLE ENDPOINT ACTIONS — LISTENER VERSION (Production Ready)
# ===============================================================#
async def candle_endpoint_actions():
    """
    Listens for live tick notifications from on_ticks() via ltp_update_condition.
    Detects 5-min candle boundary crossover and performs end-of-candle actions:
      - SMA computation
      - Order & Position reconciliation
      - Strike subscription refresh
      - Entry condition evaluation

    Safe, async, event-driven (no polling), and fits seamlessly into existing architecture.
    """

    global last_candle_time, close_value
    logging.info("🕯️ Starting candle_endpoint_actions() listener...")

    last_candle_bucket = None  # store last processed candle bucket (hour, minute//5)

    while True:
        try:
            # ------------------------------------------------------ #
            # 1️⃣ Wait for tick notification from on_ticks()
            # ------------------------------------------------------ #
            async with ltp_update_condition:
                await ltp_update_condition.wait()
                snapshot = getattr(ltp_update_condition, "snapshot", None)

            if not snapshot:
                continue  # skip empty / spurious notification

            # Only process tracked instrument
            if snapshot.get("security_id") != int(security_id_tracked):
                continue

            prev_ts = snapshot.get("prev_timestamp")
            curr_ts = snapshot.get("timestamp")
            prev_ltp = snapshot.get("prev_LTP")
            curr_ltp = snapshot.get("LTP")

            # ------------------------------------------------------ #
            # 2️⃣ Ensure valid timestamps
            # ------------------------------------------------------ #
            if not prev_ts or not curr_ts:
                continue

            prev_dt = datetime.fromtimestamp(int(prev_ts), kolkata_tz)
            curr_dt = datetime.fromtimestamp(int(curr_ts), kolkata_tz)

            prev_bucket = (prev_dt.hour, prev_dt.minute // interval)
            curr_bucket = (curr_dt.hour, curr_dt.minute // interval)

            # ------------------------------------------------------ #
            # 🕯️ 3️⃣ Detect candle boundary crossover (fixed startup trigger)
            # ------------------------------------------------------ #
            # Initialize bucket on very first tick to prevent false trigger
            if last_candle_bucket is None:
                last_candle_bucket = (curr_dt.hour, curr_dt.minute // interval)
                logging.debug(
                    "⏳ Initialized candle tracking — first tick @ %s (bucket=%s)",
                    curr_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    last_candle_bucket
                )
                continue  # Skip processing on the very first tick

            # ✅ Detect actual new 5-minute candle transition
            if last_candle_bucket != curr_bucket and prev_bucket != curr_bucket:
                last_candle_bucket = curr_bucket

                # Shift candle-close log timestamp by -5.5 hours (19800 sec)
                ts_str = datetime.fromtimestamp(curr_dt.timestamp() - 19800).strftime("%Y-%m-%d %H:%M:%S")

                logging.info("🕯️ [CANDLE CLOSE] Detected new candle @ %s", ts_str)
                logging.info(
                    "🕯️ Candle transition detected → prev_bucket=%s curr_bucket=%s prev_LTP=%.2f curr_LTP=%.2f",
                    prev_bucket, curr_bucket, (prev_ltp or 0.0), (curr_ltp or 0.0)
                )

                # -------------------------------------------------- #
                # 4️⃣ Execute Candle End Actions
                # -------------------------------------------------- #
                try:
                    await asyncio.sleep(0.1)  # brief pause for tick stability

                    # -------------------------------------------------- #
                    # 5️⃣ Update close value + previous_close_values_map
                    # -------------------------------------------------- #
                    close_value = float(prev_ltp or 0.0)
                    last_candle_time = curr_dt
                    update_previous_close_map(security_id_tracked, close_value, ts_str)
                    logging.info("💾 Closing price snapshot: %.2f | last_candle_time=%s", close_value, ts_str)

                    # -------------------------------------------------- #
                    # 6️⃣ Compute Hybrid SSMA and LSMA (Live Feed)
                    # -------------------------------------------------- #
                    await compute_hybrid_sma_from_live_feed(close_value)
                    logging.info("📈 Hybrid SSMA and LSMA computed successfully.")

                    # -------------------------------------------------- #
                    # 7️⃣ Reconcile orders & positions
                    # -------------------------------------------------- #
                    loop = asyncio.get_running_loop()
                    await loop.run_in_executor(None, reconcile_orders_and_positions, 'end')

                    # -------------------------------------------------- #
                    # 8️⃣ Refresh strikes and subscriptions
                    # -------------------------------------------------- #
                    required_strikes = find_required_strikes(close_value)
                    if not getattr(required_strikes, "empty", True):
                        ids = required_strikes["SECURITY_ID"].astype(int).tolist()
                        await subscribe_additional_instruments_v2(feed, ids)
                        logging.info("✅ Subscribed %d new instruments post-candle-close.", len(ids))
                    else:
                        logging.info("ℹ️ No new strikes required post-candle-close.")

                    # -------------------------------------------------- #
                    # 9️⃣ Evaluate Entry Conditions (after SMA refresh)
                    # -------------------------------------------------- #
                    check_entry_conditions()

                    logging.info("🔁 [CANDLE COMPLETE] All end-of-candle actions finished successfully.")

                    shifted_ts = datetime.fromtimestamp(curr_dt.timestamp() - 19800).strftime("%Y-%m-%d %H:%M:%S")

                    logging.info("🪶 Candle Summary → Close=%.2f | SSMA=%.2f | LSMA=%.2f | Time=%s",
                                close_value, ssma_Value, lsma_Value, shifted_ts)

                    logging.info("✅ Candle end actions completed for candle @ %s\n", shifted_ts)

                except Exception as e:
                    logging.exception("❌ Error during candle end processing @ %s: %s", ts_str, e)

            else:
                # 🔹 Within same candle → no boundary yet
                local_dt = datetime.fromtimestamp(curr_dt.timestamp() - 19800)
                logging.debug(
                    "Tick received within same candle [%02d:%02d] — no action.",
                    local_dt.hour,
                    (local_dt.minute // interval) * interval
                )
        except asyncio.CancelledError:
            logging.warning("🛑 candle_endpoint_actions() listener cancelled — shutting down gracefully.")
            break
        except Exception as e:
            logging.exception("⚠️ Exception in candle_endpoint_actions loop: %s", e)
            await asyncio.sleep(1)  # small cooldown to avoid rapid error loop


#################################
#   Main Function to Stratup  
#################################
async def main_func():
    # 🟢 connect feed first
    # print("Starting system initialization...")
    logging.info("Starting system initialization...")

    # 🟢 1️⃣ Run all startup preparation tasks (REST-based only)
    await startup_async()    
    logging.info("Startup tasks completed. Ready to connect to live feed.")

    # 🟢 2️⃣ Start background async tasks
    task1 = asyncio.create_task(connect_to_dhan())
    task2 = asyncio.create_task(run_every_5_minutes_midpoint(9, 5, 23, 30))     # candle midpoint
    task3 = asyncio.create_task(candle_endpoint_actions())                      # candle end (periodic)
    task4 = asyncio.create_task(live_position_monitor())                        # live position monitor

    # 🟢 start the tasks
    # print("Main async tasks started.")
    logging.info("Main async tasks started.")
    await asyncio.gather(task1, task2, task3, task4, return_exceptions=True)

#################################
#   Program Start 
#################################

if __name__ == "__main__":
    asyncio.run(main_func())