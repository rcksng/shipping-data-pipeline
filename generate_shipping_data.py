"""
Synthetic Shipping Data Generator
===================================
Generates realistic Salesforce-style source data for the shipping analytics pipeline project.

Objects generated (mirroring real Salesforce structure):
  - accounts       → shipping customers / companies
  - contacts       → customer contacts (like SF Contact object)
  - contracts      → freight contracts between Maersk and accounts
  - bookings       → cargo bookings (like SF Opportunity/Order)
  - vessels        → ships (custom SF object)
  - ports          → port locations (custom SF object)
  - cargo_events   → vessel movement events (like SF Activity/Event)
  - cases          → customer service cases (like SF Case object)
  - users          → internal Maersk users (like SF User object)

SCD2 scenarios built in (intentional changes over time):
  - Vessels change flag (country of registration)
  - Vessels change owner/operator
  - Accounts change tier (Bronze → Silver → Gold)
  - Accounts change region assignment
  - Contracts change rate tier

Output: CSV files in ./data/raw/<object>/ folders
        (mimics how Salesforce Bulk API V2 drops CSV files)

Usage:
  python generate_shipping_data.py
  python generate_shipping_data.py --records 5000 --start-date 2022-01-01
"""

import os
import csv
import random
import argparse
import hashlib
from datetime import datetime, timedelta, date
from faker import Faker

fake = Faker()
random.seed(42)
Faker.seed(42)

# ── CLI args ────────────────────────────────────────────────────────────────
parser = argparse.ArgumentParser(description="Generate synthetic shipping data")
parser.add_argument("--records",    type=int, default=2000,       help="Base record count for bookings")
parser.add_argument("--start-date", type=str, default="2022-01-01", help="Start date YYYY-MM-DD")
parser.add_argument("--end-date",   type=str, default="2024-12-31", help="End date YYYY-MM-DD")
parser.add_argument("--output-dir", type=str, default="./data/raw",  help="Output directory")
args = parser.parse_args()

START_DATE = datetime.strptime(args.start_date, "%Y-%m-%d")
END_DATE   = datetime.strptime(args.end_date,   "%Y-%m-%d")
OUT_DIR    = args.output_dir

# ── Helpers ──────────────────────────────────────────────────────────────────
def rand_date(start=START_DATE, end=END_DATE):
    delta = end - start
    return start + timedelta(days=random.randint(0, delta.days))

def rand_datetime(start=START_DATE, end=END_DATE):
    d = rand_date(start, end)
    return datetime(d.year, d.month, d.day,
                    random.randint(0, 23), random.randint(0, 59), random.randint(0, 59))

def sf_id(prefix, n):
    """Mimic Salesforce 18-char ID style"""
    h = hashlib.md5(f"{prefix}{n}".encode()).hexdigest()[:12].upper()
    return f"{prefix}{h}"

def make_dir(path):
    os.makedirs(path, exist_ok=True)

def write_csv(folder, filename, rows, fieldnames):
    make_dir(folder)
    filepath = os.path.join(folder, filename)
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"  ✓ {filepath}  ({len(rows)} rows)")
    return filepath

# ── Reference Data ────────────────────────────────────────────────────────────
REGIONS = ["Asia Pacific", "Europe", "North America", "Middle East", "Latin America", "Africa"]
COUNTRIES = {
    "Asia Pacific":   ["CN", "SG", "JP", "KR", "IN", "AU", "TH", "MY"],
    "Europe":         ["DE", "NL", "GB", "FR", "DK", "NO", "SE", "PL"],
    "North America":  ["US", "CA", "MX"],
    "Middle East":    ["AE", "SA", "QA", "KW", "OM"],
    "Latin America":  ["BR", "CO", "CL", "PE", "AR"],
    "Africa":         ["ZA", "EG", "NG", "KE", "MA"],
}
ACCOUNT_TIERS   = ["Bronze", "Silver", "Gold", "Platinum"]
VESSEL_TYPES    = ["Container Ship", "Feeder Vessel", "ULCV", "Panamax", "Sub-Panamax"]
VESSEL_FLAGS    = ["DK", "SG", "PA", "LR", "BS", "MH", "CY", "MT"]   # common ship registries
CARGO_TYPES     = ["Dry Goods", "Refrigerated", "Hazardous", "Electronics", "Automotive", "Bulk"]
PORT_TYPES      = ["Hub Port", "Feeder Port", "Transshipment Port"]
BOOKING_STATUS  = ["Confirmed", "In Transit", "Delivered", "Cancelled", "On Hold"]
CASE_CATEGORIES = ["Booking Issue", "Billing Dispute", "Cargo Damage", "Delay Inquiry",
                   "Documentation", "Container Return"]
CASE_STATUS     = ["New", "In Progress", "Resolved", "Closed", "Escalated"]
CASE_PRIORITY   = ["Low", "Medium", "High", "Critical"]
CONTRACT_TYPES  = ["Spot Rate", "Annual Contract", "Multi-Year Contract", "Preferred Customer"]

# ── 1. USERS (Maersk internal — mirrors SF User object) ──────────────────────
print("\n[1/9] Generating users...")
N_USERS = 60
DEPARTMENTS = ["Commercial", "Operations", "Customer Service", "Finance",
                "IT", "Legal", "Strategy"]
ROLES = {
    "Commercial":       ["Account Manager", "Regional Sales Director", "Sales Executive"],
    "Operations":       ["Operations Manager", "Port Coordinator", "Vessel Planner"],
    "Customer Service": ["CS Agent", "CS Team Lead", "CS Manager"],
    "Finance":          ["Finance Analyst", "Credit Controller", "Finance Manager"],
    "IT":               ["Data Engineer", "BI Developer", "Systems Analyst"],
    "Legal":            ["Contract Manager", "Compliance Officer"],
    "Strategy":         ["Strategy Analyst", "Commercial Analyst"],
}

users = []
for i in range(1, N_USERS + 1):
    dept = random.choice(DEPARTMENTS)
    role = random.choice(ROLES[dept])
    region = random.choice(REGIONS)
    created = rand_date(START_DATE, START_DATE + timedelta(days=365))
    users.append({
        "user_id":          sf_id("USR", i),
        "first_name":       fake.first_name(),
        "last_name":        fake.last_name(),
        "email":            fake.company_email(),
        "department":       dept,
        "role":             role,
        "region":           region,
        "is_active":        random.choices([True, False], weights=[90, 10])[0],
        "created_date":     created.strftime("%Y-%m-%d"),
        "last_login_date":  rand_date(created, END_DATE).strftime("%Y-%m-%d"),
        "_extracted_at":    datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
    })

write_csv(f"{OUT_DIR}/users", "users_full.csv", users,
          ["user_id","first_name","last_name","email","department","role",
           "region","is_active","created_date","last_login_date","_extracted_at"])

# ── 2. PORTS ──────────────────────────────────────────────────────────────────
print("\n[2/9] Generating ports...")
PORT_MASTER = [
    ("SGSIN", "Port of Singapore",       "SG", "Asia Pacific",   "Hub Port",             True),
    ("CNSHA", "Port of Shanghai",         "CN", "Asia Pacific",   "Hub Port",             True),
    ("NLRTM", "Port of Rotterdam",        "NL", "Europe",         "Hub Port",             True),
    ("DEHAM", "Port of Hamburg",          "DE", "Europe",         "Hub Port",             True),
    ("USLAX", "Port of Los Angeles",      "US", "North America",  "Hub Port",             True),
    ("AEDXB", "Port of Jebel Ali",        "AE", "Middle East",    "Hub Port",             True),
    ("INMAA", "Port of Chennai",          "IN", "Asia Pacific",   "Feeder Port",          True),
    ("INNSN", "Nhava Sheva",              "IN", "Asia Pacific",   "Feeder Port",          True),
    ("GBFXT", "Port of Felixstowe",       "GB", "Europe",         "Feeder Port",          True),
    ("KRPUS", "Port of Busan",            "KR", "Asia Pacific",   "Hub Port",             True),
    ("JPYOK", "Port of Yokohama",         "JP", "Asia Pacific",   "Feeder Port",          True),
    ("BRVIX", "Port of Vitória",          "BR", "Latin America",  "Feeder Port",          True),
    ("ZACPT", "Port of Cape Town",        "ZA", "Africa",         "Feeder Port",          False),
    ("MAPTM", "Port of Tanger Med",       "MA", "Africa",         "Transshipment Port",   True),
    ("COCTG", "Port of Cartagena",        "CO", "Latin America",  "Transshipment Port",   True),
]

ports = []
for i, (code, name, country, region, ptype, is_active) in enumerate(PORT_MASTER, 1):
    # SCD2 scenario: some ports get reclassified (type changes)
    original_type = ptype
    ports.append({
        "port_id":            sf_id("PRT", i),
        "port_code":          code,
        "port_name":          name,
        "country_code":       country,
        "region":             region,
        "port_type":          ptype,
        "annual_capacity_teu": random.randint(500_000, 25_000_000),
        "is_active":          is_active,
        "last_updated":       rand_date(START_DATE, START_DATE + timedelta(days=180)).strftime("%Y-%m-%d"),
        "_extracted_at":      datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
    })

# SCD2 scenario: reclassify 3 ports mid-timeline (creates changed records for silver SCD2)
port_changes = []
for port in random.sample(ports, 3):
    change_date = rand_date(START_DATE + timedelta(days=365), END_DATE)
    changed = dict(port)
    changed["port_type"] = random.choice([t for t in PORT_TYPES if t != port["port_type"]])
    changed["annual_capacity_teu"] = int(port["annual_capacity_teu"] * random.uniform(1.1, 1.5))
    changed["last_updated"] = change_date.strftime("%Y-%m-%d")
    changed["_extracted_at"] = (change_date + timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    port_changes.append(changed)

write_csv(f"{OUT_DIR}/ports", "ports_full.csv", ports,
          ["port_id","port_code","port_name","country_code","region","port_type",
           "annual_capacity_teu","is_active","last_updated","_extracted_at"])
write_csv(f"{OUT_DIR}/ports", "ports_changes.csv", port_changes,
          ["port_id","port_code","port_name","country_code","region","port_type",
           "annual_capacity_teu","is_active","last_updated","_extracted_at"])

PORT_IDS   = [p["port_id"]   for p in ports]
PORT_CODES = {p["port_id"]:  p["port_code"] for p in ports}

# ── 3. VESSELS ────────────────────────────────────────────────────────────────
print("\n[3/9] Generating vessels...")
N_VESSELS = 40
OPERATORS = ["Maersk Line", "Sealand", "MCC Transport", "Seago Line", "Safmarine"]

vessels = []
for i in range(1, N_VESSELS + 1):
    vtype   = random.choice(VESSEL_TYPES)
    flag    = random.choice(VESSEL_FLAGS)
    created = rand_date(START_DATE, START_DATE + timedelta(days=180))
    vessels.append({
        "vessel_id":         sf_id("VSL", i),
        "imo_number":        f"IMO{random.randint(1000000, 9999999)}",
        "vessel_name":       f"Maersk {fake.last_name()}",
        "vessel_type":       vtype,
        "flag_country":      flag,
        "operator":          random.choice(OPERATORS),
        "capacity_teu":      random.choice([2500, 4500, 8000, 12000, 18000, 22000]),
        "year_built":        random.randint(2005, 2023),
        "is_active":         random.choices([True, False], weights=[92, 8])[0],
        "home_port_code":    random.choice(["SGSIN", "NLRTM", "CNSHA", "DEHAM"]),
        "last_updated":      created.strftime("%Y-%m-%d"),
        "_extracted_at":     datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
    })

# SCD2 scenario: vessels change flag or operator mid-timeline
vessel_changes = []
for vessel in random.sample(vessels, 8):
    change_date = rand_date(START_DATE + timedelta(days=300), END_DATE)
    changed = dict(vessel)
    if random.random() > 0.5:
        changed["flag_country"] = random.choice([f for f in VESSEL_FLAGS if f != vessel["flag_country"]])
    else:
        changed["operator"] = random.choice([o for o in OPERATORS if o != vessel["operator"]])
    changed["last_updated"]  = change_date.strftime("%Y-%m-%d")
    changed["_extracted_at"] = (change_date + timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
    vessel_changes.append(changed)

write_csv(f"{OUT_DIR}/vessels", "vessels_full.csv", vessels,
          ["vessel_id","imo_number","vessel_name","vessel_type","flag_country","operator",
           "capacity_teu","year_built","is_active","home_port_code","last_updated","_extracted_at"])
write_csv(f"{OUT_DIR}/vessels", "vessels_changes.csv", vessel_changes,
          ["vessel_id","imo_number","vessel_name","vessel_type","flag_country","operator",
           "capacity_teu","year_built","is_active","home_port_code","last_updated","_extracted_at"])

VESSEL_IDS = [v["vessel_id"] for v in vessels]

# ── 4. ACCOUNTS (Salesforce Account object) ───────────────────────────────────
print("\n[4/9] Generating accounts...")
N_ACCOUNTS = 300

accounts = []
for i in range(1, N_ACCOUNTS + 1):
    region  = random.choice(REGIONS)
    country = random.choice(COUNTRIES[region])
    tier    = random.choices(ACCOUNT_TIERS, weights=[40, 30, 20, 10])[0]
    owner   = random.choice(users)["user_id"]
    created = rand_date(START_DATE, START_DATE + timedelta(days=365))
    accounts.append({
        "account_id":          sf_id("ACC", i),
        "account_name":        fake.company(),
        "industry":            random.choice(["Retail", "Manufacturing", "Automotive",
                                              "Chemicals", "Agriculture", "Technology",
                                              "Pharmaceuticals", "FMCG"]),
        "account_tier":        tier,
        "region":              region,
        "country_code":        country,
        "annual_revenue_usd":  random.randint(1_000_000, 500_000_000),
        "employee_count":      random.randint(50, 50000),
        "primary_trade_lane":  f"{random.choice(COUNTRIES['Asia Pacific'])}→{random.choice(COUNTRIES['Europe'])}",
        "owner_user_id":       owner,
        "is_active":           random.choices([True, False], weights=[88, 12])[0],
        "created_date":        created.strftime("%Y-%m-%d"),
        "last_modified_date":  rand_date(created, END_DATE).strftime("%Y-%m-%d"),
        "_extracted_at":       datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
    })

# SCD2 scenario: account tier upgrades (Bronze→Silver→Gold) and region reassignment
account_changes = []
for account in random.sample(accounts, 40):
    change_date = rand_date(START_DATE + timedelta(days=400), END_DATE)
    changed = dict(account)
    current_tier_idx = ACCOUNT_TIERS.index(account["account_tier"])
    if current_tier_idx < len(ACCOUNT_TIERS) - 1 and random.random() > 0.3:
        changed["account_tier"] = ACCOUNT_TIERS[current_tier_idx + 1]   # upgrade
    else:
        changed["region"]       = random.choice([r for r in REGIONS if r != account["region"]])
        changed["country_code"] = random.choice(COUNTRIES[changed["region"]])
    changed["last_modified_date"] = change_date.strftime("%Y-%m-%d")
    changed["_extracted_at"]      = (change_date + timedelta(hours=3)).strftime("%Y-%m-%dT%H:%M:%SZ")
    account_changes.append(changed)

write_csv(f"{OUT_DIR}/accounts", "accounts_full.csv", accounts,
          ["account_id","account_name","industry","account_tier","region","country_code",
           "annual_revenue_usd","employee_count","primary_trade_lane","owner_user_id",
           "is_active","created_date","last_modified_date","_extracted_at"])
write_csv(f"{OUT_DIR}/accounts", "accounts_changes.csv", account_changes,
          ["account_id","account_name","industry","account_tier","region","country_code",
           "annual_revenue_usd","employee_count","primary_trade_lane","owner_user_id",
           "is_active","created_date","last_modified_date","_extracted_at"])

ACCOUNT_IDS = [a["account_id"] for a in accounts]

# ── 5. CONTACTS (Salesforce Contact object) ───────────────────────────────────
print("\n[5/9] Generating contacts...")
N_CONTACTS = 600

contacts = []
for i in range(1, N_CONTACTS + 1):
    account = random.choice(accounts)
    created = rand_date(datetime.strptime(account["created_date"], "%Y-%m-%d"), END_DATE)
    contacts.append({
        "contact_id":       sf_id("CON", i),
        "account_id":       account["account_id"],
        "first_name":       fake.first_name(),
        "last_name":        fake.last_name(),
        "email":            fake.company_email(),
        "phone":            fake.phone_number()[:20],
        "job_title":        random.choice(["Logistics Manager", "Supply Chain Director",
                                           "Procurement Manager", "Operations Lead",
                                           "Freight Coordinator", "VP Logistics", "CFO"]),
        "contact_type":     random.choice(["Primary", "Billing", "Operations", "Technical"]),
        "is_active":        random.choices([True, False], weights=[85, 15])[0],
        "created_date":     created.strftime("%Y-%m-%d"),
        "_extracted_at":    datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
    })

write_csv(f"{OUT_DIR}/contacts", "contacts_full.csv", contacts,
          ["contact_id","account_id","first_name","last_name","email","phone",
           "job_title","contact_type","is_active","created_date","_extracted_at"])

CONTACT_IDS = [c["contact_id"] for c in contacts]

# ── 6. CONTRACTS ──────────────────────────────────────────────────────────────
print("\n[6/9] Generating contracts...")
N_CONTRACTS = 500

contracts_list = []
for i in range(1, N_CONTRACTS + 1):
    account   = random.choice(accounts)
    owner     = random.choice(users)["user_id"]
    ctype     = random.choice(CONTRACT_TYPES)
    start_dt  = rand_date(START_DATE, END_DATE - timedelta(days=90))
    duration  = random.choice([90, 180, 365, 730])
    end_dt    = min(start_dt + timedelta(days=duration), END_DATE + timedelta(days=365))
    rate_tier = random.choice(["Standard", "Preferred", "Premium", "Key Account"])
    contracts_list.append({
        "contract_id":          sf_id("CTR", i),
        "account_id":           account["account_id"],
        "contract_name":        f"{account['account_name']} - {ctype} {start_dt.year}",
        "contract_type":        ctype,
        "rate_tier":            rate_tier,
        "origin_region":        account["region"],
        "destination_region":   random.choice([r for r in REGIONS if r != account["region"]]),
        "cargo_type":           random.choice(CARGO_TYPES),
        "volume_commitment_teu": random.choice([100, 250, 500, 1000, 2500, 5000]),
        "rate_per_teu_usd":     random.randint(800, 4500),
        "contract_value_usd":   random.randint(50_000, 10_000_000),
        "start_date":           start_dt.strftime("%Y-%m-%d"),
        "end_date":             end_dt.strftime("%Y-%m-%d"),
        "status":               random.choices(
                                    ["Active", "Expired", "Draft", "Cancelled"],
                                    weights=[50, 30, 10, 10])[0],
        "owner_user_id":        owner,
        "created_date":         start_dt.strftime("%Y-%m-%d"),
        "_extracted_at":        datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
    })

write_csv(f"{OUT_DIR}/contracts", "contracts_full.csv", contracts_list,
          ["contract_id","account_id","contract_name","contract_type","rate_tier",
           "origin_region","destination_region","cargo_type","volume_commitment_teu",
           "rate_per_teu_usd","contract_value_usd","start_date","end_date",
           "status","owner_user_id","created_date","_extracted_at"])

CONTRACT_IDS = [c["contract_id"] for c in contracts_list]

# ── 7. BOOKINGS (core fact source — SF Opportunity/Order) ─────────────────────
print("\n[7/9] Generating bookings...")
N_BOOKINGS = args.records

bookings = []
for i in range(1, N_BOOKINGS + 1):
    account     = random.choice(accounts)
    contact     = random.choice([c for c in contacts if c["account_id"] == account["account_id"]] or contacts)
    contract    = random.choice([c for c in contracts_list if c["account_id"] == account["account_id"]] or contracts_list)
    vessel      = random.choice(vessels)
    origin_port = random.choice(ports)
    dest_port   = random.choice([p for p in ports if p["port_id"] != origin_port["port_id"]])
    owner       = random.choice(users)["user_id"]

    booking_dt  = rand_datetime()
    etd         = booking_dt + timedelta(days=random.randint(3, 30))
    transit     = random.randint(14, 45)
    eta         = etd + timedelta(days=transit)
    teu         = random.choice([1, 2, 5, 10, 20, 40])
    rate        = random.randint(800, 5000)

    # Introduce some data quality issues (realistic raw data problems)
    cargo_type  = random.choice(CARGO_TYPES)
    if random.random() < 0.03:  cargo_type = None      # 3% nulls
    if random.random() < 0.02:  teu = -teu             # 2% negative TEU (bad data)

    bookings.append({
        "booking_id":           sf_id("BKG", i),
        "booking_reference":    f"MAE{random.randint(1000000, 9999999)}",
        "account_id":           account["account_id"],
        "contact_id":           contact["contact_id"],
        "contract_id":          contract["contract_id"],
        "vessel_id":            vessel["vessel_id"],
        "origin_port_id":       origin_port["port_id"],
        "destination_port_id":  dest_port["port_id"],
        "cargo_type":           cargo_type,
        "container_size":       random.choice(["20ft", "40ft", "40ft HC", "45ft"]),
        "teu_count":            teu,
        "booking_status":       random.choices(BOOKING_STATUS, weights=[10, 20, 55, 10, 5])[0],
        "freight_rate_usd":     rate,
        "total_revenue_usd":    round(teu * rate * random.uniform(0.9, 1.1), 2),
        "booking_date":         booking_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "etd":                  etd.strftime("%Y-%m-%d"),
        "eta":                  eta.strftime("%Y-%m-%d"),
        "actual_departure":     (etd + timedelta(days=random.randint(-2, 5))).strftime("%Y-%m-%d")
                                if random.random() > 0.15 else None,
        "actual_arrival":       (eta + timedelta(days=random.randint(-3, 10))).strftime("%Y-%m-%d")
                                if random.random() > 0.25 else None,
        "owner_user_id":        owner,
        "created_date":         booking_dt.strftime("%Y-%m-%d"),
        "last_modified_date":   (booking_dt + timedelta(days=random.randint(0, 30))).strftime("%Y-%m-%d"),
        "_extracted_at":        datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
    })

write_csv(f"{OUT_DIR}/bookings", "bookings_full.csv", bookings,
          ["booking_id","booking_reference","account_id","contact_id","contract_id",
           "vessel_id","origin_port_id","destination_port_id","cargo_type",
           "container_size","teu_count","booking_status","freight_rate_usd",
           "total_revenue_usd","booking_date","etd","eta","actual_departure",
           "actual_arrival","owner_user_id","created_date","last_modified_date","_extracted_at"])

# ── 8. CARGO EVENTS (vessel movement log) ─────────────────────────────────────
print("\n[8/9] Generating cargo events...")
EVENT_TYPES = ["Departed", "Arrived", "Loaded", "Discharged",
               "In Transit", "Customs Hold", "Delayed", "Delivered"]

cargo_events = []
event_counter = 1
for booking in random.sample(bookings, min(len(bookings), args.records)):
    if booking["booking_status"] in ["Cancelled"]:
        continue
    n_events = random.randint(2, 6)
    event_dt = datetime.strptime(booking["booking_date"], "%Y-%m-%dT%H:%M:%SZ")
    for j in range(n_events):
        event_dt += timedelta(days=random.randint(1, 8), hours=random.randint(0, 23))
        if event_dt > END_DATE:
            break
        cargo_events.append({
            "event_id":          sf_id("EVT", event_counter),
            "booking_id":        booking["booking_id"],
            "vessel_id":         booking["vessel_id"],
            "port_id":           random.choice([booking["origin_port_id"],
                                                booking["destination_port_id"],
                                                random.choice(PORT_IDS)]),
            "event_type":        EVENT_TYPES[min(j, len(EVENT_TYPES)-1)],
            "event_timestamp":   event_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "location_lat":      round(random.uniform(-60, 70), 6),
            "location_lon":      round(random.uniform(-180, 180), 6),
            "delay_hours":       random.choice([0, 0, 0, 0, 4, 8, 12, 24, 48])
                                 if random.random() < 0.2 else 0,
            "notes":             random.choice([None, None, None,
                                                "Weather delay", "Port congestion",
                                                "Customs inspection", "Equipment issue"]),
            "_extracted_at":     datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        })
        event_counter += 1

write_csv(f"{OUT_DIR}/cargo_events", "cargo_events_full.csv", cargo_events,
          ["event_id","booking_id","vessel_id","port_id","event_type","event_timestamp",
           "location_lat","location_lon","delay_hours","notes","_extracted_at"])

# ── 9. CASES (Salesforce Case object) ─────────────────────────────────────────
print("\n[9/9] Generating cases...")
N_CASES = 800

cases = []
for i in range(1, N_CASES + 1):
    account   = random.choice(accounts)
    contact   = random.choice([c for c in contacts if c["account_id"] == account["account_id"]] or contacts)
    booking   = random.choice([b for b in bookings if b["account_id"] == account["account_id"]] or bookings)
    owner     = random.choice(users)["user_id"]
    created   = rand_date(START_DATE, END_DATE - timedelta(days=14))
    resolved  = created + timedelta(days=random.randint(1, 60))
    status    = random.choices(CASE_STATUS, weights=[15, 25, 35, 20, 5])[0]
    cases.append({
        "case_id":           sf_id("CAS", i),
        "case_number":       f"CS-{random.randint(100000, 999999)}",
        "account_id":        account["account_id"],
        "contact_id":        contact["contact_id"],
        "booking_id":        booking["booking_id"],
        "subject":           f"{random.choice(CASE_CATEGORIES)} - {booking['booking_reference']}",
        "category":          random.choice(CASE_CATEGORIES),
        "priority":          random.choices(CASE_PRIORITY, weights=[35, 40, 20, 5])[0],
        "status":            status,
        "owner_user_id":     owner,
        "created_date":      created.strftime("%Y-%m-%d"),
        "resolved_date":     resolved.strftime("%Y-%m-%d") if status in ["Resolved", "Closed"] else None,
        "resolution_days":   (resolved - created).days if status in ["Resolved", "Closed"] else None,
        "customer_rating":   random.randint(1, 5) if status in ["Resolved", "Closed"] else None,
        "_extracted_at":     datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
    })

write_csv(f"{OUT_DIR}/cases", "cases_full.csv", cases,
          ["case_id","case_number","account_id","contact_id","booking_id","subject",
           "category","priority","status","owner_user_id","created_date","resolved_date",
           "resolution_days","customer_rating","_extracted_at"])

# ── Summary ───────────────────────────────────────────────────────────────────
print("\n" + "="*60)
print("DATA GENERATION COMPLETE")
print("="*60)
print(f"\nOutput directory : {OUT_DIR}")
print(f"\nObjects generated:")
print(f"  users          : {N_USERS:>6,} rows")
print(f"  ports          : {len(ports):>6,} rows  (+{len(port_changes)} SCD2 changes)")
print(f"  vessels        : {N_VESSELS:>6,} rows  (+{len(vessel_changes)} SCD2 changes)")
print(f"  accounts       : {N_ACCOUNTS:>6,} rows  (+{len(account_changes)} SCD2 changes)")
print(f"  contacts       : {N_CONTACTS:>6,} rows")
print(f"  contracts      : {N_CONTRACTS:>6,} rows")
print(f"  bookings       : {N_BOOKINGS:>6,} rows  (includes intentional data quality issues)")
print(f"  cargo_events   : {len(cargo_events):>6,} rows")
print(f"  cases          : {N_CASES:>6,} rows")
print(f"\nSCD2 scenarios built in:")
print(f"  - Vessels: flag_country and operator changes")
print(f"  - Ports:   port_type and capacity changes")
print(f"  - Accounts: tier upgrades and region reassignments")
print(f"\nIntentional data quality issues in bookings:")
print(f"  - ~3% null cargo_type values")
print(f"  - ~2% negative teu_count values (bad source data)")
print(f"  - ~15% missing actual_departure dates")
print(f"  - ~25% missing actual_arrival dates")
print(f"\nNext step: load these CSVs into Databricks as your bronze layer.")
print(f"  See notebooks/01_bronze_ingestion.py")
print("="*60)
