import csv
import random
import re
from datetime import date, timedelta

from faker import Faker

fake = Faker("en_US")
random.seed(42)
Faker.seed(42)

# Config 
OUTPUT_FILE   = "data/employees_raw.csv"
TOTAL_RECORDS = 1200          # > 1 000 as required
DEPARTMENTS   = ["IT", "Analytics", "HR", "Finance", "Marketing", "Operations",
                 "it", "ANALYTICS", "hr", "finance"]   # mixed case on purpose
JOB_TITLES    = ["Software Engineer", "Data Analyst", "Manager", "Director",
                 "HR Specialist", "Accountant", "Marketing Lead", "DevOps Engineer",
                 "data analyst", "SOFTWARE ENGINEER"]  # mixed case on purpose
STATUSES      = ["Active", "Inactive", "active", "ACTIVE", "inactive"]

# Helpers 

def random_salary_str() -> str:
    """Returns salary with various dirty formats."""
    amount = random.randint(30_000, 150_000)
    fmt = random.choice(["plain", "dollar", "comma", "both"])
    if fmt == "plain":
        return str(amount)
    if fmt == "dollar":
        return f"${amount}"
    if fmt == "comma":
        return f"{amount:,}"
    return f"${amount:,}"


def random_email(first: str, last: str) -> str:
    """Mix of valid and invalid email addresses."""
    domain = random.choice(["company.com", "COMPANY.COM", "corp.org", "firm.net"])
    style  = random.choice(["valid_lower", "valid_mixed", "no_at", "no_domain",
                             "double_at", "valid_lower"])
    base = f"{first.lower()}.{last.lower()}"
    if style == "valid_lower":
        return f"{base}@{domain.lower()}"
    if style == "valid_mixed":
        return f"{first}.{last}@{domain}"
    if style == "no_at":
        return f"{base}{domain}"          # missing @
    if style == "no_domain":
        return f"{base}@"                 # missing domain
    if style == "double_at":
        return f"{base}@@{domain}"
    return f"{base}@{domain.lower()}"


def random_hire_date() -> str:
    """Mostly past dates; ~5 % future (data errors)."""
    if random.random() < 0.05:
        future = date.today() + timedelta(days=random.randint(30, 730))
        return future.isoformat()
    start = date(2000, 1, 1)
    delta = (date.today() - start).days
    return (start + timedelta(days=random.randint(0, delta))).isoformat()


def random_birth_date() -> str | None:
    """Age 22–65; ~3 % missing."""
    if random.random() < 0.03:
        return ""
    start = date(1960, 1, 1)
    end   = date(2002, 12, 31)
    delta = (end - start).days
    return (start + timedelta(days=random.randint(0, delta))).isoformat()


def maybe_null(value: str, prob: float = 0.05) -> str:
    """Randomly blank a non-critical field."""
    return "" if random.random() < prob else value


# Generate rows
rows = []
employee_ids = list(range(1001, 1001 + TOTAL_RECORDS))
# Inject ~50 duplicate IDs for dedup testing
duplicates = random.sample(employee_ids[:500], 50)

for idx in employee_ids:
    first = fake.first_name()
    last  = fake.last_name()

    # Intentionally inconsistent casing on names (~30 %)
    if random.random() < 0.15:
        first = first.upper()
    elif random.random() < 0.15:
        first = first.lower()

    row = {
        "employee_id": idx,
        "first_name":  first,
        "last_name":   last,
        "email":       random_email(re.sub(r"\s+", "", first), re.sub(r"\s+", "", last)),
        "hire_date":   random_hire_date(),
        "job_title":   maybe_null(random.choice(JOB_TITLES), 0.04),
        "department":  maybe_null(random.choice(DEPARTMENTS), 0.03),
        "salary":      random_salary_str() if random.random() > 0.02 else "",
        "manager_id":  maybe_null(str(random.randint(2001, 2050)), 0.10),
        "address":     maybe_null(fake.street_address(), 0.05),
        "city":        maybe_null(fake.city(), 0.04),
        "state":       maybe_null(fake.state_abbr(), 0.04),
        "zip_code":    maybe_null(fake.zipcode(), 0.04),
        "birth_date":  random_birth_date(),
        "status":      random.choice(STATUSES),
    }
    rows.append(row)

# Append duplicate rows
for dup_id in duplicates:
    original = next(r for r in rows if r["employee_id"] == dup_id)
    dup = original.copy()
    dup["salary"] = random_salary_str()   # slightly different to be realistic
    rows.append(dup)

random.shuffle(rows)

# Write CSV
FIELDS = ["employee_id", "first_name", "last_name", "email", "hire_date",
          "job_title", "department", "salary", "manager_id", "address",
          "city", "state", "zip_code", "birth_date", "status"]

with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=FIELDS)
    writer.writeheader()
    writer.writerows(rows)

print(f"✅  Generated {len(rows)} rows → {OUTPUT_FILE}")
print(f"   Duplicates injected : {len(duplicates)}")
print(f"   Total unique IDs    : {TOTAL_RECORDS}")
