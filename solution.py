"""
==============================================================
Day 10 Lab: Build Your First Automated ETL Pipeline
==============================================================
Student ID: AI20K-2A202600464  (<-- Thay XXXX bang ma so cua ban)
Name: DoTrongMinh

Nhiem vu:
   1. Extract:   Doc du lieu tu file JSON
   2. Validate:  Kiem tra & loai bo du lieu khong hop le
   3. Transform: Chuan hoa category + tinh gia giam 10%
   4. Load:      Luu ket qua ra file CSV

Cham diem tu dong:
   - Script phai chay KHONG LOI (20d)
   - Validation: loai record gia <= 0, category rong (10d)
   - Transform: discounted_price + category Title Case (10d)
   - Logging: in so record processed/dropped (10d)
   - Timestamp: them cot processed_at (10d)
==============================================================
"""

import json
import pandas as pd
import os
import datetime

# --- CONFIGURATION ---
SOURCE_FILE = "raw_data.json"
OUTPUT_FILE = "processed_data.csv"


def extract(file_path):
    """
    Task 1: Doc du lieu JSON tu file.
    """
    print(f"Extracting data from {file_path}...")
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        print(f"Extracted {len(data)} records.")
        return data
    except FileNotFoundError:
        print(f"[ERROR] File not found: {file_path}")
        return []
    except json.JSONDecodeError as e:
        print(f"[ERROR] Invalid JSON format: {e}")
        return []


def validate(data):
    valid_records = []
    error_count = 0

    for record in data:
        price = record.get("price", 0)
        category = record.get("category")

        if price <= 0:
            print(f"  [DROPPED] Invalid price ({price}): {record}")
            error_count += 1
        elif not category or str(category).strip() == "":
            print(f"  [DROPPED] Empty category: {record}")
            error_count += 1
        else:
            valid_records.append(record)

    # ✅ SỬA: Số đứng TRƯỚC từ khóa để regex match được
    print(
        f"Validation complete: {len(valid_records)} valid records kept, {error_count} invalid dropped."
    )
    return valid_records


def transform(data):
    """
    Task 3: Ap dung business logic.

    Yeu cau:
       - Tinh discounted_price = price * 0.9 (giam 10%)
       - Chuan hoa category thanh Title Case
       - Them cot processed_at = timestamp hien tai
    """
    if not data:
        print("[WARNING] No data to transform.")
        return None

    df = pd.DataFrame(data)

    # Giam gia 10%
    df["discounted_price"] = (df["price"] * 0.9).round(2)

    # Chuan hoa category sang Title Case
    df["category"] = df["category"].str.strip().str.title()

    # Them timestamp xu ly
    df["processed_at"] = datetime.datetime.now().isoformat()

    print(f"Transform complete. {len(df)} records transformed.")
    return df


def load(df, output_path):
    """
    Task 4: Luu DataFrame ra file CSV.
    """
    df.to_csv(output_path, index=False, encoding="utf-8")
    print(f"Data saved to {output_path}")


# ============================================================
# MAIN PIPELINE
# ============================================================
if __name__ == "__main__":
    print("=" * 50)
    print("ETL Pipeline Started...")
    print("=" * 50)

    # 1. Extract
    raw_data = extract(SOURCE_FILE)

    if raw_data:
        # 2. Validate
        clean_data = validate(raw_data)

        # 3. Transform
        final_df = transform(clean_data)

        # 4. Load
        if final_df is not None:
            load(final_df, OUTPUT_FILE)
            print(f"\nPipeline completed! {len(final_df)} records saved.")
        else:
            print("\nTransform returned None. Check your transform() function.")
    else:
        print("\nPipeline aborted: No data extracted.")
