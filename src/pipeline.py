"""
Data Preparation Pipeline - Automobile Dataset

Alur ETL:
    Extract   : membaca dataset mentah dari data/raw/
    Transform : inspeksi -> cleaning -> transformasi
    Load      : menyimpan hasil ke data/processed/

Jalankan dari root project:
    python src/pipeline.py
"""

import re
from pathlib import Path

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Konfigurasi
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent.parent
RAW_PATH = BASE_DIR / "data" / "raw" / "automobileEDA_dirty_training.csv"
PROCESSED_PATH = BASE_DIR / "data" / "processed" / "automobileEDA_processed.csv"

DATE_COL = "transaction_date"
TARGET_COL = "price"

# Kolom kategorikal (teks) yang relevan untuk diperiksa dan dibersihkan
CATEGORICAL_COLS = [
    "make", "aspiration", "num-of-doors", "body-style", "drive-wheels",
    "engine-location", "engine-type", "num-of-cylinders", "fuel-system",
    "horsepower-binned",
]

# Sinonim kategori yang perlu diseragamkan (setelah huruf kecil)
CATEGORY_SYNONYMS = {
    "drive-wheels": {"awd": "4wd"},  # AWD = all-wheel drive = 4wd
}

# Kolom yang dipakai untuk mencocokkan mobil dengan spesifikasi identik
SPEC_COLS = ["wheel-base", "engine-size", "horsepower", "engine-type", "fuel-system", "bore"]

# Transformasi
MINMAX_COLS = ["horsepower", "engine-size", "curb-weight"]
ONEHOT_COLS = ["body-style", "drive-wheels"]
ORDINAL_MAPS = {
    "num-of-doors": {"two": 2, "four": 4},
    "num-of-cylinders": {
        "two": 2, "three": 3, "four": 4, "five": 5,
        "six": 6, "eight": 8, "twelve": 12,
    },
    "horsepower-binned": {"Low": 0, "Medium": 1, "High": 2},
}


def print_section(title: str) -> None:
    """Mencetak judul bagian agar output terminal mudah dibaca."""
    print("\n" + "=" * 72)
    print(title)
    print("=" * 72)


def text_columns(df: pd.DataFrame) -> list[str]:
    """Daftar kolom bertipe teks (kompatibel dengan pandas 2 dan 3)."""
    return [c for c in df.columns if pd.api.types.is_string_dtype(df[c])]


def date_pattern(value) -> str:
    """Mengubah string tanggal menjadi pola, mis. '2025-01-01' -> '9999-99-99'."""
    if pd.isna(value):
        return "<missing>"
    return re.sub(r"\d", "9", re.sub(r"[A-Za-z]", "A", str(value)))


# ---------------------------------------------------------------------------
# 1. EXTRACT
# ---------------------------------------------------------------------------
def load_data(path: Path) -> pd.DataFrame:
    """Membaca dataset CSV mentah."""
    print_section("1. LOAD DATA (Extract)")
    df = pd.read_csv(path)
    print(f"Dataset dimuat dari : {path}")
    print(f"Ukuran              : {df.shape[0]} baris x {df.shape[1]} kolom")
    return df


# ---------------------------------------------------------------------------
# 2. TRANSFORM - Inspeksi
# ---------------------------------------------------------------------------
def inspect_data(df: pd.DataFrame) -> None:
    """Menampilkan kondisi awal dataset ke terminal."""
    print_section("2. DATA INSPECTION")

    print("\n[2.1] Lima baris pertama")
    print(df.head())

    print(f"\n[2.2] Jumlah baris dan kolom: {df.shape[0]} baris, {df.shape[1]} kolom")

    print("\n[2.3] Nama kolom")
    print(list(df.columns))

    print("\n[2.4] Tipe data per kolom")
    print(df.dtypes.to_string())

    print("\n[2.5] Missing values per kolom")
    missing = df.isna().sum()
    missing = missing[missing > 0]
    print(missing.to_string() if not missing.empty else "Tidak ada missing values")
    print(f"Total missing values: {int(missing.sum())}")

    n_dup = int(df.duplicated().sum())
    print(f"\n[2.6] Jumlah duplicate records: {n_dup}")
    if n_dup:
        print(df[df.duplicated(keep=False)].iloc[:, :8].to_string())

    print("\n[2.7] Nilai unik kolom kategorikal")
    for col in CATEGORICAL_COLS:
        values = sorted(df[col].dropna().unique().tolist())
        n_missing = int(df[col].isna().sum())
        print(f"- {col} ({len(values)} unik, {n_missing} kosong): {values}")

    print(f"\n[2.8] Pola format {DATE_COL} (tipe saat ini: {df[DATE_COL].dtype})")
    print(df[DATE_COL].map(date_pattern).value_counts().to_string())

    print("\n[2.9] Konsistensi fuel-system vs jenis bahan bakar (diesel)")
    print(pd.crosstab(df["fuel-system"], df["diesel"].map({0: "gas", 1: "diesel"})).to_string())

    print("\n[2.10] Statistik deskriptif kolom numerik")
    print(df.describe().T[["count", "mean", "min", "max"]].to_string())


# ---------------------------------------------------------------------------
# 3. TRANSFORM - Cleaning
# ---------------------------------------------------------------------------
def parse_mixed_date(value):
    """
    Mengubah tanggal dengan berbagai format menjadi Timestamp.
    Format ditentukan dari polanya (tidak ditebak) agar tidak salah membaca hari/bulan:
        2025-01-05  -> %Y-%m-%d
        02/01/2025  -> %d/%m/%Y
        01-03-2025  -> %m-%d-%Y   (terbukti dari nilai seperti 02-16-2025)
        04-Jan-2025 -> %d-%b-%Y
    """
    if pd.isna(value):
        return pd.NaT
    value = str(value).strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        fmt = "%Y-%m-%d"
    elif re.fullmatch(r"\d{2}/\d{2}/\d{4}", value):
        fmt = "%d/%m/%Y"
    elif re.fullmatch(r"\d{2}-\d{2}-\d{4}", value):
        fmt = "%m-%d-%Y"
    elif re.fullmatch(r"\d{2}-[A-Za-z]{3}-\d{4}", value):
        fmt = "%d-%b-%Y"
    else:
        return pd.NaT
    return pd.to_datetime(value, format=fmt)


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Membersihkan dataset berdasarkan masalah yang ditemukan saat inspeksi."""
    print_section("3. DATA CLEANING")
    df = df.copy()  # DataFrame mentah tidak diubah
    rows_before = len(df)
    missing_before = df.isna().sum()
    changes: dict[str, list[str]] = {}

    def log(col: str, message: str) -> None:
        changes.setdefault(col, []).append(message)
        print(f"  - [{col}] {message}")

    # 3.1 Hapus spasi berlebih dan seragamkan huruf pada kolom kategorikal
    print("\n[3.1] Menyeragamkan penulisan kategori (strip spasi + huruf kecil)")
    for col in CATEGORICAL_COLS:
        original = df[col].copy()
        cleaned = df[col].str.strip()
        if col != "horsepower-binned":  # Low/Medium/High sudah konsisten
            cleaned = cleaned.str.lower()
        cleaned = cleaned.replace(CATEGORY_SYNONYMS.get(col, {}))
        n_changed = int((original.fillna("") != cleaned.fillna("")).sum())
        df[col] = cleaned
        if n_changed:
            log(col, f"{n_changed} nilai diseragamkan")

    # 3.2 Perbaiki tipe data tanggal (4 format berbeda -> datetime)
    print(f"\n[3.2] Mengubah {DATE_COL} dari teks ke datetime")
    df[DATE_COL] = df[DATE_COL].map(parse_mixed_date).astype("datetime64[ns]")
    log(DATE_COL, "teks berbagai format -> datetime64 (YYYY-MM-DD)")

    # 3.3 Hapus duplicate records (setelah penyeragaman agar varian penulisan ikut terdeteksi)
    print("\n[3.3] Menghapus duplicate records")
    n_dup = int(df.duplicated().sum())
    df = df.drop_duplicates().reset_index(drop=True)
    print(f"  - {n_dup} baris duplikat dihapus")

    # 3.4 Tangani nilai tidak wajar: mobil diesel tetapi fuel-system bukan 'idi'
    print("\n[3.4] Memperbaiki nilai tidak wajar fuel-system pada mobil diesel")
    diesel_systems = df.loc[df["diesel"] == 1, "fuel-system"]
    dominant = diesel_systems.mode()[0]
    bad = (df["diesel"] == 1) & (df["fuel-system"] != dominant)
    if bad.any():
        print(df.loc[bad, ["make", "fuel-system", "diesel", "compression-ratio"]].to_string())
        df.loc[bad, "fuel-system"] = dominant
        log("fuel-system", f"{int(bad.sum())} mobil diesel diubah ke '{dominant}'")

    # 3.5 Missing values
    print("\n[3.5] Menangani missing values")

    # price adalah target -> baris tanpa harga dihapus, bukan ditebak
    n_price = int(df[TARGET_COL].isna().sum())
    df = df.dropna(subset=[TARGET_COL]).reset_index(drop=True)
    print(f"  - [{TARGET_COL}] {n_price} baris tanpa harga dihapus")

    # transaction_date berurutan harian -> isi dengan interpolasi dari tanggal sebelum/sesudah
    n_date = int(df[DATE_COL].isna().sum())
    if n_date:
        epoch = pd.Timestamp("1970-01-01")
        days = (df[DATE_COL] - epoch).dt.days  # NaN untuk tanggal kosong
        df[DATE_COL] = epoch + pd.to_timedelta(days.interpolate().round(), unit="D")
        log(DATE_COL, f"{n_date} tanggal kosong diisi interpolasi")

    # make: cari mobil lain dengan spesifikasi mesin identik
    for idx in df.index[df["make"].isna()]:
        mask = (df[SPEC_COLS] == df.loc[idx, SPEC_COLS]).all(axis=1) & df["make"].notna()
        candidates = df.loc[mask, "make"].unique()
        value = candidates[0] if len(candidates) == 1 else "unknown"
        df.loc[idx, "make"] = value
        log("make", f"baris {idx} diisi '{value}' (spesifikasi identik)")

    # num-of-doors: modus pada body-style yang sama
    n_doors = int(df["num-of-doors"].isna().sum())
    if n_doors:
        mode_by_body = df.groupby("body-style")["num-of-doors"].agg(lambda s: s.mode()[0])
        df["num-of-doors"] = df["num-of-doors"].fillna(df["body-style"].map(mode_by_body))
        log("num-of-doors", f"{n_doors} nilai diisi modus per body-style")

    # stroke: median (tahan terhadap outlier)
    n_stroke = int(df["stroke"].isna().sum())
    if n_stroke:
        median = df["stroke"].median()
        df["stroke"] = df["stroke"].fillna(median)
        log("stroke", f"{n_stroke} nilai diisi median ({median})")

    # horsepower: median dalam kelompok horsepower-binned yang sama
    n_hp = int(df["horsepower"].isna().sum())
    if n_hp:
        group_median = df.groupby("horsepower-binned")["horsepower"].transform("median")
        df["horsepower"] = df["horsepower"].fillna(group_median)
        log("horsepower", f"{n_hp} nilai diisi median per horsepower-binned")

    # horsepower-binned: tentukan dari horsepower dengan batas kelas yang ada di data
    n_bin = int(df["horsepower-binned"].isna().sum())
    if n_bin:
        low_max = df.loc[df["horsepower-binned"] == "Low", "horsepower"].max()
        med_max = df.loc[df["horsepower-binned"] == "Medium", "horsepower"].max()
        derived = pd.Series(
            np.select([df["horsepower"] <= low_max, df["horsepower"] <= med_max],
                      ["Low", "Medium"], default="High"),
            index=df.index,
        )
        df["horsepower-binned"] = df["horsepower-binned"].fillna(derived)
        log("horsepower-binned",
            f"{n_bin} nilai ditentukan dari horsepower (Low<={low_max:g}, Medium<={med_max:g}, High>{med_max:g})")

    # Ringkasan
    missing_after = df.isna().sum()
    print("\n[3.6] Ringkasan cleaning")
    print(f"  Jumlah baris       : {rows_before} -> {len(df)}")
    print(f"  Duplikat dihapus   : {n_dup}")
    print(f"  Baris tanpa price  : {n_price} dihapus")
    print(f"  Total missing      : {int(missing_before.sum())} -> {int(missing_after.sum())}")
    summary = pd.DataFrame({"sebelum": missing_before, "sesudah": missing_after})
    print(summary[summary["sebelum"] > 0].to_string())
    print(f"  Kolom yang berubah : {list(changes)}")
    return df


# ---------------------------------------------------------------------------
# 4. TRANSFORM - Transformasi
# ---------------------------------------------------------------------------
def min_max_scale(series: pd.Series) -> pd.Series:
    """Min-Max Scaling ke rentang [0, 1]: (x - min) / (max - min)."""
    min_val, max_val = series.min(), series.max()
    if max_val == min_val:
        return pd.Series(0.0, index=series.index)
    return (series - min_val) / (max_val - min_val)


def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """Normalisasi kolom numerik dan encoding kolom kategorikal."""
    print_section("4. DATA TRANSFORMATION")
    df = df.copy()
    sample = df.index[:5]

    # 4.1 Min-Max Scaling -> kolom baru <nama>-scaled (kolom asli dipertahankan)
    print("\n[4.1] Min-Max Scaling")
    for col in MINMAX_COLS:
        new_col = f"{col}-scaled"
        df[new_col] = min_max_scale(df[col])
        print(f"  - {col} (min={df[col].min():g}, max={df[col].max():g}) -> {new_col}")
    scaled_cols = [c for col in MINMAX_COLS for c in (col, f"{col}-scaled")]
    print(df.loc[sample, scaled_cols].round(4).to_string())

    # 4.2 One-Hot Encoding (kategori nominal, tanpa urutan)
    print("\n[4.2] One-Hot Encoding")
    for col in ONEHOT_COLS:
        dummies = pd.get_dummies(df[col], prefix=col, prefix_sep="-", dtype=int)
        df = pd.concat([df, dummies], axis=1)
        print(f"  - {col} -> {list(dummies.columns)}")
        print(pd.concat([df.loc[sample, [col]], dummies.loc[sample]], axis=1).to_string())

    # 4.3 Ordinal Encoding (kategori yang punya urutan/angka alami)
    print("\n[4.3] Ordinal Encoding")
    for col, mapping in ORDINAL_MAPS.items():
        new_col = f"{col}-encoded"
        df[new_col] = df[col].map(mapping)
        unmapped = df.loc[df[new_col].isna(), col].unique()
        if len(unmapped):
            raise ValueError(f"Nilai {col} tanpa mapping: {unmapped}")
        df[new_col] = df[new_col].astype(int)
        print(f"  - {col} -> {new_col}: {mapping}")
    ord_cols = [c for col in ORDINAL_MAPS for c in (col, f"{col}-encoded")]
    print(df.loc[sample, ord_cols].to_string())

    return df


# ---------------------------------------------------------------------------
# 5. LOAD
# ---------------------------------------------------------------------------
def save_data(df: pd.DataFrame, path: Path) -> None:
    """Menyimpan dataset hasil olahan sebagai file baru."""
    print_section("5. SAVE DATA (Load)")
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, date_format="%Y-%m-%d")
    print(f"Processed dataset disimpan ke : {path}")
    print(f"Ukuran akhir                  : {df.shape[0]} baris x {df.shape[1]} kolom")
    print(f"Sisa missing values           : {int(df.isna().sum().sum())}")


# ---------------------------------------------------------------------------
# PIPELINE
# ---------------------------------------------------------------------------
def run_pipeline(raw_path: Path = RAW_PATH, processed_path: Path = PROCESSED_PATH) -> pd.DataFrame:
    """Menjalankan seluruh pipeline ETL dari awal sampai akhir."""
    df_raw = load_data(raw_path)
    inspect_data(df_raw)
    df_clean = clean_data(df_raw)
    df_final = transform_data(df_clean)
    save_data(df_final, processed_path)
    print_section("PIPELINE SELESAI")
    return df_final


if __name__ == "__main__":
    run_pipeline()
