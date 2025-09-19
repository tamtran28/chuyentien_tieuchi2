import io
import re
import numpy as np
import pandas as pd
import streamlit as st

st.set_page_config(page_title="Mục 18 – GTCG", layout="wide")
st.title("📘 Mục 18 – GTCG (Streamlit)")
st.caption("Tải 2 file Excel (.xlsx) rồi bấm **Xử lý**. Kết quả sẽ có 2 sheet: tieu chi 1,2 & tieu chi 3.")

# Bắt buộc openpyxl để đọc .xlsx
try:
    import openpyxl  # noqa
except Exception:
    st.error("Thiếu thư viện **openpyxl**. Cài: `pip install openpyxl`")
    st.stop()

# ------------------------- Helpers -------------------------
def read_xlsx(uploaded_file, label):
    if not uploaded_file:
        st.error(f"Thiếu file: {label}")
        st.stop()
    if not uploaded_file.name.lower().endswith(".xlsx"):
        st.error(f"{label} phải là .xlsx")
        st.stop()
    try:
        return pd.read_excel(uploaded_file, engine="openpyxl")
    except Exception as e:
        st.error(f"Lỗi đọc {label}: {e}")
        st.stop()

def process_ttk(df_raw: pd.DataFrame) -> pd.DataFrame:
    """Sheet 1: Tiêu chí 1,2 (dựa trên file GTCG 1)."""
    df = df_raw.copy()

    # Đảm bảo ACC_NO là text
    if "ACC_NO" in df.columns:
        df["ACC_NO"] = df["ACC_NO"].astype(str)

    # Chuẩn hóa ngày
    df["INVT_TRAN_DATE"] = pd.to_datetime(df["INVT_TRAN_DATE"], errors="coerce")
    df.sort_values(["INVT_SRL_NUM"], ascending=True, inplace=True, na_position="last")
    df.reset_index(drop=True, inplace=True)

    # (1) Số lần in hỏng: PASSBOOK_STATUS='F' & INVT_LOCN_CODE_TO='IS'
    failure_mask = (df["PASSBOOK_STATUS"] == "F") & (df["INVT_LOCN_CODE_TO"] == "IS")
    fail_counts = (
        df.loc[failure_mask, "ACC_NO"].value_counts()
        .rename_axis("ACC_NO")
        .reset_index(name="Số lần in hỏng")
    )
    df = df.merge(fail_counts, on="ACC_NO", how="left")
    df["Số lần in hỏng"] = df["Số lần in hỏng"].fillna(0).astype(int)

    # (2) TTK in hỏng nhiều lần trong 01 ngày (>=2 trong cùng ngày)
    df["DATE_ONLY"] = df["INVT_TRAN_DATE"].dt.date
    daily_fail = (
        df.loc[failure_mask]
        .groupby(["ACC_NO", "DATE_ONLY"])
        .size()
        .reset_index(name="daily_fail_cnt")
    )
    df = df.merge(daily_fail, on=["ACC_NO", "DATE_ONLY"], how="left")
    df["TTK in hỏng nhiều lần trong 01 ngày"] = np.where(
        df["daily_fail_cnt"].fillna(0) >= 2, "X", ""
    )
    df.drop(columns=["daily_fail_cnt"], inplace=True)

    # (3) Số lần in hết dòng: PASSBOOK_STATUS='U' & INVT_LOCN_CODE_TO='IS'
    hetdong_mask = (df["PASSBOOK_STATUS"] == "U") & (df["INVT_LOCN_CODE_TO"] == "IS")
    hetdong_counts = (
        df.loc[hetdong_mask, "ACC_NO"].value_counts()
        .rename_axis("ACC_NO")
        .reset_index(name="Số lần in hết dòng")
    )
    df = df.merge(hetdong_counts, on="ACC_NO", how="left")
    df["Số lần in hết dòng"] = df["Số lần in hết dòng"].fillna(0).astype(int)

    # (4) TTK in hết dòng nhiều lần trong 01 ngày (>=2)
    daily_het = (
        df.loc[hetdong_mask]
        .groupby(["ACC_NO", "DATE_ONLY"])
        .size()
        .reset_index(name="daily_het_cnt")
    )
    df = df.merge(daily_het, on=["ACC_NO", "DATE_ONLY"], how="left")
    df["TTK in hết dòng nhiều lần trong 01 ngày"] = np.where(
        df["daily_het_cnt"].fillna(0) >= 2, "X", ""
    )
    df.drop(columns=["daily_het_cnt"], inplace=True)

    # # (5) Vừa in hỏng vừa in hết dòng trong 01 ngày
    # mix = (
    #     df.groupby(["ACC_NO", "DATE_ONLY"])
    #     .agg(**{
    #         "sum_hong": ("Số lần in hỏng", "sum"),
    #         "sum_het": ("Số lần in hết dòng", "sum"),
    #     })
    #     .reset_index()
    # )
    # mix["TTK vừa in hỏng vừa in hết dòng trong 01 ngày"] = np.where(
    #     (mix["sum_hong"] > 0) & (mix["sum_het"] > 0), "X", ""
    # )
    # df = df.merge(
    #     mix[["ACC_NO", "DATE_ONLY", "TTK vừa in hỏng vừa in hết dòng trong 01 ngày"]],
    #     on=["ACC_NO", "DATE_ONLY"],
    #     how="left",
    # )

    # # Format ngày
    # df["INVT_TRAN_DATE"] = df["INVT_TRAN_DATE"].dt.strftime("%m/%d/%Y")
    # df.drop(columns=["DATE_ONLY"], inplace=True)

    # return df
# (5) Vừa in hỏng vừa in hết dòng trong 01 ngày

# Gộp dữ liệu in hỏng và in hết dòng theo tài khoản và ngày
mix = (
    df.groupby(['ACC_NO', 'DATE_ONLY'])
    .agg({
        'Số lần in hỏng': 'sum',
        'Số lần in hết dòng': 'sum'
    })
    .reset_index()
)

# Đánh dấu trường hợp vừa in hỏng vừa in hết dòng trong 01 ngày
mix['TTK vừa in hỏng vừa in hết dòng trong 01 ngày'] = np.where(
    (mix['Số lần in hỏng'] > 0) & (mix['Số lần in hết dòng'] > 0),
    'X', ''
)

# Merge kết quả này vào lại bảng chính
df = pd.merge(
    df,
    mix[['ACC_NO', 'DATE_ONLY', 'TTK vừa in hỏng vừa in hết dòng trong 01 ngày']],
    on=['ACC_NO', 'DATE_ONLY'],
    how='left'
)

# Xóa cột ngày phụ
df.drop(columns=['DATE_ONLY'], inplace=True)

# Nếu cần: định dạng lại cột ngày xuất cho đẹp
df['INVT_TRAN_DATE'] = df['INVT_TRAN_DATE'].dt.strftime('%m/%d/%Y')

return df

def process_phoi(df_raw: pd.DataFrame, sol_code: str) -> pd.DataFrame:
    """Sheet 2: Tiêu chí 3 (dựa trên file GTCG 2)."""
    df = df_raw.copy()

    # Tạo TBL từ INVT_XFER_PARTICULAR theo prefix {sol}G...
    prefix_tbl = f"{sol_code}G"
    pattern = rf"({re.escape(prefix_tbl)}[^\s/]*)"
    df["TBL"] = df["INVT_XFER_PARTICULAR"].astype(str).str.extract(pattern)[0]

    # (1) Phôi hỏng không gắn số:
    # INVT_LOCN_CODE_TO chứa FAIL/FAIL PRINT và INVT_XFER_PARTICULAR không chứa prefix
    df["Phôi hỏng không gắn số"] = (
        df["INVT_LOCN_CODE_TO"].astype(str).str.contains("FAIL PRINT|FAIL", na=False)
        & ~df["INVT_XFER_PARTICULAR"].astype(str).str.contains(prefix_tbl, na=False)
    ).map({True: "X", False: ""})

    # (2) Số lần phát hành: INVT_LOCN_CODE_TO='IS' và có TBL
    mask_ph = (df["INVT_LOCN_CODE_TO"] == "IS") & (df["TBL"].notna())
    ph_counts = df.loc[mask_ph, "TBL"].value_counts().to_dict()
    df["Số lần phát hành"] = df["TBL"].map(ph_counts).fillna(0).astype(int)

    # Ngày rút gọn
    df["INVT_TRAN_DATE_ONLY"] = pd.to_datetime(df["INVT_TRAN_DATE"], errors="coerce").dt.date

    # (3) PH nhiều lần trong 1 ngày: IS và (TBL, DATE) có >=2
    df["PH nhiều lần trong 1 ngày"] = ""
    df_is = df[df["INVT_LOCN_CODE_TO"] == "IS"].copy()
    count_tbl_date = (
        df_is.groupby(["TBL", "INVT_TRAN_DATE_ONLY"]).size().reset_index(name="cnt")
    )
    keys_multi = set(
        zip(
            count_tbl_date.loc[count_tbl_date["cnt"] >= 2, "TBL"],
            count_tbl_date.loc[count_tbl_date["cnt"] >= 2, "INVT_TRAN_DATE_ONLY"],
        )
    )
    df.loc[
        df.apply(lambda r: (r["INVT_LOCN_CODE_TO"] == "IS") and ((r["TBL"], r["INVT_TRAN_DATE_ONLY"]) in keys_multi), axis=1),
        "PH nhiều lần trong 1 ngày"
    ] = "X"

    # (4) Số lần in hỏng (FAIL/FAIL PRINT) theo TBL
    mask_hong = df["INVT_LOCN_CODE_TO"].isin(["FAIL", "FAIL PRINT"]) & df["TBL"].notna()
    hong_counts = df.loc[mask_hong, "TBL"].value_counts().to_dict()
    df["Số lần in hỏng"] = df["TBL"].map(hong_counts).fillna(0).astype(int)

    # (5) In hỏng nhiều lần trong 1 ngày:
    # yêu cầu: INVT_LOCN_CODE_TO = 'FAIL PRINT' & Số lần in hỏng >= 2
    df["(5) In hỏng nhiều lần trong 1 ngày"] = ""
    mask_hong_2 = (df["INVT_LOCN_CODE_TO"] == "FAIL PRINT") & (df["Số lần in hỏng"] >= 2)
    groups = (
        df.loc[mask_hong_2]
        .groupby(["TBL", "INVT_TRAN_DATE_ONLY"])
        .filter(lambda g: len(g) >= 2)
    )
    df.loc[groups.index, "(5) In hỏng nhiều lần trong 1 ngày"] = "X"

    # (6) PH nhiều lần + có in hỏng
    df["PH nhiều lần + có in hỏng"] = np.where(
        (df["Số lần phát hành"] > 1) & (df["Số lần in hỏng"] > 0), "X", ""
    )

    # Bỏ cột tạm
    df.drop(columns=["INVT_TRAN_DATE_ONLY", "TBL"], inplace=True)

    return df


# ------------------------- UI -------------------------
c1, c2 = st.columns(2)
with c1:
    file_gtcg1 = st.file_uploader("GTCG 1 (.xlsx) — ví dụ: MUC 18 GTCG 1 1201 1.xlsx", type=["xlsx"])
with c2:
    file_gtcg2 = st.file_uploader("GTCG 2 (.xlsx) — ví dụ: MUC 18 GTCG 2 1201 1.xlsx", type=["xlsx"])

sol_code = st.text_input("Nhập mã SOL kiểm toán (ví dụ: 1201)", value="1201").strip()
run = st.button("▶️ Xử lý", type="primary")

# ------------------------- RUN -------------------------
if run:
    df1 = read_xlsx(file_gtcg1, "GTCG 1")
    df2 = read_xlsx(file_gtcg2, "GTCG 2")

    # Sheet 1
    ttk = process_ttk(df1)
    # Sheet 2
    phoi = process_phoi(df2, sol_code)

    st.subheader("📄 Kết quả – Tiêu chí 1,2")
    st.dataframe(ttk.head(100), use_container_width=True)

    st.subheader("📄 Kết quả – Tiêu chí 3")
    st.dataframe(phoi.head(100), use_container_width=True)

    # Xuất Excel 2 sheet
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        ttk.to_excel(writer, sheet_name="tieu chi 1,2", index=False)
        phoi.to_excel(writer, sheet_name="tieu chi 3", index=False)

    st.download_button(
        "⬇️ Tải file kết quả (Phoi_the.xlsx)",
        data=out.getvalue(),
        file_name="Phoi_the.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
