
import io
import re
import numpy as np
import pandas as pd
import streamlit as st

st.set_page_config(page_title="Mục 18 - GTCG Toolkit", layout="wide")

st.title("📘 Mục 18 - GTCG: Xử lý & Tổng hợp")
st.caption("Giữ nguyên kiểu dữ liệu khi import (đặc biệt ACC_NO), xuất Excel 2 sheet.")

# =============== Helpers ===============

def read_excel_keep_text(uploaded_file, force_acc_no_text=True):
    """Đọc Excel và giữ nguyên dữ liệu. Nếu force_acc_no_text=True thì ACC_NO luôn dạng text."""
    if uploaded_file is None:
        return None
    try:
        if force_acc_no_text:
            df = pd.read_excel(uploaded_file, dtype={'ACC_NO': str})
            # Chuẩn hoá ACC_NO để tránh NaN -> 'nan' hoặc float -> '123.0'
            if 'ACC_NO' in df.columns:
                df['ACC_NO'] = df['ACC_NO'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
        else:
            df = pd.read_excel(uploaded_file)  # để pandas tự suy đoán dtype
        return df
    except Exception as e:
        st.error(f"Lỗi đọc Excel: {e}")
        return None

def ensure_datetime(series):
    try:
        return pd.to_datetime(series, errors='coerce')
    except Exception:
        return pd.to_datetime(series.astype(str), errors='coerce')

# =============== Phần 1: Tiêu chí in hỏng / hết dòng (TTK) ===============

def process_ttk(df):
    """Triển khai logic mục 2.3.2 theo đoạn code của bạn."""
    df = df.copy()

    # Định dạng cột
    if 'ACC_NO' in df.columns:
        df['ACC_NO'] = df['ACC_NO'].astype(str).str.strip()

    if 'INVT_TRAN_DATE' in df.columns:
        df['INVT_TRAN_DATE'] = ensure_datetime(df['INVT_TRAN_DATE'])

    # Sắp xếp theo INVT_SRL_NUM nếu có
    if 'INVT_SRL_NUM' in df.columns:
        df.sort_values(by='INVT_SRL_NUM', ascending=True, inplace=True)
        df.reset_index(drop=True, inplace=True)

    # (1) Số lần in hỏng
    # Điều kiện: PASSBOOK_STATUS == 'F' và INVT_LOCN_CODE_TO == 'IS'
    failure_mask = (
        df.get('PASSBOOK_STATUS', pd.Series(False, index=df.index)).eq('F') &
        df.get('INVT_LOCN_CODE_TO', pd.Series('', index=df.index)).eq('IS')
    )
    total_failure_counts = df.loc[failure_mask, 'ACC_NO'].map(df.loc[failure_mask, 'ACC_NO'].value_counts())
    df['Số lần in hỏng'] = total_failure_counts.fillna(0).astype(int)

    # (2) TTK in hỏng nhiều lần trong 01 ngày
    df['TTK in hỏng nhiều lần trong 01 ngày'] = ''
    if df['INVT_TRAN_DATE'].notna().any():
        daily_failure_counts = df[failure_mask].groupby(['ACC_NO', df['INVT_TRAN_DATE'].dt.date]).transform('size')
        df['daily_failures'] = daily_failure_counts
        df['TTK in hỏng nhiều lần trong 01 ngày'] = np.where(df['daily_failures'] >= 2, 'X', '')
        df.drop(columns=['daily_failures'], inplace=True, errors='ignore')

    # Chuẩn bị cột ngày
    df['INVT_TRAN_DATE'] = ensure_datetime(df['INVT_TRAN_DATE'])
    df['TRAN_DATE_ONLY'] = df['INVT_TRAN_DATE'].dt.date

    # (3) Số lần in hết dòng
    hetdong_mask = (
        df.get('PASSBOOK_STATUS', pd.Series('', index=df.index)).eq('U') &
        df.get('INVT_LOCN_CODE_TO', pd.Series('', index=df.index)).eq('IS')
    )
    df['Số lần in hết dòng'] = df.loc[hetdong_mask, 'ACC_NO'].map(
        df.loc[hetdong_mask, 'ACC_NO'].value_counts()
    ).fillna(0).astype(int)

    # (4) TTK in hết dòng nhiều lần trong 01 ngày
    df['TTK in hết dòng nhiều lần trong 01 ngày'] = ''
    try:
        df['daily_het_dong'] = df[hetdong_mask].groupby(
            ['ACC_NO', 'TRAN_DATE_ONLY']
        )['ACC_NO'].transform('count')
        df['TTK in hết dòng nhiều lần trong 01 ngày'] = np.where(df['daily_het_dong'] >= 2, 'X', '')
    except Exception:
        pass
    df.drop(columns=['daily_het_dong'], inplace=True, errors='ignore')

    # (5) TTK vừa in hỏng vừa in hết dòng trong 01 ngày
    df_temp = df.groupby(['ACC_NO', 'TRAN_DATE_ONLY']).agg({
        'Số lần in hỏng': 'sum',
        'Số lần in hết dòng': 'sum'
    }).reset_index()
    df_temp['TTK vừa in hỏng vừa in hết dòng trong 01 ngày'] = np.where(
        (df_temp['Số lần in hỏng'] > 0) & (df_temp['Số lần in hết dòng'] > 0), 'X', ''
    )
    df = pd.merge(
        df,
        df_temp[['ACC_NO', 'TRAN_DATE_ONLY', 'TTK vừa in hỏng vừa in hết dòng trong 01 ngày']],
        on=['ACC_NO', 'TRAN_DATE_ONLY'],
        how='left'
    )

    # Định dạng lại ngày (mm/dd/yyyy) theo code gốc
    df['INVT_TRAN_DATE'] = pd.to_datetime(df['INVT_TRAN_DATE'], errors='coerce').dt.strftime('%m/%d/%Y')

    # Xoá cột phụ nếu không cần
    df.drop(columns=['TRAN_DATE_ONLY'], inplace=True, errors='ignore')
    return df

# =============== Phần 2: Phát hành / In hỏng theo TBL (Mục 18_2205_GTCG1) ===============

def extract_tbl(series, prefix_tbl):
    pattern = rf'({re.escape(prefix_tbl)}[^\s/]*)'
    return series.astype(str).str.extract(pattern)[0]

def process_phoi(df, sol_kiem_toan):
    """Triển khai các tiêu chí (1)-(6) như code bạn."""
    df = df.copy()

    prefix_tbl = f"{sol_kiem_toan}G"
    df['TBL'] = extract_tbl(df['INVT_XFER_PARTICULAR'].astype(str), prefix_tbl)

    # (1) Phôi hỏng không gắn số
    df['(1) Phôi hỏng không gắn số'] = (
        (df.get('INVT_LOCN_CODE_TO', pd.Series('', index=df.index)) == 'IS') &
        ~df['INVT_XFER_PARTICULAR'].astype(str).str.contains(prefix_tbl, na=False)
    ).map({True: 'X', False: ''})

    # (2) Số lần phát hành
    mask_ph = (
        (df.get('INVT_LOCN_CODE_TO', pd.Series('', index=df.index)) == 'IS') &
        (df['TBL'].notna())
    )
    df_ph = df[mask_ph]
    ph_counts = df_ph['TBL'].value_counts().to_dict()
    df['(2) Số lần phát hành'] = df['TBL'].map(ph_counts).fillna(0).astype(int)

    # (3) PH nhiều lần trong 1 ngày
    df['(3) PH nhiều lần trong 1 ngày'] = ''
    df['INVT_TRAN_DATE_ONLY'] = ensure_datetime(df['INVT_TRAN_DATE']).dt.date
    mask_ph_2plus = df['(2) Số lần phát hành'] >= 2
    try:
        df.loc[mask_ph_2plus, '(3) PH nhiều lần trong 1 ngày'] = (
            df[mask_ph_2plus]
            .groupby(['TBL', 'INVT_TRAN_DATE_ONLY'], group_keys=False)
            .apply(lambda g: pd.Series(['X'] * len(g), index=g.index))
        )
    except Exception:
        pass

    # (4) Số lần in hỏng
    mask_hong = (
        df.get('INVT_LOCN_CODE_TO', pd.Series('', index=df.index)).isin(['FAIL', 'FAIL PRINT']) &
        df['TBL'].notna()
    )
    df_hong = df[mask_hong]
    hong_counts = df_hong['TBL'].value_counts().to_dict()
    df['(4) Số lần in hỏng'] = df['TBL'].map(hong_counts).fillna(0).astype(int)

    # (5) In hỏng nhiều lần trong 1 ngày
    df['(5) In hỏng nhiều lần trong 1 ngày'] = ''
    mask_hong_2plus = df['(4) Số lần in hỏng'] >= 2
    try:
        df.loc[mask_hong_2plus, '(5) In hỏng nhiều lần trong 1 ngày'] = (
            df[mask_hong_2plus]
            .groupby(['TBL', 'INVT_TRAN_DATE_ONLY'], group_keys=False)
            .apply(lambda g: pd.Series(['X'] * len(g), index=g.index))
        )
    except Exception:
        pass

    # (6) PH nhiều lần + có in hỏng
    df['(6) PH nhiều lần + có in hỏng'] = df.apply(
        lambda row: 'X' if (
            row['(3) PH nhiều lần trong 1 ngày'] == 'X' and (
                row['(1) Phôi hỏng không gắn số'] == 'X' or row['(4) Số lần in hỏng'] >= 1
            )
        ) else '',
        axis=1
    )

    # Xoá cột phụ
    df.drop(columns=['INVT_TRAN_DATE_ONLY', 'TBL'], inplace=True, errors='ignore')
    return df

# =============== UI ===============

st.subheader("1) Nhập dữ liệu")

c1, c2 = st.columns(2)
with c1:
    st.markdown("**File TTK (ví dụ: Muc18_1403_GTCG.xlsx)**")
    file_ttk = st.file_uploader("Chọn 1 file TTK", type=['xlsx'], key="ttk")
    ttk_force_text = st.checkbox("Luôn đọc ACC_NO dạng text (khuyến nghị)", value=True)
with c2:
    st.markdown("**File PHÔI (ví dụ: Muc18_2205_GTCG1_*.xlsx)**")
    files_phoi = st.file_uploader("Chọn 1 hoặc nhiều file PHÔI", type=['xlsx'], accept_multiple_files=True, key="phoi")
    sol_kiem_toan = st.text_input("Mã SOL kiểm toán (ví dụ 2205)", value="2205")

run = st.button("▶️ Chạy xử lý")

if run:
    out_buffers = {}

    # ---- Phần 1: TTK ----
    df_ttk = read_excel_keep_text(file_ttk, force_acc_no_text=ttk_force_text) if file_ttk else None
    if df_ttk is not None:
        df_ttk_out = process_ttk(df_ttk)
        st.success("✔️ Hoàn thành phần TTK (2.3.2)")
        st.dataframe(df_ttk_out.head(200))

        bio = io.BytesIO()
        with pd.ExcelWriter(bio, engine='openpyxl') as writer:
            df_ttk_out.to_excel(writer, sheet_name='tieu chi 1,2', index=False)
        out_buffers['TTK_only.xlsx'] = bio.getvalue()
    else:
        st.info("Bỏ qua phần TTK vì chưa chọn file.")

    # ---- Phần 2: PHÔI ----
    if files_phoi:
        df_list = []
        for f in files_phoi:
            df_i = pd.read_excel(f, dtype=None)  # giữ nguyên dtype gốc do Excel cung cấp
            # Đảm bảo cột ngày có thể xử lý
            if 'INVT_TRAN_DATE' in df_i.columns:
                df_i['INVT_TRAN_DATE'] = ensure_datetime(df_i['INVT_TRAN_DATE'])
            df_list.append(df_i)
        df_phoi_raw = pd.concat(df_list, ignore_index=True) if len(df_list) > 1 else df_list[0]

        df_phoi_out = process_phoi(df_phoi_raw, sol_kiem_toan=sol_kiem_toan.strip())
        st.success("✔️ Hoàn thành phần PHÔI (1)-(6)")
        st.dataframe(df_phoi_out.head(200))

        bio2 = io.BytesIO()
        with pd.ExcelWriter(bio2, engine='openpyxl') as writer:
            # Nếu có cả TTK lẫn PHÔI: theo yêu cầu xuất 2 sheet
            if 'TTK_only.xlsx' in out_buffers:
                # Sheet 1: TTK
                pd.read_excel(io.BytesIO(out_buffers['TTK_only.xlsx'])).to_excel(writer, sheet_name='tieu chi 1,2', index=False)
                # Sheet 2: PHÔI
                df_phoi_out.to_excel(writer, sheet_name='tieu chi 3', index=False)
            else:
                # Chỉ PHÔI
                df_phoi_out.to_excel(writer, sheet_name='tieu chi 3', index=False)
        out_buffers['Phoi_the_output.xlsx'] = bio2.getvalue()
    else:
        st.info("Bỏ qua phần PHÔI vì chưa chọn file.")

    # ---- Gộp xuất một file chung nếu có đủ ----
    if 'TTK_only.xlsx' in out_buffers and 'Phoi_the_output.xlsx' in out_buffers:
        st.download_button("⬇️ Tải Excel (2 sheet: TTK & PHÔI)",
                           data=out_buffers['Phoi_the_output.xlsx'],
                           file_name="Muc18_TTK_PHOI.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    # Xuất riêng từng phần nếu có
    if 'TTK_only.xlsx' in out_buffers:
        st.download_button("⬇️ Tải Excel TTK (tieu chi 1,2)",
                           data=out_buffers['TTK_only.xlsx'],
                           file_name="TTK_only.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    if 'Phoi_the_output.xlsx' in out_buffers:
        st.download_button("⬇️ Tải Excel PHÔI (tieu chi 3)",
                           data=out_buffers['Phoi_the_output.xlsx'],
                           file_name="Phoi_the_output.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

st.divider()
with st.expander("📦 Yêu cầu môi trường / Gợi ý chạy"):
    st.markdown("""
    **Cài đặt:**
    ```bash
    pip install streamlit pandas numpy openpyxl
    ```

    **Chạy ứng dụng:**
    ```bash
    streamlit run app.py
    ```

    **Ghi chú giữ nguyên kiểu dữ liệu:**
    - Mặc định phần PHÔI dùng `dtype=None` để giữ kiểu pandas suy luận từ Excel.
    - Riêng `ACC_NO` (TTK) thường cần giữ **text** để không mất số 0 đầu. Bật checkbox *"Luôn đọc ACC_NO dạng text"*.
    - Các cột ngày sẽ được chuyển sang `datetime` nội bộ để tính, nhưng xuất ra Excel vẫn hiển thị chuẩn.
    """)
