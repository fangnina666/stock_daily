import pandas as pd
import os
import re
import pandas as pd
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
REPORT_DIR = os.path.join(BASE_DIR, "reports")

def load_industry_map(filepath="industry_map.csv"):
    """
    載入產業對照表 (CSV or Excel)
    CSV 欄位需包含：股票代號,股票名稱,產業
    """
    if filepath.endswith(".csv"):
        df = pd.read_csv(filepath, dtype={"股票代號": str})
    else:
        df = pd.read_excel(filepath, dtype={"股票代號": str})
    return dict(zip(df["股票代號"], df["產業"]))

# ============================================================
# 找最近兩日的檔案
# ============================================================
def get_latest_two_files(folder="reports"):
    """
    從指定資料夾中，挑選出最近兩個日期的檔案
    檔名格式需包含: 券商分點買賣超明細_YYYYMMDD.xlsx
    """
    pattern = r"券商分點買賣超明細_(\d{8})\.xlsx"
    files = []

    for f in os.listdir(folder):
        match = re.match(pattern, f)
        if match:
            date_str = match.group(1)
            files.append((date_str, os.path.join(folder, f)))

    if not files or len(files) < 2:
        raise ValueError("資料夾內不足兩個符合格式的檔案")

    # 依日期排序，取最後兩個
    files_sorted = sorted(files, key=lambda x: x[0])
    latest_two = files_sorted[-2:]

    return latest_two  # [(日期字串, 路徑), ...]

# ============================================================
# 主分析流程
# ============================================================
def df_to_markdown_table(df, day1_label, day2_label):
    table = "| 股票代號 | 股票名稱 | 券商名稱 | {} 淨買超 | {} 淨買超 | Δ 淨買超 | 是否顯著異常 |\n".format(day1_label, day2_label)
    table += "|----------|----------|----------|-----------|-----------|-----------|--------------|\n"
    for _, row in df.iterrows():
        abnormal_flag = "✅" if row["異常"] else "⚠️"
        table += f"| {row['股票代號']} | {row['股票名稱']} | {row['子券商名稱']} | {int(row[f'淨買超_{day1_label}'])} | {int(row[f'淨買超_{day2_label}'])} | {int(row['Δ淨買超'])} | {abnormal_flag} |\n"
    return table


def analyze_two_day_chip_flow(file_day1, file_day2, industry_map=None,
                              day1_label="Day1", day2_label="Day2", 
                              top_n=3, concentration_threshold=0.6,
                              min_delta=50, min_volume=200, min_broker_volume=50,
                              output_path=None, output_format="md",
                              return_concentrated_only=False):
    """
    分析兩日籌碼流向 + 當日集中度 + 產業標籤 + 報告輸出
    - return_concentrated_only=True 時，回傳的 abnormal_df 僅保留「Day2 高度集中(占比>=threshold)」的券商列。
    """

    # === 載入兩日檔案 ===
    df1 = pd.read_excel(file_day1, dtype={"股票代號": str})
    df2 = pd.read_excel(file_day2, dtype={"股票代號": str})

    df1 = df1.drop_duplicates(subset=["資料日期","股票代號", "股票名稱", "子券商名稱", "買入張數", "賣出張數"])
    df2 = df2.drop_duplicates(subset=["資料日期","股票代號", "股票名稱", "子券商名稱", "買入張數", "賣出張數"])

    # 計算淨買超
    df1["淨買超"] = df1["買入張數"] - df1["賣出張數"]
    df2["淨買超"] = df2["買入張數"] - df2["賣出張數"]

    # 合併兩日
    merged = pd.merge(
        df1[["股票代號", "股票名稱", "子券商名稱", "淨買超"]].rename(columns={"淨買超": f"淨買超_{day1_label}"}),
        df2[["股票代號", "股票名稱", "子券商名稱", "淨買超"]].rename(columns={"淨買超": f"淨買超_{day2_label}"}),
        on=["股票代號", "股票名稱", "子券商名稱"],
        how="outer"
    ).fillna(0)

    # Δ淨買超
    merged["Δ淨買超"] = merged[f"淨買超_{day2_label}"] - merged[f"淨買超_{day1_label}"]

    # 計算 Day2 平均 & 標準差
    stats_day2 = merged.groupby("股票代號")[f"淨買超_{day2_label}"].agg(["mean", "std"]).reset_index()
    merged = merged.merge(stats_day2, on="股票代號", how="left")

    # ⚠️ 嚴格版異常條件
    merged["異常"] = (
        (merged["Δ淨買超"] > min_delta) &
        (merged[f"淨買超_{day2_label}"] > min_volume) &
        (merged[f"淨買超_{day2_label}"] > merged["mean"] + 2 * merged["std"])
    )

    # 過濾 ETF (00 開頭)
    abnormal_df = merged[(merged["異常"]) & (~merged["股票代號"].astype(str).str.startswith("00"))]

    # === Day2 籌碼結構（改：用「正向買方占比」避免正負互抵造成假高占比）===
    agg_day2 = df2.groupby(["股票代號", "股票名稱", "子券商名稱"])["淨買超"].sum().reset_index()
    
    # 過濾掉 ETF & 小於 min_broker_volume 的券商
    agg_day2 = agg_day2[
        (~agg_day2["股票代號"].astype(str).str.startswith("00")) &
        (agg_day2["淨買超"].abs() >= min_broker_volume)
    ].copy()
    
    # ✅ 只看「正向淨買超」做占比（把賣超視為 0，避免總淨買超=小數字被放大）
    agg_day2["正向淨買超"] = agg_day2["淨買超"].clip(lower=0)
    
    # 分母：該股票 Day2 所有券商「正向淨買超」加總（買方總量）
    stock_buy_total = (
        agg_day2.groupby("股票代號")["正向淨買超"]
                .sum()
                .reset_index()
                .rename(columns={"正向淨買超": "買方總量"})
    )
    
    merged_day2 = agg_day2.merge(stock_buy_total, on="股票代號", how="left")
    
    # ✅ 過濾：買方總量太小的股票不看（用 min_volume 當門檻）
    merged_day2 = merged_day2[merged_day2["買方總量"] >= min_volume].copy()
    
    # 占比：該券商的正向淨買超 / 買方總量
    merged_day2["占比"] = merged_day2["正向淨買超"] / merged_day2["買方總量"]
    
    # top_n 主力券商：用「正向淨買超」排序更直觀
    flow_df = (
        merged_day2.sort_values(["股票代號", "正向淨買超"], ascending=[True, False])
                  .groupby("股票代號")
                  .head(top_n)
    )

    # === 濃縮：只保留 Day2 高度集中的券商（g2 的聯集）===
    g2_all = flow_df[flow_df["占比"] >= concentration_threshold][
        ["股票代號","股票名稱","子券商名稱"]
    ].drop_duplicates()

    abnormal_df_concentrated = abnormal_df.merge(
        g2_all, on=["股票代號","股票名稱","子券商名稱"], how="inner"
    )

    # === 生成 Markdown 報告 ===
    report_lines = [f"# {day2_label} 異常籌碼分析報告", ""]
    industry_summary = []

    # 注意：以下報告段落中的 g2 改為使用參數 concentration_threshold
    # 並且若 return_concentrated_only=True，可只以 abnormal_df_concentrated 作為迭代來源
    source_abn = abnormal_df_concentrated if return_concentrated_only else abnormal_df

    for stock, g in source_abn.groupby("股票代號"):
        stock_name = g["股票名稱"].iloc[0]
        industry = industry_map.get(str(stock), "未分類") if industry_map else "未分類"
        industry_summary.append(industry)

        report_lines.append(f"## {stock} {stock_name} ({industry})")

        for _, row in g.sort_values("Δ淨買超", ascending=False).iterrows():
            report_lines.append(
                f"- {row['子券商名稱']}：{day1_label} 淨買超 {int(row[f'淨買超_{day1_label}'])} 張 → "
                f"{day2_label} 淨買超 {int(row[f'淨買超_{day2_label}'])} 張，Δ {int(row['Δ淨買超'])} 張"
            )

        report_lines.append(f"### 當日籌碼主力 (僅顯示占比 ≥ {concentration_threshold:.0%})")
        g2 = flow_df[(flow_df["股票代號"] == stock) & (flow_df["占比"] >= concentration_threshold)]
        for _, row in g2.iterrows():
            report_lines.append(f"- {row['子券商名稱']}: {int(row['淨買超'])} 張，占比 {row['占比']:.1%}")

        if not g2.empty:
            report_lines.append("👉 籌碼高度集中，顯示主力券商鎖碼\n")
        else:
            report_lines.append("👉 無單一券商占比達門檻，籌碼未明顯集中\n")

    # === 總結產業 ===
    if industry_summary:
        industry_counts = pd.Series(industry_summary).value_counts()
        summary_text = "、".join([f"{k} ({v} 檔)" for k, v in industry_counts.items()])
        report_lines.append("### 總結")
        report_lines.append(f"今日異常籌碼主要集中在：{summary_text}。")
    else:
        report_lines.append("### 總結")
        report_lines.append("今日未觀察到顯著異常籌碼。")

    result_text = "\n".join(report_lines)

    # === 輸出報告檔案 ===
    if output_path:
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(result_text)

    # 依參數決定回傳哪一個 abnormal_df
    abnormal_out = abnormal_df_concentrated if return_concentrated_only else abnormal_df
    return result_text, abnormal_out, flow_df

# ============================================================
# 產生 LLM Prompt
# ============================================================
def generate_llm_prompt(day1_label, day2_label, abnormal_df):
    base_prompt = f"""
你是一名台股籌碼分析助理。
以下提供 {day1_label} 與 {day2_label} 的券商分點進出比較結果。
請依照以下任務，輸出一份「異常籌碼分析報告」：

【任務要求】
1. 條列出異常的股票與券商，需包含：
   - 股票代號、股票名稱
   - 券商名稱
   - {day1_label} 淨買超、{day2_label} 淨買超、Δ淨買超
   - 是否顯著異常
2. 分析該股票在 {day2_label} 的籌碼集中度（僅依據異常券商），判斷是否有主力鎖碼。
3. 過濾掉 00 開頭的 ETF / 指數商品。
4. 總結（3–5 句話）：指出異常券商集中在哪些股票或產業族群，並推測可能的市場意圖。

【異常數據】
"""

    for _, row in abnormal_df.iterrows():
        base_prompt += f"""
- {row['股票代號']} {row['股票名稱']} / {row['子券商名稱']}
  {day1_label}: {int(row[f'淨買超_{day1_label}'])} 張
  {day2_label}: {int(row[f'淨買超_{day2_label}'])} 張
  Δ: {int(row['Δ淨買超'])} 張
"""
    return base_prompt

# ============================================================
# 主程式
# ============================================================
if __name__ == "__main__":
    # 找最近兩日檔案
    (day1_str, file_day1), (day2_str, file_day2) = get_latest_two_files(REPORT_DIR)

    print(f"分析最近兩日檔案：{day1_str}, {day2_str}")

    # 載入產業對照表
    industry_map = load_industry_map("industry_map.csv")

    # 執行分析
    report, abnormal_df, flow_df = analyze_two_day_chip_flow(
        file_day1, file_day2,
        industry_map=industry_map,
        day1_label=day1_str,
        day2_label=day2_str,
        top_n=3,
        concentration_threshold=0.6,
        min_delta=50, min_volume=200, min_broker_volume=50,
        output_path=f"reports/abnormal_report_{day2_str}.md"
    )

    print(report)
    # 產生 prompt.txt
    
        # 僅取前 5 檔異常股票
    abnormal_top = abnormal_df.groupby("股票代號").head(1).head(20)
    flow_top = flow_df[flow_df["股票代號"].isin(abnormal_top["股票代號"])]
    
    # 產生精簡版 Prompt
    prompt_text = generate_llm_prompt(day1_str, day2_str, abnormal_top)
    with open(f"reports/llm_prompt_{day2_str}.txt", "w", encoding="utf-8") as f:
        f.write(prompt_text)
     
        '''
    prompt_text = generate_llm_prompt(day1_str, day2_str, abnormal_df)
    with open(f"reports/llm_prompt_{day2_str}.txt", "w", encoding="utf-8") as f:
       f.write(prompt_text)
       '''

    print("已生成報告 abnormal_report.md 與 LLM Prompt.txt，可直接丟給 ChatGPT/Gemini")

    markdown_table = df_to_markdown_table(abnormal_df, day1_str, day2_str)
    with open(f"reports/abnormal_table_{day2_str}.md", "w", encoding="utf-8") as f:
        f.write(markdown_table)
        
    abnormal_df.to_excel(f"reports/abnormal_table_{day2_str}.xlsx", index=False)
    
    print(markdown_table)   # 在 console 印出來

