#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# created by SilverJiang
# ---------- 导入依赖 ----------
import os
import json
import time
import getpass
import requests
from bs4 import BeautifulSoup, NavigableString

# ---------- MySQL 驱动回退：优先 mysql-connector，否则尝试 pymysql ----------
try:
    import mysql.connector as mysql_connector
    MYSQL_DRIVER = "mysql-connector"
except Exception:
    try:
        import pymysql as mysql_connector
        MYSQL_DRIVER = "pymysql"
    except Exception:
        print("未安装 MySQL 驱动。请先运行：")
        print("  python -m pip install mysql-connector-python requests")
        print("或  python -m pip install pymysql requests")
        raise

CONFIG_FILE = "config.json"
SELECTION_FILE = "./selections.json"


# ---------- 工具函数 ----------

def is_html(text: str) -> bool:
    """
简单判断是否为 HTML 文本
- 含有 <tag> 或 &nbsp; 等常见 HTML 特征
    """
    if not text:
        return False
    if "<" in text and ">" in text:
        try:
            soup = BeautifulSoup(text, "html.parser")
            return bool(soup.find())  # 如果有标签，算 HTML
        except Exception:
            return False
    return False

def load_json_if_exists(path):
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return None


def save_json(path, obj):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def quote_ident(name):
    return "`" + name.replace("`", "``") + "`"


# ---------- 数据库连接（兼容两种驱动） ----------
def get_db_connection(cfg):
    host = cfg.get("host", "localhost")
    port = cfg.get("port", 3306)
    user = cfg.get("user", "root")
    password = cfg.get("password", "")
    database = cfg.get("database")

    if MYSQL_DRIVER == "mysql-connector":
        conn = mysql_connector.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            charset="utf8mb4",
            autocommit=False,
        )
        # mysql-connector 有 conn.database 属性
        try:
            conn.database = database
        except Exception:
            pass
    else:
        # pymysql: 参数名是 db
        conn = mysql_connector.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            db=database,
            charset="utf8mb4",
        )
        # 尝试关闭自动提交（若可用）
        try:
            conn.autocommit(False)
        except Exception:
            try:
                conn.autocommit = False
            except Exception:
                pass
        # 给连接对象附加 database 属性以兼容旧代码
        try:
            conn.database = database
        except Exception:
            pass
    return conn


# ---------- schema / 表 / 列 查询 ----------
def find_target_tables(conn, schema, pattern="fa_ldcms_document"):
    cursor = conn.cursor()
    sql = """
SELECT TABLE_NAME
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = %s AND TABLE_NAME LIKE %s
ORDER BY TABLE_NAME
    """
    cursor.execute(sql, (schema, pattern + "%"))
    rows = [r[0] for r in cursor.fetchall()]
    cursor.close()
    return rows


def list_table_columns(conn, schema, table):
    cursor = conn.cursor()
    sql = """
SELECT COLUMN_NAME, DATA_TYPE, COLUMN_KEY
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
ORDER BY ORDINAL_POSITION
    """
    cursor.execute(sql, (schema, table))
    rows = cursor.fetchall()
    cursor.close()
    return rows


def detect_primary_key(conn, schema, table):
    cols = list_table_columns(conn, schema, table)
    for col, dtype, key in cols:
        if key == "PRI":
            return col
    return cols[0][0] if cols else None


def add_target_column_if_needed(conn, table, target_col, schema=None):
    # schema 如果为 None，则尝试使用 conn.database
    dbname = schema or getattr(conn, "database", None)
    cursor = conn.cursor()
    sql = """
SELECT COUNT(*)
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_NAME = %s
    """
    cursor.execute(sql, (dbname, table, target_col))
    exists = cursor.fetchone()[0] > 0
    cursor.close()
    if not exists:
        alter_sql = f"ALTER TABLE {quote_ident(table)} ADD COLUMN {quote_ident(target_col)} LONGTEXT NULL"
        cursor = conn.cursor()
        cursor.execute(alter_sql)
        conn.commit()
        cursor.close()
        print(f"已创建列 {target_col} 于表 {table}")


# ---------- HTML 文本节点提取 ----------
def extract_text_nodes(html: str):
    soup = BeautifulSoup(html, "html.parser")
    nodes = []

    def recurse(el, path=""):
        for idx, child in enumerate(el.contents):
            child_path = f"{path}/{el.name}[{idx}]" if el.name else path
            if isinstance(child, NavigableString):
                text = child.strip()
                if text:
                    nodes.append({
                        "text": text,
                        "parent_tag": el.name if el.name else "",
                        "attrs": dict(el.attrs) if el.name else {},
                        "path": child_path
                    })
            else:
                recurse(child, path=child_path)

    recurse(soup, path="")
    return nodes

# ---------- HTML 重建 ----------
def rebuild_html_from_nodes(html: str, translated_texts: list):
    soup = BeautifulSoup(html, "html.parser")
    idx = 0

    def recurse_replace(el):
        nonlocal idx
        for child in el.contents:
            if isinstance(child, NavigableString):
                if child.strip() and idx < len(translated_texts):
                    child.replace_with(translated_texts[idx])
                    idx += 1
            else:
                recurse_replace(child)

    recurse_replace(soup)
    return str(soup)


# ---------- 翻译 API 封装 ----------

class TranslatorAPI_MIC:
    def __init__(self, key, region, target_lang="de", endpoint="https://api.cognitive.microsofttranslator.com"):
        if not key or not region:
            raise ValueError("需要提供 Microsoft Translator 的 api_key 和 api_region")
        self.key = key
        self.region = region
        self.target_lang = target_lang
        self.endpoint = endpoint.rstrip("/")

    def translate_batch(self, texts, lang=None, retry=3, timeout=30):
        if not texts:
            return []

        target_lang = lang if lang else self.target_lang
        url = f"{self.endpoint}/translate?api-version=3.0&to={target_lang}&textType=html"

        headers = {
            "Ocp-Apim-Subscription-Key": self.key,
            "Ocp-Apim-Subscription-Region": self.region,
            "Content-Type": "application/json",
        }
        body = [{"Text": t} for t in texts]

        attempt = 0
        while attempt < retry:
            try:
                r = requests.post(url, headers=headers, json=body, timeout=timeout)
                if r.status_code == 200:
                    data = r.json()
                    out = []
                    for item in data:
                        tr = item.get("translations", [])
                        out.append(tr[0].get("text", "") if tr else "")
                    return out
                else:
                    print(f"{body}")
                    print(f"翻译 API 返回 {r.status_code}: {r.text}")
            except Exception as e:
                print(f"{body}")
                print("调用翻译 API 出错：", e)
            attempt += 1
            time.sleep(2 ** attempt)

        # 多次失败：返回空译文（保持长度一致）
        return [""] * len(texts)

    def translate_html(self, html, lang=None):
        """
翻译 HTML：提取文本 → 批量翻译 → 重建 HTML
        """
        nodes = extract_text_nodes(html)
        if not nodes:
            return html
        texts = [n["text"] for n in nodes]
        translated_texts = self.translate_batch(texts, lang=lang)
        return rebuild_html_from_nodes(html, translated_texts)


# ---------- 交互选择 ----------
def interactive_select_columns(tables_cols):
    selections = {}
    for t, cols in tables_cols.items():
        print(f"\n表 {t} 的列：")
        for i, (col, dtype, key) in enumerate(cols, 1):
            print(f"  {i}. {col} ({dtype}){' PK' if key == 'PRI' else ''}")
        user = input("选择要翻译的列（逗号分隔，回车跳过，all 全选）：").strip()
        if not user:
            selections[t] = []
        elif user.lower() == "all":
            selections[t] = [c[0] for c in cols]
        else:
            idxs = []
            for tok in user.split(","):
                tok = tok.strip()
                if "-" in tok:
                    a, b = tok.split("-", 1)
                    try:
                        a1 = int(a); b1 = int(b)
                        idxs.extend(range(a1, b1 + 1))
                    except:
                        pass
                else:
                    if tok.isdigit():
                        idxs.append(int(tok))
            selections[t] = [cols[i - 1][0] for i in idxs if 1 <= i <= len(cols)]
    return selections


# ---------- 翻译并写回（断点续传、失败不写） ----------
def translate_and_update(conn, translator, selections, schema, lang="de", batch_size=10):
    """
    使用 TranslatorAPI_MIC 对 HTML/文本列进行翻译并写回数据库。
    支持断点续传：翻译失败的行下次可继续翻译，不覆盖原始列。
    selections: {table_name: [col1, col2, ...], ...}
    schema: 数据库名
    lang: 目标语言
    batch_size: 每次批量处理行数
    """
    summary = {}
    for table, cols in selections.items():
        if not cols:
            continue

        pk = detect_primary_key(conn, schema, table)
        if not pk:
            print(f"表 {table} 无法检测到主键，跳过")
            continue

        summary[table] = {}

        for col in cols:
            target_col = f"{col}_{lang}"
            add_target_column_if_needed(conn, table, target_col, schema=schema)

            # 统计待翻译行数
            cursor = conn.cursor()
            sql_count = f"""
SELECT COUNT(*)
FROM {quote_ident(table)}
WHERE {quote_ident(col)} IS NOT NULL
  AND {quote_ident(col)} <> ''
  AND ({quote_ident(target_col)} IS NULL OR {quote_ident(target_col)} = '')
"""
            cursor.execute(sql_count)
            total = cursor.fetchone()[0]
            cursor.close()

            print(f"\n表 {table} 列 {col} 需要翻译 {total} 行")
            summary[table][col] = {"total": total, "ok": 0, "fail": 0}

            offset = 0
            while offset < total:
                cursor = conn.cursor()
                sql = f"""
SELECT {quote_ident(pk)}, {quote_ident(col)}
FROM {quote_ident(table)}
WHERE {quote_ident(col)} IS NOT NULL
  AND {quote_ident(col)} <> ''
  AND ({quote_ident(target_col)} IS NULL OR {quote_ident(target_col)} = '')
LIMIT %s OFFSET %s
"""
                cursor.execute(sql, (batch_size, offset))
                rows = cursor.fetchall()
                cursor.close()

                if not rows:
                    break

                ids = [r[0] for r in rows]
                texts = [r[1] or "" for r in rows]

                # 使用 TranslatorAPI_MIC 翻译 HTML
                try:
                    translations = []
                    for txt in texts:
                        try:
                            if is_html(txt):
                                # HTML 文本 → 提取节点翻译
                                tr = translator.translate_html(txt, lang=lang)
                            else:
                                # 纯文本 → 直接批量翻译
                                tr = translator.translate_batch([txt], lang=lang)[0]
                            translations.append(tr)
                        except Exception as e:
                            print(f"翻译出错: {e}")
                            translations.append("")

                except Exception as e:
                    print(f"批量翻译 HTML 出错: {e}")
                    translations = [""] * len(texts)

                # 写回数据库
                cursor = conn.cursor()
                for rid, src, tr in zip(ids, texts, translations):
                    if tr and tr.strip():
                        try:
                            sql_up = f"""
UPDATE {quote_ident(table)}
SET {quote_ident(target_col)} = %s , {quote_ident(col)} = %s
WHERE {quote_ident(pk)} = %s
"""
                            cursor.execute(sql_up, (tr,tr, rid))
                            summary[table][col]["ok"] += 1
                        except Exception as e:
                            print(f"更新失败 行 {rid}: {e}")
                            summary[table][col]["fail"] += 1
                    else:
                        print(f"翻译失败，跳过 行 {rid}")
                        summary[table][col]["fail"] += 1
                conn.commit()
                cursor.close()

                offset += batch_size
                print(f"已处理 {min(offset, total)}/{total} 行")

    return summary


def run():
    cfg = load_json_if_exists(CONFIG_FILE)
    if not cfg:
        cfg = {
            "host": input("数据库主机 (默认 localhost): ").strip() or "localhost",
            "port": int(input("数据库端口 (默认 3306): ").strip() or "3306"),
            "user": input("数据库用户 (默认 root): ").strip() or "root",
            "password": getpass.getpass("数据库密码: "),
            "database": input("数据库名: ").strip(),
            "api_key": input("Microsoft Translator Key: ").strip(),
            "api_region": input("Microsoft Translator Region: ").strip(),
            "batch_size": int(input("批量大小 (默认10): ").strip() or "10"),
        }
        save_json(CONFIG_FILE, cfg)
        print("配置已保存到 config.json")

    # 连接 DB（确保使用与你安装驱动相同的 Python）
    conn = get_db_connection(cfg)

    # 查找表
    # tables = find_target_tables(conn, cfg["database"])
    # if not tables:
    #     print("未找到 fa_ldcms_document 相关表（请确认数据库名和表名前缀）")
    #     conn.close()
    #     return

    # 列出列
    # tables_cols = {t: list_table_columns(conn, cfg["database"], t) for t in tables}

    # 读取历史选择
    selections = load_json_if_exists(SELECTION_FILE)
    if not selections:
        selections = interactive_select_columns(tables_cols)
        save_json(SELECTION_FILE, selections)
        print("选择已保存到 selections.json")


    #微软翻译api
    translator = TranslatorAPI_MIC(cfg["api_key"], cfg["api_region"])

    summary = translate_and_update(conn, translator, selections, schema=cfg["database"], lang="de", batch_size=cfg.get("batch_size", 10))

    print("\n=== 翻译总结 ===")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    conn.close()


if __name__ == "__main__":
    run()
