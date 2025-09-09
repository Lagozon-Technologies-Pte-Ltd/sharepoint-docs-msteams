# graph_fetch_and_extract.py
import os
import tempfile
import json
import requests
from dotenv import load_dotenv
from msal import ConfidentialClientApplication
import PyPDF2
import pandas as pd
import docx

load_dotenv()

# ===== CONFIG from env =====
TENANT_ID = os.getenv("sp_TENANT_ID")              # e.g. 60ed51ee-...
CLIENT_ID = os.getenv("SP_CLIENT_ID")              # App (client) id
CLIENT_SECRET = os.getenv("SP_CLIENT_SECRET")      # client secret (rotate if exposed)
HOSTNAME = os.getenv("SP_HOSTNAME", "lagozon.sharepoint.com")
SITE_PATH = os.getenv("SITE_PATH", "test2")       # the path part of /sites/{SITE_PATH}
LIBRARY_NAME = os.getenv("LIBRARY_NAME", "Documents")  # optional, default Documents

if not (TENANT_ID and CLIENT_ID and CLIENT_SECRET):
    raise SystemExit("Set AZ_TENANT_ID, SP_CLIENT_ID, SP_CLIENT_SECRET in environment (or .env)")

# ===== MSAL token helper =====
def get_graph_token():
    authority = f"https://login.microsoftonline.com/{TENANT_ID}"
    app = ConfidentialClientApplication(CLIENT_ID, authority=authority, client_credential=CLIENT_SECRET)
    # request Graph app-only token
    result = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
    if "access_token" not in result:
        raise SystemExit("Failed to obtain access token: " + json.dumps(result, indent=2))
    return result["access_token"]

# ===== Graph helpers =====
def graph_get(url, token, params=None):
    headers = {"Authorization": f"Bearer {token}"}
    r = requests.get(url, headers=headers, params=params)
    r.raise_for_status()
    return r.json()

def graph_get_stream(url, token, params=None):
    headers = {"Authorization": f"Bearer {token}"}
    r = requests.get(url, headers=headers, params=params, stream=True)
    r.raise_for_status()
    return r

# ===== Extractor (copied/adapted from your code) =====
def extract_text_from_file(file_path, file_name):
    try:
        file_ext = os.path.splitext(file_name)[1].lower()
        if file_ext == '.pdf':
            with open(file_path, 'rb') as file:
                pdf_reader = PyPDF2.PdfReader(file)
                text = ""
                for page_num, page in enumerate(pdf_reader.pages):
                    page_text = page.extract_text()
                    if page_text:
                        text += f"Page {page_num + 1}:\n{page_text}\n\n"
                return text if text else "No text could be extracted from PDF"

        elif file_ext == '.csv':
            try:
                df = pd.read_csv(file_path, nrows=50)
                return df.to_string()
            except:
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
                    return file.read(5000)

        elif file_ext in ['.xlsx', '.xls']:
            try:
                excel_file = pd.ExcelFile(file_path)
                text = ""
                for sheet_name in excel_file.sheet_names[:3]:
                    df = pd.read_excel(file_path, sheet_name=sheet_name, nrows=20)
                    text += f"Sheet: {sheet_name}\n{df.to_string()}\n\n"
                return text
            except Exception as e:
                return f"Excel content (partial): {str(e)}"

        elif file_ext == '.docx':
            try:
                doc = docx.Document(file_path)
                text = ""
                for paragraph in doc.paragraphs:
                    if paragraph.text.strip():
                        text += paragraph.text + "\n"
                return text if text else "No text found in Word document"
            except Exception as e:
                return f"Word document processing error: {e}"

        elif file_ext in ['.txt', '.md', '.json', '.xml', '.html']:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
                return file.read(10000)

        else:
            try:
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
                    return file.read(3000)
            except:
                return f"Unsupported file type: {file_ext}"
    except Exception as e:
        return f"Error processing file: {e}"

# ===== Graph drive traversal & download =====
def find_site(token, hostname=HOSTNAME, site_path=SITE_PATH):
    url = f"https://graph.microsoft.com/v1.0/sites/{hostname}:/sites/{site_path}"
    return graph_get(url, token)

def find_drive_for_library(token, site_id, library_name=LIBRARY_NAME):
    # List drives (document libraries) in the site and match by name/displayName
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives"
    data = graph_get(url, token)
    for d in data.get("value", []):
        # match on name or drive/displayName
        if d.get("name", "").lower() == library_name.lower() or d.get("driveType","").lower() == library_name.lower():
            return d
        if d.get("displayName", "").lower() == library_name.lower():
            return d
    # fallback to default site drive
    # many sites have the "Documents" library as the default drive root
    if data.get("value"):
        return data["value"][0]
    return None

def list_children(token, drive_id, item_id=None):
    # item_id None => list root children
    if item_id:
        url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{item_id}/children"
    else:
        url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root/children"
    return graph_get(url, token).get("value", [])

def download_drive_item(token, drive_id, item_id, local_path):
    # content endpoint
    url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{item_id}/content"
    r = graph_get_stream(url, token)
    # stream to file
    with open(local_path, "wb") as f:
        for chunk in r.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)
    return local_path

def walk_and_download(token, drive_id, parent_item_id=None, docs=None, path_prefix=""):
    if docs is None:
        docs = []
    children = list_children(token, drive_id, parent_item_id)
    for child in children:
        name = child.get("name")
        item_id = child.get("id")
        folder = child.get("folder")
        file_meta = child.get("file")
        item_path = os.path.join(path_prefix, name)
        if folder:
            # folder: recurse
            walk_and_download(token, drive_id, item_id, docs, path_prefix=item_path)
        elif file_meta:
            # file: download and extract
            suffix = os.path.splitext(name)[1]
            with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                local_path = tmp.name
            try:
                download_drive_item(token, drive_id, item_id, local_path)
                content = extract_text_from_file(local_path, name)
                # remove temp file
                try:
                    os.unlink(local_path)
                except:
                    pass
                docs.append({"name": item_path, "content": content})
                print(f"Downloaded & extracted: {item_path}")
            except Exception as e:
                print(f"Failed to download {item_path}: {e}")
    return docs

# ===== Top-level function to fetch docs =====
def fetch_docs_from_graph(library_name=LIBRARY_NAME):
    token = get_graph_token()
    site = find_site(token, HOSTNAME, SITE_PATH)
    site_id = site["id"]
    print("Found site:", site.get("displayName"), "id:", site_id)

    drive = find_drive_for_library(token, site_id, library_name)
    if not drive:
        raise SystemExit(f"Could not find drive/library named {library_name}")
    drive_id = drive["id"]
    print("Using drive:", drive.get("name") or drive.get("displayName"), "id:", drive_id)

    docs = walk_and_download(token, drive_id)
    return docs

# ===== If run directly, fetch documents and print summary =====
if __name__ == "__main__":
    print("Fetching documents from Microsoft Graph...")
    docs = fetch_docs_from_graph()
    print(f"\nDone. Extracted content from {len(docs)} files.")
    for d in docs:
        print(f"- {d['name']} ({len(d['content'])} chars)")

    # If you want to call your OpenAI QA pipeline, uncomment and adapt:
    # from your_module import ask_openai   # import your ask_openai function
    # if docs:
    #     ans = ask_openai("Summarize the documentation", docs)
    #     print("OpenAI answer:", ans)
