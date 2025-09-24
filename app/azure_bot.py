# app_rag_chroma_bot.py
import os
import tempfile
import json
import requests
from dotenv import load_dotenv
from msal import ConfidentialClientApplication
import PyPDF2
from urllib.parse import urljoin

import pandas as pd
import docx
from fastapi import FastAPI, Query, Request
from fastapi.responses import JSONResponse
from typing import Optional, List, Dict, Any
from openai import OpenAI
import numpy as np
import time
import chromadb
from chromadb.config import Settings  # harmless import for some chroma installs

import asyncio
from concurrent.futures import ThreadPoolExecutor
import logging

# ---------- load env ----------
load_dotenv()

# ---------- logging ----------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("app_rag_chroma_bot")

# ---------- OpenAI client ----------
openai_api_key = os.getenv("OPENAI_API_KEY")
openai_client = OpenAI(api_key=openai_api_key)

# ---------- FastAPI ----------
app = FastAPI()

# ---------- Chroma config ----------
CHROMA_PERSIST_DIR = os.getenv("CHROMA_PERSIST_DIR", "./chromadb_store")
CHROMA_COLLECTION = os.getenv("CHROMA_COLLECTION", "sharepoint_docs")

# ---------- SharePoint / Graph config ----------
TENANT_ID = os.getenv("sp_TENANT_ID")
CLIENT_ID = os.getenv("SP_CLIENT_ID")
CLIENT_SECRET = os.getenv("SP_CLIENT_SECRET")
HOSTNAME = os.getenv("SP_HOSTNAME", "lagozon.sharepoint.com")
SITE_PATH = os.getenv("SITE_PATH", "test2")
LIBRARY_NAME = os.getenv("LIBRARY_NAME", "Documents")

if not (TENANT_ID and CLIENT_ID and CLIENT_SECRET):
    raise SystemExit("Set sp_TENANT_ID, SP_CLIENT_ID, SP_CLIENT_SECRET in environment (or .env)")

# ---------- Bot credentials (for replying via Bot Framework) ----------
# This is the Azure AD App Registration used for the bot (same as in Azure Bot resource)
MICROSOFT_APP_ID = os.getenv("MICROSOFT_APP_ID", "")
MICROSOFT_APP_PASSWORD = os.getenv("MICROSOFT_APP_PASSWORD", "")
MICROSOFT_APP_TENANT_ID = os.getenv("MICROSOFT_APP_TENANT_ID", TENANT_ID)

if not (MICROSOFT_APP_ID and MICROSOFT_APP_PASSWORD and MICROSOFT_APP_TENANT_ID):
    logger.warning("Bot credentials (MICROSOFT_APP_ID / MICROSOFT_APP_PASSWORD / MICROSOFT_APP_TENANT_ID) not fully set. Replies to Bot Framework will fail without them.")

# ---------- RAG/embedding params ----------
EMBED_MODEL = "text-embedding-3-small"
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "2000"))
CHUNK_OVERLAP = int(os.getenv("CHUNK_OVERLAP", "200"))
BATCH_SIZE = int(os.getenv("EMBED_BATCH", "16"))

# ---------- Chroma init ---------- 
try:
    chroma_client = chromadb.PersistentClient(path=CHROMA_PERSIST_DIR)
except Exception:
    chroma_client = chromadb.Client()
collection = chroma_client.get_or_create_collection(name=CHROMA_COLLECTION)

# ---------- ThreadPool ----------
executor = ThreadPoolExecutor(max_workers=4)
 
# ----------------- Graph helpers -----------------
def get_graph_token():
    """Token to call Microsoft Graph (SharePoint)."""
    authority = f"https://login.microsoftonline.com/{TENANT_ID}"
    app_msal = ConfidentialClientApplication(CLIENT_ID, authority=authority, client_credential=CLIENT_SECRET)
    result = app_msal.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
    if "access_token" not in result:
        raise SystemExit("Failed to obtain Graph access token: " + json.dumps(result, indent=2))
    return result["access_token"]

def get_bot_token():
    """Token to call Bot Framework service (used to post activities to serviceUrl)."""
    authority = f"https://login.microsoftonline.com/{MICROSOFT_APP_TENANT_ID}"
    app_msal = ConfidentialClientApplication(MICROSOFT_APP_ID, authority=authority, client_credential=MICROSOFT_APP_PASSWORD)
    result = app_msal.acquire_token_for_client(scopes=["https://api.botframework.com/.default"])
    if "access_token" not in result:
        logger.error("Failed to obtain Bot Framework token: %s", json.dumps(result))
        raise SystemExit("Failed to obtain Bot Framework token: " + json.dumps(result, indent=2))
    return result["access_token"]

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

# lightweight listing (names only)
def find_site(token, hostname=HOSTNAME, site_path=SITE_PATH):
    url = f"https://graph.microsoft.com/v1.0/sites/{hostname}:/sites/{site_path}"
    return graph_get(url, token)

def find_drive_for_library(token, site_id, library_name=LIBRARY_NAME):
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives"
    data = graph_get(url, token)
    for d in data.get("value", []):
        if d.get("name", "").lower() == library_name.lower() or d.get("driveType","").lower() == library_name.lower():
            return d
        if d.get("displayName", "").lower() == library_name.lower():
            return d
    if data.get("value"):
        return data["value"][0]
    return None

def list_children(token, drive_id, item_id=None):
    if item_id:
        url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{item_id}/children"
    else:
        url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root/children"
    return graph_get(url, token).get("value", [])

def download_drive_item(token, drive_id, item_id, local_path):
    url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{item_id}/content"
    r = graph_get_stream(url, token)
    with open(local_path, "wb") as f:
        for chunk in r.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)
    return local_path

def list_docs_from_graph(library_name=LIBRARY_NAME):
    token = get_graph_token()
    site = find_site(token, HOSTNAME, SITE_PATH)
    site_id = site["id"]
    drive = find_drive_for_library(token, site_id, library_name)
    if not drive:
        raise SystemExit(f"Could not find drive/library named {library_name}")
    drive_id = drive["id"]

    items = list_children(token, drive_id)
    return [
        {
            "name": item.get("name"),
            "id": item.get("id"),
            "downloadUrl": item.get("@microsoft.graph.downloadUrl")
        }
        for item in items if item.get("file")
    ]
def build_doc_preview_card(answer: str, retrieved_docs: List[Dict[str, Any]]):
    attachments = []
    # Get unique docs
    unique_doc_names = {doc["doc_name"] for doc in retrieved_docs if doc.get("doc_name")}
    all_docs = list_docs_from_graph()
    name_to_doc = {d["name"]: d for d in all_docs}

    for doc_name in unique_doc_names:
        doc = name_to_doc.get(doc_name)
        if not doc:
            continue

        # HeroCard with file preview
        attachments.append({
            "contentType": "application/vnd.microsoft.card.hero",
            "content": {
                "title": doc_name,
                "text": answer,
                "buttons": [
                    {
                        "type": "invoke",
                        "title": f"Open {doc_name}",
                        "value": {
                            "type": "openFilePreview",
                            "filePreviewInfo": {
                                "name": doc_name,
                                "fileType": doc_name.split(".")[-1],
                                "objectUrl": doc.get("downloadUrl")
                            }
                        }
                    }
                ]
            }
        })

    return {"attachments": attachments}
def fetch_docs_from_graph(library_name=LIBRARY_NAME):
    token = get_graph_token()
    site = find_site(token, HOSTNAME, SITE_PATH)
    site_id = site["id"]
    drive = find_drive_for_library(token, site_id, library_name)
    if not drive:
        raise SystemExit(f"Could not find drive/library named {library_name}")
    drive_id = drive["id"]
    docs = []
    def _walk_and_download(parent_item_id=None, path_prefix=""):
        children = list_children(token, drive_id, parent_item_id)
        for child in children:
            name = child.get("name")
            item_id = child.get("id")
            folder = child.get("folder")
            file_meta = child.get("file")
            item_path = os.path.join(path_prefix, name)
            if folder:
                _walk_and_download(item_id, path_prefix=item_path)
            elif file_meta:
                suffix = os.path.splitext(name)[1]
                with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                    local_path = tmp.name
                try:
                    download_drive_item(token, drive_id, item_id, local_path)
                    content = extract_text_from_file(local_path, name)
                    try:
                        os.unlink(local_path)
                    except:
                        pass
                    docs.append({"name": item_path, "content": content})
                except Exception:
                    docs.append({"name": item_path, "content": ""})
    _walk_and_download()
    return docs

# ----------------- File extraction (same as yours) -----------------
def extract_text_from_file(file_path, file_name):
    try:
        file_ext = os.path.splitext(file_name)[1].lower()
        if file_ext == '.pdf':
            with open(file_path, 'rb') as file:
                pdf_reader = PyPDF2.PdfReader(file)
                text = ""
                for page_num, page in enumerate(pdf_reader.pages):
                    try:
                        page_text = page.extract_text()
                    except Exception:
                        page_text = None
                    if page_text:
                        text += f"Page {page_num + 1}:\n{page_text}\n\n"
                return text if text else ""
        elif file_ext == '.csv':
            try:
                df = pd.read_csv(file_path, nrows=200)
                return df.to_string()
            except:
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
                    return file.read(5000)
        elif file_ext in ['.xlsx', '.xls']:
            try:
                excel_file = pd.ExcelFile(file_path)
                text = ""
                for sheet_name in excel_file.sheet_names[:5]:
                    df = pd.read_excel(file_path, sheet_name=sheet_name, nrows=50)
                    text += f"Sheet: {sheet_name}\n{df.to_string()}\n\n"
                return text
            except Exception:
                return ""
        elif file_ext == '.docx':
            try:
                doc = docx.Document(file_path)
                text = ""
                for paragraph in doc.paragraphs:
                    if paragraph.text.strip():
                        text += paragraph.text + "\n"
                return text
            except Exception:
                return ""
        elif file_ext in ['.txt', '.md', '.json', '.xml', '.html']:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
                return file.read(20000)
        else:
            try:
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
                    return file.read(5000)
            except:
                return ""
    except Exception:
        return ""

# ----------------- Chunking & embeddings -----------------
def chunk_text(text: str, chunk_size: int = CHUNK_SIZE, overlap: int = CHUNK_OVERLAP):
    if not text:
        return []
    chunks = []
    start = 0
    length = len(text)
    while start < length:
        end = start + chunk_size
        piece = text[start:end]
        chunks.append((start, min(end, length), piece))
        if end >= length:
            break
        start = end - overlap
    return chunks

def embed_texts(texts: List[str]) -> List[List[float]]:
    embeddings = []
    for i in range(0, len(texts), BATCH_SIZE):
        batch = texts[i:i+BATCH_SIZE]
        resp = openai_client.embeddings.create(model=EMBED_MODEL, input=batch)
        for d in resp.data:
            embeddings.append(d.embedding)
    return embeddings

# ----------------- Build index (Chroma) -----------------
def build_index_to_chroma(force_rebuild: bool = False, library_name: str = LIBRARY_NAME):
    try:
        existing_count = collection.count()
        if not force_rebuild and existing_count and existing_count > 0:
            return {"status": "exists", "count": existing_count}
    except Exception:
        pass

    if force_rebuild:
        try:
            chroma_client.delete_collection(name=CHROMA_COLLECTION)
        except Exception:
            pass
        new_collection = chroma_client.get_or_create_collection(name=CHROMA_COLLECTION)
        globals()["collection"] = new_collection

    docs = fetch_docs_from_graph(library_name)

    chunk_texts = []
    chunk_metadatas = []
    chunk_ids = []

    for doc in docs:
        name = doc.get("name")
        full = doc.get("content", "") or ""
        chunks = chunk_text(full)
        for idx, (start, end, piece) in enumerate(chunks):
            cid = f"{name}__chunk_{idx}"
            meta = {"doc_name": name, "chunk_id": cid, "start": start, "end": end}
            chunk_ids.append(cid)
            chunk_texts.append(piece)
            chunk_metadatas.append(meta)

    if not chunk_texts:
        return {"status": "empty", "count": 0}

    embeddings = embed_texts(chunk_texts)

    collection.add(ids=chunk_ids, documents=chunk_texts, metadatas=chunk_metadatas, embeddings=embeddings)

    try:
        chroma_client.persist()
    except Exception:
        pass

    return {"status": "built", "count": collection.count()}

# ----------------- Query & retrieval (Chroma) -----------------
def query_chroma_topk(query: str, top_k: int = 4, doc_name_filter: Optional[str] = None):
    resp = openai_client.embeddings.create(model=EMBED_MODEL, input=query)
    q_emb = resp.data[0].embedding

    where = None
    if doc_name_filter:
        where = {"doc_name": doc_name_filter}

    res = collection.query(query_embeddings=[q_emb], n_results=top_k, where=where, include=['documents', 'metadatas', 'distances'])
    retrieved = []
    if res and 'documents' in res and len(res['documents']) > 0:
        docs_list = res['documents'][0]
        metas_list = res['metadatas'][0]
        dists = res.get('distances', [[]])[0]
        for doc_text, meta, dist in zip(docs_list, metas_list, dists):
            retrieved.append({
                "score": float(dist) if dist is not None else None,
                "doc_name": meta.get("doc_name"),
                "chunk_id": meta.get("chunk_id"),
                "start": meta.get("start"),
                "end": meta.get("end"),
                "text": doc_text
            })
    return retrieved

def build_context_from_chunks(chunks: List[Dict[str, Any]], max_chars: int = 20000):
    ctx_parts = []
    accumulated = 0
    for c in chunks:
        name = c.get("doc_name", "<unknown>")
        chunk_id = c.get("chunk_id", "<chunk>")
        text = c.get("text", "") or ""
        allowed = max_chars - accumulated
        if allowed <= 0:
            break
        piece = text if len(text) <= allowed else text[:allowed]
        ctx_parts.append(f"### Document: {name} | Chunk: {chunk_id}\n{piece}\n")
        accumulated += len(piece)
    return "\n\n".join(ctx_parts)

def answer_with_context(question: str, context: str, model: str = "gpt-4", max_tokens: int = 400):
    system_msg = "You are a helpful assistant. Answer the user's question using ONLY the provided document context. If the answer is not present, say you can't find it."
    user_prompt = f"Context:\n{context}\n\nQuestion: {question}\nAnswer concisely and cite document names and chunk ids if useful."
    resp = openai_client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system_msg},
            {"role": "user", "content": user_prompt}
        ],
        max_tokens=max_tokens,
        temperature=0.0,
    )
    answer = resp.choices[0].message.content.strip()
    return answer, resp

# ---------- Bot integration (direct MSAL + REST) ----------
def get_rag_answer_sync(question: str):
    retrieved = query_chroma_topk(question, top_k=4)
    if not retrieved:
        return "I couldn't find relevant information in the documents.", []
    context = build_context_from_chunks(retrieved, max_chars=12000)
    answer, _ = answer_with_context(question, context, model="gpt-4", max_tokens=400)
    return answer, retrieved
@app.post("/api/messages")
async def messages(req: Request):
    try:
        data = await req.json()
    except Exception:
        logger.exception("Failed to parse incoming request as JSON")
        return JSONResponse(status_code=200, content={})  # ack but ignore

    logger.info("Received activity: %s", json.dumps(data)[:2000])

    # ignore non-messages
    if data.get("type") != "message" or not data.get("text"):
        return JSONResponse(status_code=200, content={})

    user_text = data.get("text", "").strip().lower()
    reply = None

    # -------------------- Handle commands --------------------
    if user_text in ["refresh index", "rebuild index", "update docs"]:
        result = build_index_to_chroma(force_rebuild=True)
        reply = {
            "type": "message",
            "text": f"✅ Index refreshed. {result['count']} chunks indexed.",
            "from": data.get("recipient"),
            "recipient": data.get("from"),
            "conversation": data.get("conversation"),
        }

    elif user_text in ["list docs", "show documents", "list documents"]:
        try:
            docs = list_docs_from_graph()
            if docs:
                reply = {
                    "type": "message",
                    "text": "📂 Available documents:\n" + "\n".join([d["name"] for d in docs]),
                    "from": data.get("recipient"),
                    "recipient": data.get("from"),
                    "conversation": data.get("conversation"),
                }
            else:
                reply = {
                    "type": "message",
                    "text": "No documents found.",
                    "from": data.get("recipient"),
                    "recipient": data.get("from"),
                    "conversation": data.get("conversation"),
                }
        except Exception as e:
            logger.exception("Error listing docs")
            reply = {
                "type": "message",
                "text": f"❌ Failed to list documents: {e}",
                "from": data.get("recipient"),
                "recipient": data.get("from"),
                "conversation": data.get("conversation"),
            }

    # -------------------- Else → RAG answer with preview card --------------------
    else:
# Run RAG in threadpool
        loop = asyncio.get_running_loop()
        answer, retrieved = await loop.run_in_executor(executor, get_rag_answer_sync, user_text)

        # Attach preview links only for retrieved docs
        reply_card = build_doc_preview_card(answer, retrieved)

        reply = {
            "type": "message",
            "from": data.get("recipient"),
            "recipient": data.get("from"),
            "conversation": data.get("conversation"),
            **reply_card
        }

    # -------------------- Send reply --------------------
    try:
        service_url = data.get("serviceUrl")
        conversation_id = data.get("conversation", {}).get("id")
        url = urljoin(service_url, f"v3/conversations/{conversation_id}/activities")

        headers = {"Content-Type": "application/json"}
        if not service_url.startswith("http://localhost"):
            bot_token = get_bot_token()
            headers["Authorization"] = f"Bearer {bot_token}"

        r = requests.post(url, headers=headers, json=reply, timeout=30)
        logger.info("Posted reply: %s %s", r.status_code, r.text[:1000])
    except Exception as e:
        logger.exception("Failed to post reply: %s", e)
# ---------- admin/dev endpoints ----------
@app.get("/build_index")
def build_index_api(force: bool = Query(False, description="Force rebuild index in Chroma"),
                    library_name: Optional[str] = Query(None, description="SharePoint document library name")):
    try:
        lib = library_name or LIBRARY_NAME
        result = build_index_to_chroma(force_rebuild=force, library_name=lib)
        return JSONResponse(content={"status": "ok", "result": result})
    except Exception as e:
        logger.exception("Error in build_index_api")
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.post("/rag_query")
async def rag_query(request: Request,
                    question: str = Query(..., description="Question to ask based on docs"),
                    top_k: int = Query(4, description="Number of chunks to retrieve"),
                    max_context_chars: int = Query(20000, description="Max chars of aggregated context"),
                    model: str = Query("gpt-4", description="Model to use"),
                    doc_name_filter: Optional[str] = Query(None, description="Optional filter to restrict to a doc_name")):
    if not question:
        return JSONResponse(status_code=400, content={"error": "Question is required."})
    try:
        retrieved = query_chroma_topk(question, top_k=top_k, doc_name_filter=doc_name_filter)
        if not retrieved:
            return {"question": question, "answer": "", "retrieved_chunks": []}
        context = build_context_from_chunks(retrieved, max_chars=max_context_chars)
        answer, raw = answer_with_context(question, context, model=model)
        return {"question": question, "answer": answer, "retrieved_chunks": [{"doc_name": r["doc_name"], "chunk_id": r["chunk_id"], "score": r["score"]} for r in retrieved]}
    except Exception as e:
        logger.exception("Error in rag_query")
        return JSONResponse(status_code=500, content={"error": f"RAG error: {str(e)}"})

@app.get("/fetch_docs")
def fetch_docs_api(library_name: Optional[str] = Query(None, description="SharePoint document library name")):
    libname = library_name or LIBRARY_NAME
    try:
        docs = fetch_docs_from_graph(libname)
        return JSONResponse(content=docs)
    except Exception as e:
        logger.exception("Error in fetch_docs_api")
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/list_docs")
def list_docs_api(library_name: Optional[str] = Query(None, description="SharePoint document library name")):
    libname = library_name or LIBRARY_NAME
    try:
        docs = list_docs_from_graph(libname)
        return JSONResponse(content=docs)
    except Exception as e:
        logger.exception("Error in list_docs_api")
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/")
def root():
    return {"status": "up", "info": "Use /build_index to index docs (Chroma) and /rag_query to ask questions using chunked RAG."}
