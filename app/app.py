# app_rag_chroma.py
import os
import tempfile
import json
import requests
from dotenv import load_dotenv
from msal import ConfidentialClientApplication
import PyPDF2
import pandas as pd
import docx
from fastapi import FastAPI, Query, Request
from fastapi.responses import JSONResponse
from typing import Optional, List, Dict, Any
from openai import OpenAI
import numpy as np
import time
import chromadb
from chromadb.config import Settings

load_dotenv()
# OpenAI client (expects OPENAI_API_KEY in env)
openai_client = OpenAI()
app = FastAPI()

# Chroma config: persist locally by default
CHROMA_PERSIST_DIR = os.getenv("CHROMA_PERSIST_DIR", "./chromadb_store")
CHROMA_COLLECTION = os.getenv("CHROMA_COLLECTION", "sharepoint_docs")

# App & MS Graph config (env)
TENANT_ID = os.getenv("sp_TENANT_ID")
CLIENT_ID = os.getenv("SP_CLIENT_ID")
CLIENT_SECRET = os.getenv("SP_CLIENT_SECRET")
HOSTNAME = os.getenv("SP_HOSTNAME", "lagozon.sharepoint.com")
SITE_PATH = os.getenv("SITE_PATH", "test2")
LIBRARY_NAME = os.getenv("LIBRARY_NAME", "Documents")

if not (TENANT_ID and CLIENT_ID and CLIENT_SECRET):
    raise SystemExit("Set sp_TENANT_ID, SP_CLIENT_ID, SP_CLIENT_SECRET in environment (or .env)")

# RAG / chunking / embedding params
EMBED_MODEL = "text-embedding-3-small"   # or text-embedding-3-large
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "2000"))
CHUNK_OVERLAP = int(os.getenv("CHUNK_OVERLAP", "200"))
BATCH_SIZE = int(os.getenv("EMBED_BATCH", "16"))

# Initialize Chroma client (DuckDB+parquet persistence)
chroma_client = chromadb.PersistentClient(path=CHROMA_PERSIST_DIR)
collection = chroma_client.get_or_create_collection(name=CHROMA_COLLECTION)
# ---------- Graph helpers ----------
def get_graph_token():
    authority = f"https://login.microsoftonline.com/{TENANT_ID}"
    app = ConfidentialClientApplication(CLIENT_ID, authority=authority, client_credential=CLIENT_SECRET)
    result = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
    if "access_token" not in result:
        raise SystemExit("Failed to obtain access token: " + json.dumps(result, indent=2))
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
def list_docs_from_graph(library_name=LIBRARY_NAME):
    token = get_graph_token()
    site = find_site(token, HOSTNAME, SITE_PATH)
    site_id = site["id"]
    drive = find_drive_for_library(token, site_id, library_name)
    if not drive:
        raise SystemExit(f"Could not find drive/library named {library_name}")
    drive_id = drive["id"]

    # List children (root level only — adjust if you want recursion)
    items = list_children(token, drive_id)
    return [{"name": item.get("name"), "id": item.get("id")} for item in items if item.get("file")]

# ---------- File extraction ----------
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

# ---------- Graph site/drive helpers ----------
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
            walk_and_download(token, drive_id, item_id, docs, path_prefix=item_path)
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
    return docs

def fetch_docs_from_graph(library_name=LIBRARY_NAME):
    token = get_graph_token()
    site = find_site(token, HOSTNAME, SITE_PATH)
    site_id = site["id"]
    drive = find_drive_for_library(token, site_id, library_name)
    if not drive:
        raise SystemExit(f"Could not find drive/library named {library_name}")
    drive_id = drive["id"]
    docs = walk_and_download(token, drive_id)
    return docs

# ---------- chunking and embeddings ----------
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

# ---------- Build index (Chroma) ----------
def build_index_to_chroma(force_rebuild: bool = False, library_name: str = LIBRARY_NAME):
    """
    Build (or reuse) Chroma collection from SharePoint docs.
    If force_rebuild True -> clears and rebuilds collection.
    """
    # If not forcing and collection has documents, reuse
    try:
        # note: chroma collection.count() may exist; we'll check via get size
        existing_count = collection.count()  # number of items
        if not force_rebuild and existing_count and existing_count > 0:
            return {"status": "exists", "count": existing_count}
    except Exception:
        # if count fails, continue to rebuild
        pass

    # Clear collection if forcing
    if force_rebuild:
        try:
            chroma_client.delete_collection(name=CHROMA_COLLECTION)
        except Exception:
            pass
        # recreate
        new_collection = chroma_client.get_or_create_collection(name=CHROMA_COLLECTION)
        globals()["collection"] = new_collection

    # Fetch docs
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
            # metadata stored as dict
            meta = {
                "doc_name": name,
                "chunk_id": cid,
                "start": start,
                "end": end
            }
            chunk_ids.append(cid)
            chunk_texts.append(piece)
            chunk_metadatas.append(meta)

    if not chunk_texts:
        return {"status": "empty", "count": 0}

    # embed all chunks (batch)
    embeddings = embed_texts(chunk_texts)

    # Add to Chroma (Chroma will store documents and metadata; we pass embeddings)
    # Chroma's collection.add expects: ids, documents/texts, metadatas, embeddings
    collection.add(
        ids=chunk_ids,
        documents=chunk_texts,
        metadatas=chunk_metadatas,
        embeddings=embeddings
    )

    # persist to disk
    try:
        chroma_client.persist()
    except Exception:
        pass

    return {"status": "built", "count": collection.count()}

# ---------- Query & retrieval (Chroma) ----------
def query_chroma_topk(query: str, top_k: int = 4, doc_name_filter: Optional[str] = None):
    # embed query
    resp = openai_client.embeddings.create(model=EMBED_MODEL, input=query)
    q_emb = resp.data[0].embedding

    # build filter for chroma: metadata filter by doc_name if provided
    where = None
    if doc_name_filter:
        where = {"doc_name": doc_name_filter}

    # Chroma query: returns documents, metadatas, distances
    res = collection.query(
        query_embeddings=[q_emb],
        n_results=top_k,
        where=where,
        include=['documents', 'metadatas', 'distances']
    )
    # res is dict-like; results in lists
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

# ---------- FastAPI endpoints ----------
app = FastAPI()

@app.get("/build_index")
def build_index_api(force: bool = Query(False, description="Force rebuild index in Chroma"),
                    library_name: Optional[str] = Query(None, description="SharePoint library name")):
    try:
        start = time.time()
        lib = library_name or LIBRARY_NAME
        result = build_index_to_chroma(force_rebuild=force, library_name=lib)
        took = time.time() - start
        return JSONResponse(content={"status": "ok", "result": result, "took_seconds": took})
    except Exception as e:
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
        return JSONResponse(status_code=500, content={"error": f"RAG error: {str(e)}"})

@app.get("/fetch_docs")
def fetch_docs_api(library_name: Optional[str] = Query(None, description="SharePoint document library name")):
    libname = library_name or LIBRARY_NAME
    try:
        docs = fetch_docs_from_graph(libname)
        return JSONResponse(content=docs)
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.post("/ask_doc_question")
async def ask_doc_question(request: Request, question: str = Query(..., description="Question to ask based on documents")):
    return await rag_query(request, question)
@app.get("/list_docs")
def list_docs_api(library_name: Optional[str] = Query(None, description="SharePoint document library name")):
    libname = library_name or LIBRARY_NAME
    try:
        docs = list_docs_from_graph(libname)
        return JSONResponse(content=docs)
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/")
def root():
    return {"status": "up", "info": "Use /build_index to index docs (Chroma) and /rag_query to ask questions using chunked RAG."}
