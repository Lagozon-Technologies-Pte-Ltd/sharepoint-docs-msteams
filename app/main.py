import os
import tempfile
from dotenv import load_dotenv
from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.user_credential import UserCredential
from openai import OpenAI
import PyPDF2
import pandas as pd
import docx  # For Word documents
from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.authentication_context import AuthenticationContext



# Load environment variables
load_dotenv()

# ===== CONFIG =====
SITE_URL = "https://lagozon.sharepoint.com/sites/mysite"
USERNAME = "rohit.verma@lagozon.com"
PASSWORD = "Lagozon@12356"

# Authenticate with SharePoint
try:
    ctx = ClientContext(SITE_URL).with_credentials(UserCredential(USERNAME, PASSWORD))
    print("Authentication successful")
except Exception as e:
    print(f"Authentication failed: {e}")
    exit()

# ===== FETCH DOCUMENTS =====
def fetch_docs_from_sharepoint(library_name="Documents"):
    """Fetch documents from SharePoint with proper file type handling."""
    try:
        print(f"Accessing library: {library_name}")
        
        # Get the library and load items with specific properties
        library = ctx.web.lists.get_by_title(library_name)
        items = library.items
        ctx.load(items, ["File", "File/Name", "File/ServerRelativeUrl", "File/Length"])
        ctx.execute_query()
        
        print(f"Found {len(items)} items in library")
        
        docs = []
        for i, item in enumerate(items):
            file = item.properties.get('File')
            if file:
                file_name = file.properties.get('Name', 'Unknown')
                file_url = file.properties.get('ServerRelativeUrl', '')
                file_size = file.properties.get('Length', 0)
                
                print(f"\nProcessing item {i+1}: {file_name} ({file_size} bytes)")
                
                try:
                    # Download file to temporary location
                    with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(file_name)[1]) as temp_file:
                        file = ctx.web.get_file_by_server_relative_url(file_url)
                        file.download(temp_file).execute_query()
                        temp_file_path = temp_file.name
                    
                    # Process based on file type
                    content = extract_text_from_file(temp_file_path, file_name)
                    
                    # Clean up temporary file
                    os.unlink(temp_file_path)
                    
                    if content:
                        print(f"Successfully extracted {len(content)} characters from {file_name}")
                        docs.append({"name": file_name, "content": content})
                    else:
                        print(f"Could not extract content from {file_name}")
                        
                except Exception as download_error:
                    print(f"Error downloading {file_name}: {download_error}")
                    continue
        
        return docs
    
    except Exception as e:
        print(f"Error fetching SharePoint docs: {e}")
        return []

def extract_text_from_file(file_path, file_name):
    """Extract text content from different file types."""
    try:
        file_ext = os.path.splitext(file_name)[1].lower()
        
        if file_ext == '.pdf':
            # Extract text from PDF
            with open(file_path, 'rb') as file:
                pdf_reader = PyPDF2.PdfReader(file)
                text = ""
                for page_num, page in enumerate(pdf_reader.pages):
                    page_text = page.extract_text()
                    if page_text:
                        text += f"Page {page_num + 1}:\n{page_text}\n\n"
                return text if text else "No text could be extracted from PDF"
        
        elif file_ext == '.csv':
            # Extract text from CSV
            try:
                df = pd.read_csv(file_path, nrows=50)  # First 50 rows
                return df.to_string()
            except:
                # If CSV parsing fails, try reading as text
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
                    return file.read(5000)
        
        elif file_ext in ['.xlsx', '.xls']:
            # Extract text from Excel
            try:
                excel_file = pd.ExcelFile(file_path)
                text = ""
                for sheet_name in excel_file.sheet_names[:3]:  # First 3 sheets
                    df = pd.read_excel(file_path, sheet_name=sheet_name, nrows=20)
                    text += f"Sheet: {sheet_name}\n{df.to_string()}\n\n"
                return text
            except Exception as e:
                return f"Excel content (partial): {str(e)}"
        
        elif file_ext == '.docx':
            # Extract text from Word document
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
            # Read text files directly
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
                return file.read(10000)
        
        else:
            # Try to read as text anyway for unknown file types
            try:
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
                    return file.read(3000)
            except:
                return f"Unsupported file type: {file_ext}"
            
    except Exception as e:
        return f"Error processing file: {e}"

# ===== OPENAI: ANSWER USER QUERY =====
def ask_openai(query, docs):
    """Send docs + query to OpenAI for Q&A."""
    if not docs:
        return "No documents found or could not extract content from documents."

    # Combine docs into context (limit to avoid token limits)
    context = "\n\n".join([f"Document: {d['name']}\nContent:\n{d['content'][:4000]}..." for d in docs])

    prompt = f"""
Based on the following SharePoint documents, please answer the question.

Documents Context:
{context}

Question: {query}

Please provide a comprehensive answer based on the document content. If the answer cannot be found in the documents, please state that clearly.
"""

    try:
        client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2,
            max_tokens=1000
        )
        return response.choices[0].message.content
    except Exception as e:
        return f"Error calling OpenAI: {e}"

# ===== MAIN EXECUTION =====
if __name__ == "__main__":
    print("Fetching documents from SharePoint...")
    docs = fetch_docs_from_sharepoint("Documents")
    
    print(f"\nFound {len(docs)} documents with extractable content")
    for doc in docs:
        print(f"- {doc['name']} ({len(doc['content'])} characters)")
    
    if docs:
        # Ask questions about the documents
        questions = [
            "What is the main topic of the PDF document?",
            "What data is contained in the CSV and Excel files?",
            "Summarize the content of the Word document",
            "What are the key findings or conclusions across all documents?"
        ]
        
        for i, question in enumerate(questions, 1):
            print(f"\n{'='*60}")
            print(f"Question {i}: {question}")
            answer = ask_openai(question, docs)
            print(f"Answer: {answer}")
    
    else:
        print("No documents could be processed.")