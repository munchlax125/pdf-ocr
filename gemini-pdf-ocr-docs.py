import os
import re
import gspread
import google.generativeai as genai
from google.oauth2 import service_account
from googleapiclient.discovery import build # <--- Docs API 사용을 위해 추가
from dotenv import load_dotenv

load_dotenv()

# --- ⚙️ 1. 사용자 설정 ---
SERVICE_ACCOUNT_FILE = 'pdf-ocr.json'
DOCS_DOCUMENT_NAME = 'pdf-ocr' # <--- 저장할 구글 문서 이름
PDF_FOLDER_PATH = './pdfs/'

# --- 🤖 2. 추출 프롬프트 (전체 텍스트 추출용으로 변경) ---
GEMINI_TEXT_PROMPT = """
주어진 PDF 문서의 모든 텍스트를 순서대로 빠짐없이 추출해주세요.
어떠한 내용도 요약하거나 수정하지 말고, 원본 그대로의 텍스트를 제공해야 합니다.
다른 설명이나 JSON 형식 없이, 오직 추출된 텍스트 내용만 응답으로 출력해주세요.
"""

# --- 🔧 3. 유틸리티 함수 (Gemini 호출 및 Docs API 함수로 변경) ---

def extract_full_text_with_gemini(file_path: str, prompt: str):
    """
    File API를 사용하여 PDF에서 전체 텍스트를 추출합니다.
    """
    print(f"\n📄 '{os.path.basename(file_path)}' 파일의 전체 텍스트 추출 시작...")
    
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"오류: PDF 파일을 찾을 수 없습니다. 경로: {file_path}")

    uploaded_file = None
    try:
        print("☁️ File API로 PDF 파일을 업로드합니다...")
        uploaded_file = genai.upload_file(path=file_path, display_name=os.path.basename(file_path))
        
        model = genai.GenerativeModel(model_name="gemini-1.5-flash-latest")
        
        print("🧠 Gemini에게 텍스트 추출을 요청합니다...")
        response = model.generate_content([uploaded_file, prompt])

        print("✅ 텍스트 추출 완료.")
        # JSON 파싱 없이, 응답 텍스트 전체를 반환
        return response.text
    
    finally:
        if uploaded_file:
            print(f"🗑️ 업로드된 파일 '{uploaded_file.display_name}'을 삭제합니다.")
            genai.delete_file(uploaded_file.name)

def get_or_create_doc(docs_service, drive_service, title):
    """
    주어진 제목의 구글 문서를 찾거나, 없으면 새로 생성합니다.
    """
    # 1. '내 드라이브'에서 해당 이름의 문서 검색
    response = drive_service.files().list(
        q=f"name='{title}' and mimeType='application/vnd.google-apps.document' and trashed=false",
        spaces='drive',
        fields='files(id, name)'
    ).execute()
    
    files = response.get('files', [])
    if files:
        doc_id = files[0].get('id')
        print(f"📄 기존 구글 문서 '{title}' (ID: {doc_id})를 찾았습니다.")
        return doc_id
    else:
        # 2. 문서가 없으면 새로 생성
        print(f"📄 '{title}' 문서를 찾을 수 없어 새로 생성합니다...")
        doc = {'title': title}
        doc = docs_service.documents().create(body=doc).execute()
        doc_id = doc.get('documentId')
        print(f"✨ 새 구글 문서 '{title}' (ID: {doc_id})를 생성했습니다.")
        return doc_id

def append_text_to_doc(docs_service, document_id, filename, text_to_append):
    """
    구글 문서의 끝에 파일 이름과 추출된 텍스트를 추가합니다.
    """
    # 문서의 현재 끝 위치를 찾기 위함 (요청 본문을 비워두면 됨)
    requests = [
        {
            'insertText': {
                'location': { 'index': 1 }, # 문서 맨 앞에 추가
                'text': f"--- {filename} ---\n\n{text_to_append}\n\n"
            }
        }
    ]
    docs_service.documents().batchUpdate(
        documentId=document_id, body={'requests': requests}
    ).execute()


# --- 🚀 4. 메인 실행 로직 (Docs API 사용으로 변경) ---
def main():
    print("--- 🚀 PDF 전체 텍스트 추출 및 구글 문서 입력을 시작합니다 ---")

    try:
        # 1. Gemini 인증
        genai.configure()
        print("✅ Gemini SDK 초기화 성공!")

        # 2. 구글 서비스(Docs, Drive) 인증
        # Docs API를 사용하기 위해 scope 추가
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets", # 시트 관련 기능이 없으면 제거 가능
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/documents" # <--- 구글 문서 편집 권한 추가
        ]
        creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scopes)
        
        # Docs 및 Drive API 서비스 빌드
        docs_service = build('docs', 'v1', credentials=creds)
        drive_service = build('drive', 'v3', credentials=creds)
        
        print("✅ 구글 Docs 및 Drive API 연결 성공!")

    except Exception as e:
        print(f"\n❌ 구글 API 연결 또는 인증 실패: {e}")
        return

    # 3. 작업할 구글 문서 가져오기 또는 생성하기
    try:
        doc_id = get_or_create_doc(docs_service, drive_service, DOCS_DOCUMENT_NAME)
    except Exception as e:
        print(f"❌ 구글 문서 처리 중 오류 발생: {e}")
        return

    # 4. PDF 파일 목록 가져오기
    try:
        pdf_files = [f for f in os.listdir(PDF_FOLDER_PATH) if f.lower().endswith('.pdf')]
        if not pdf_files:
            print(f"❌ '{PDF_FOLDER_PATH}' 폴더에 PDF 파일이 없습니다.")
            return
        print(f"\n📂 총 {len(pdf_files)}개의 PDF 파일을 처리합니다: {pdf_files}")
    except FileNotFoundError:
        print(f"❌ 폴더를 찾을 수 없습니다: '{PDF_FOLDER_PATH}'")
        return

    # 5. 각 PDF 파일에 대해 반복 작업 수행
    error_files = []
    for pdf_file in pdf_files:
        try:
            full_path = os.path.join(PDF_FOLDER_PATH, pdf_file)
            
            # 전체 텍스트 추출
            extracted_text = extract_full_text_with_gemini(full_path, GEMINI_TEXT_PROMPT)
            
            # 구글 문서에 추가
            append_text_to_doc(docs_service, doc_id, pdf_file, extracted_text)
            
            print(f"👍 '{pdf_file}' 처리 완료. 구글 문서에 추가했습니다.")

        except Exception as e:
            error_message = f"🚨 '{pdf_file}' 처리 중 오류 발생: {e}"
            print(error_message)
            error_files.append(pdf_file) # 오류 발생 파일 목록에 추가
            continue

    print("\n--- ✨ 모든 작업이 완료되었습니다 ---")
    if error_files:
        print(f"총 {len(error_files)}개의 파일에서 오류가 발생했습니다: {error_files}")


if __name__ == '__main__':
    main()