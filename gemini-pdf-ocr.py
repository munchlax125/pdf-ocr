import os
import re
import json
import gspread
import google.generativeai as genai # <--- 변경: 라이브러리 임포트 변경
from google.oauth2 import service_account
from dotenv import load_dotenv # .env 파일 불러오기

load_dotenv()

# --- ⚙️ 1. 사용자 설정 ---
# GCP 관련 설정은 새로운 SDK에서 직접 사용되지 않지만, 다른 연동을 위해 유지합니다.
API_KEY = os.getenv("GOOGLE_API_KEY") # <--- API 키 추가
GCP_PROJECT_ID = "pdf-ocr-project-464708" 
GCP_LOCATION = "us-central1"
SERVICE_ACCOUNT_FILE = 'pdf-ocr.json'
SPREADSHEET_NAME = 'pdf-ocr'
PDF_FOLDER_PATH = './pdfs/'

# --- 🤖 2. 추출 필드 및 프롬프트 ---
EXTRACTION_FIELDS = [
    "성명", "생년월일", "안내유형", "기장의무", "추계시 적용경비율",
    "ARS 개별인증번호", "소득종류", "이자", "배당", "근로-단일", "근로-복수",
    "연금", "기타", "종교인 기타소득유무", "중간예납세액", "원천징수세액",
    "국민연금보험료", "개인연금저축", "소기업소상공인공제부금 (노란우산공제)",
    "퇴직연금세액공제", "연금계좌세액공제", "사업자 등록번호", "상호",
    "수입종류 구분코드", "업종 코드", "사업 형태", "기장 의무", "경비율",
    "수입금액", "일반", "자가", "일반(기본)", "자가(초과)"
]
json_example = "{\n" + ",\n".join([f'  "{field}": "값"' for field in EXTRACTION_FIELDS]) + "\n}"
GEMINI_PROMPT = f"""
## 역할
당신은 이미지 속 표(Table)의 내용을 정확하게 인식하고 구조화된 JSON으로 변환하는 OCR 전문가입니다.
## 작업 순서
1.  이미지에서 정확히 순서대로 다음 열 헤더들을 식별합니다: "사업자 등록번호", "상호", "수입종류 구분코드", "업종 코드".
2.  각 데이터 행을 읽으면서, 각 셀의 내용은 바로 위에 위치한 열 헤더에 해당하는 값으로 인식해야 합니다.
3.  셀이 시각적으로 비어있으면, "N/A"로 처리합니다.
4.  **[매우 중요]** 하나의 셀 안에 텍스트가 여러 줄로 나뉘어 있을 때, 이 텍스트 덩어리 전체는 **하나의 값**입니다. 예를 들어, 이미지의 '사업소득지급명세\n서 등 결정자료'라는 텍스트는 **전체가 '수입종류 구분코드' 열에 속하는 하나의 값**입니다. 옆 칸인 '상호'가 비어있다고 해서 텍스트의 일부를 '상호'의 값으로 절대 할당해서는 안됩니다.
5.  절대로 열의 위치를 바꾸거나 값을 다른 열에 할당해서는 안 됩니다.
6. "이자", "배당", "근로-단일", "근로-복수", "연금", "기타"는 "O" 또는 "X"의 값을 가집니다. 다른 값을 출력하지 않도록 주의해주세요.
## 최종 지시
이제 주어진 이미지에 대해 위의 규칙을 엄격하게 따라서, 각 행을 JSON 객체로 변환하고 전체를 리스트(배열) 형태로 출력해주세요.
### 추출할 항목
{', '.join(EXTRACTION_FIELDS)}
### 출력 형식
결과는 반드시 아래와 같은 JSON 형식으로만 응답해야 합니다. 다른 설명이나 코멘트는 절대 추가하지 마세요.
{json_example}
"""

# --- 💰 숫자 정제 대상 필드 (변경 없음) ---
currency_fields = [
    "중간예납세액", "원천징수세액", "국민연금보험료", "개인연금저축",
    "소기업소상공인공제부금 (노란우산공제)", "퇴직연금세액공제", "연금계좌세액공제"
]

# --- 🔧 유틸리티 함수 ---
def clean_currency(value: str) -> str: # (변경 없음)
    if not isinstance(value, str): return "0"
    if value.strip() in ["", "없음", "N/A"]: return "0"
    cleaned = re.sub(r"[^\d]", "", value)
    return cleaned if cleaned else "0"

# <--- 변경: 함수 전체가 새로운 SDK 방식으로 수정됨
def extract_data_with_gemini(file_path: str, prompt: str):
    """
    최신 Google Generative AI SDK와 File API를 사용하여 PDF에서 데이터를 추출합니다.
    """
    print(f"\n📄 '{os.path.basename(file_path)}' 파일 처리 시작...")
    
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"오류: PDF 파일을 찾을 수 없습니다. 경로: {file_path}")

    uploaded_file = None
    try:
        # 1. File API를 사용해 파일 업로드
        print("☁️ File API로 PDF 파일을 업로드합니다...")
        uploaded_file = genai.upload_file(path=file_path, display_name=os.path.basename(file_path))
        
        # 2. 모델 초기화 및 콘텐츠 생성 요청
        # 최신 Gemini 1.5 Flash 모델 사용
        model = genai.GenerativeModel(model_name="gemini-2.5-flash")
        
        print("🧠 Gemini에게 데이터 추출을 요청합니다...")
        # 업로드된 파일 객체와 프롬프트를 함께 전달
        response = model.generate_content([uploaded_file, prompt])

        # 3. 응답에서 JSON 추출 (기존과 동일)
        match = re.search(r"\{.*\}", response.text, re.DOTALL)
        if not match:
            raise ValueError(f"Gemini 응답에서 유효한 JSON을 찾지 못했습니다. 응답 내용: {response.text}")

        response_text = match.group(0)
        print("✅ 데이터 추출 완료.")
        return json.loads(response_text)
    
    finally:
        # 4. 처리 후 업로드된 파일 삭제 (리소스 관리)
        if uploaded_file:
            print(f"🗑️ 업로드된 파일 '{uploaded_file.display_name}'을 삭제합니다.")
            genai.delete_file(uploaded_file.name)


# --- 🚀 Main ---
def main():
    print("--- 🚀 PDF 일괄 처리 및 스프레드시트 입력을 시작합니다 ---")

    # --- 구글 API 인증 (Gemini 및 Sheets) ---
    try:
        genai.configure() 
        print("✅ Gemini SDK 초기화 성공!")

        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scopes)
        client = gspread.authorize(creds)
        spreadsheet = client.open(SPREADSHEET_NAME)
        
        # --- 👇 1. 기본 작업 시트 및 오류 로그 시트 설정 ---
        worksheet = spreadsheet.sheet1
        
        try:
            log_worksheet = spreadsheet.worksheet("오류_로그")
            print("📝 '오류_로그' 시트를 찾았습니다.")
        except gspread.exceptions.WorksheetNotFound:
            print("📝 '오류_로그' 시트를 찾을 수 없어 새로 생성합니다.")
            log_worksheet = spreadsheet.add_worksheet(title="오류_로그", rows="100", cols="10")
            log_worksheet.append_row(["파일 이름", "오류 내용"]) # 헤더 추가
            
        print("✅ 구글 스프레드시트 연결 성공!")

    except Exception as e:
        print(f"\n❌ 구글 API 연결 또는 인증 실패: {e}")
        return

    # 헤더 추가 로직 (변경 없음)
    try:
        if not worksheet.row_values(1):
            print("📝 기본 시트에 헤더를 추가합니다...")
            headers = ["파일이름"] + EXTRACTION_FIELDS
            worksheet.append_row(headers)
        else:
            print("📝 기본 시트 헤더가 이미 존재합니다.")
    except Exception as e:
        print(f"❌ 헤더 확인 중 오류 발생: {e}")

    # PDF 파일 목록 가져오기 (변경 없음)
    try:
        pdf_files = [f for f in os.listdir(PDF_FOLDER_PATH) if f.lower().endswith('.pdf')]
        if not pdf_files:
            print(f"❌ '{PDF_FOLDER_PATH}' 폴더에 PDF 파일이 없습니다.")
            return
        print(f"\n📂 총 {len(pdf_files)}개의 PDF 파일을 처리합니다: {pdf_files}")
    except FileNotFoundError:
        print(f"❌ 폴더를 찾을 수 없습니다: '{PDF_FOLDER_PATH}'")
        return

    error_count = 0 # 오류 카운트 변수
    # --- 각 PDF 파일에 대해 반복 작업 수행 ---
    for pdf_file in pdf_files:
        try:
            full_path = os.path.join(PDF_FOLDER_PATH, pdf_file)
            extracted_data = extract_data_with_gemini(full_path, GEMINI_PROMPT)
            
            data_row = [pdf_file]
            for field in EXTRACTION_FIELDS:
                value = extracted_data.get(field, 'N/A')
                if not value or str(value).strip() == 'N/A':
                    value = 'N/A'
                else:
                    value = str(value).replace('\n', ' ')
                    if field in currency_fields:
                        value = clean_currency(value)
                data_row.append(value)
            
            worksheet.append_row(data_row)
            print(f"👍 '{pdf_file}' 처리 완료. 스프레드시트에 추가했습니다.")

        except Exception as e:
            # --- 👇 2. 오류 발생 시 터미널 출력 및 시트 기록 ---
            error_message = f"🚨 '{pdf_file}' 처리 중 오류 발생: {e}"
            print(error_message)
            log_worksheet.append_row([pdf_file, str(e)]) # '오류_로그' 시트에 기록
            error_count += 1
            continue

    print("\n--- ✨ 모든 작업이 완료되었습니다 ---")
    if error_count > 0:
        print(f"총 {error_count}개의 파일에서 오류가 발생했습니다. 자세한 내용은 '오류_로그' 시트를 확인하세요.")


if __name__ == '__main__':
    main()
