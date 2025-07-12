import os
import re
import json
import gspread
from google.oauth2 import service_account
import google.generativeai as genai
from dotenv import load_dotenv

load_dotenv()

# --- ⚙️ 1. 사용자 설정 ---
API_KEY = os.getenv("GOOGLE_API_KEY")  # .env 파일에서 API 키 가져오기
SERVICE_ACCOUNT_FILE = 'pdf-ocr.json'
SPREADSHEET_NAME = 'pdf-ocr'
PDF_FOLDER_PATH = './pdfs/'

# --- 🤖 2. 추출 필드 및 프롬프트 ---
EXTRACTION_FIELDS = [
    "성명", "생년월일", "안내유형", "기장의무", "추계시 적용경비율",
    "소득종류", "이자", "배당", "근로-단일", "근로-복수",
    "연금", "기타", "종교인 기타소득유무", "중간예납세액", "원천징수세액",
    "국민연금보험료", "개인연금저축", "소기업소상공인공제부금 (노란우산공제)",
    "퇴직연금세액공제", "연금계좌세액공제", "사업자 등록번호", "상호", "수입금액 구분코드",
    "업종 코드", "사업 형태", "기장 의무", "경비율",
    "수입금액", "일반", "자가", "일반(기본)", "자가(초과)"
]

json_example = "[\n" + "  {\n" + ",\n".join([f'    "{field}": "값"' for field in EXTRACTION_FIELDS]) + "\n  },\n  {\n" + ",\n".join([f'    "{field}": "값2"' for field in EXTRACTION_FIELDS]) + "\n  }\n]"

GEMINI_PROMPT = f"""
## 역할
당신은 주어진 문서 전체를 종합적으로 분석하여, 여러 다른 위치와 형식의 표나 텍스트에서 데이터를 정확히 추출하고 구조화된 JSON으로 변환하는 OCR 전문가입니다.

## 작업 순서

### 1단계: 전체 문서에서 단일 값 필드 스캔
먼저 문서 전체를 스캔하여 다음 항목들처럼 주로 한 번만 나타나는 값들을 찾습니다:
- "성명", "생년월일", "안내유형", "기장의무"
- "중간예납세액", "원천징수세액"
- "국민연금보험료", "개인연금저축", "소기업소상공인공제부금 (노란우산공제)" 등

### 2단계: 사업소득 표의 모든 행 찾기
'사업장별 수입금액' 또는 유사한 표에서 **모든 행(데이터)을 찾아주세요**. 
- 각 행은 하나의 사업소득 항목을 나타냅니다
- **빈 행이나 누락된 행이 없도록 주의깊게 확인해주세요**
- 다음 필드들을 각 행에서 추출: "사업자 등록번호", "상호", "수입종류 구분코드", "업종 코드", "수입금액", "경비율" 등

### 3단계: 각 행별 JSON 객체 생성
**사업소득 표의 각 행마다** 별도의 JSON 객체를 생성합니다:
1. 해당 행의 사업 관련 데이터로 객체를 채웁니다
2. **1단계에서 찾은 모든 공통 데이터(성명, 생년월일 등)를 동일하게 복사합니다**

### 4단계: 완전한 JSON 배열 생성
- **모든 사업소득 행이 포함되도록 확인**
- 각 객체는 모든 필드를 포함해야 함
- 값이 없는 필드는 "N/A" 또는 빈 문자열로 설정

## 중요 지침
- **절대로 데이터를 누락하지 마세요**
- **모든 사업소득 행을 찾아 각각 별도의 JSON 객체로 만드세요**
- 하나의 문서에 여러 사업소득이 있다면, 그 수만큼 JSON 객체가 생성되어야 합니다

### 추출할 항목
{', '.join(EXTRACTION_FIELDS)}

### 출력 형식 (여러 행이 있을 경우의 예시)
{json_example}

**반드시 JSON 배열 형태로만 응답하고, 다른 설명은 추가하지 마세요.**
"""

# --- 💰 숫자 정제 대상 필드 ---
currency_fields = [
    "중간예납세액", "원천징수세액", "국민연금보험료", "개인연금저축",
    "소기업소상공인공제부금 (노란우산공제)", "퇴직연금세액공제", "연금계좌세액공제", "수입금액"
]

# --- 🔧 유틸리티 함수 ---
def clean_currency(value: str) -> str:
    if not isinstance(value, str): return "0"
    if value.strip() in ["", "없음", "N/A"]: return "0"
    cleaned = re.sub(r"[^\d]", "", value)
    return cleaned if cleaned else "0"

def safe_extract_json(text):
    """
    텍스트에서 JSON 배열을 안전하게 추출하는 함수
    """
    # 여러 패턴으로 JSON 찾기 시도
    patterns = [
        r'\[[\s\S]*?\]',  # JSON 배열 (가장 우선)
        r'```json\s*([\s\S]*?)\s*```',  # 마크다운 JSON 블록
        r'```\s*([\s\S]*?)\s*```',  # 일반 마크다운 블록
        r'\{[\s\S]*?\}',  # JSON 객체 (단일)
    ]
    
    for pattern in patterns:
        matches = re.findall(pattern, text, re.DOTALL)
        for match in matches:
            try:
                # 마크다운 패턴의 경우
                if '```' in pattern and isinstance(match, str):
                    json_data = json.loads(match.strip())
                else:
                    json_data = json.loads(match)
                
                # 배열이 아닌 경우 배열로 변환
                if isinstance(json_data, dict):
                    return [json_data]
                elif isinstance(json_data, list):
                    return json_data
                    
            except json.JSONDecodeError:
                continue
    
    return None

def extract_data_with_gemini(file_path: str, prompt: str):
    """
    Google Generative AI SDK를 사용하여 PDF에서 데이터를 추출합니다.
    """
    print(f"\n📄 '{os.path.basename(file_path)}' 파일 처리 시작...")
    
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"오류: PDF 파일을 찾을 수 없습니다. 경로: {file_path}")

    uploaded_file = None
    max_retries = 3
    
    for attempt in range(max_retries):
        try:
            print(f"🔄 시도 {attempt + 1}/{max_retries}")
            
            # 1. File API를 사용해 파일 업로드
            print("☁️ File API로 PDF 파일을 업로드합니다...")
            uploaded_file = genai.upload_file(path=file_path, display_name=os.path.basename(file_path))
            
            # 2. 모델 초기화 및 콘텐츠 생성 요청
            model = genai.GenerativeModel(model_name="gemini-2.5-flash")
            
            print("🧠 Gemini에게 데이터 추출을 요청합니다...")
            response = model.generate_content([uploaded_file, prompt])
            
            print(f"📝 응답 받음 (시도 {attempt + 1}/{max_retries})")
            print(f"응답 길이: {len(response.text)} 문자")
            
            # 3. 안전한 JSON 추출
            extracted_data = safe_extract_json(response.text)
            
            if extracted_data is None:
                print(f"⚠️ 시도 {attempt + 1}: JSON 추출 실패")
                print(f"응답 미리보기: {response.text[:500]}...")
                if attempt < max_retries - 1:
                    continue
                else:
                    raise ValueError(f"모든 시도에서 JSON 추출 실패. 원본 응답:\n{response.text}")
            
            print(f"✅ 데이터 추출 완료. {len(extracted_data)}개 항목 발견")
            return extracted_data
            
        except Exception as e:
            print(f"❌ 시도 {attempt + 1} 실패: {e}")
            if attempt == max_retries - 1:
                raise
        finally:
            # 4. 처리 후 업로드된 파일 삭제
            if uploaded_file:
                try:
                    print(f"🗑️ 업로드된 파일 '{uploaded_file.display_name}'을 삭제합니다.")
                    genai.delete_file(uploaded_file.name)
                    uploaded_file = None
                except Exception as e:
                    print(f"⚠️ 파일 삭제 중 오류: {e}")

def validate_and_fix_data(data_list):
    """
    추출된 데이터의 유효성을 검사하고 수정
    """
    if not isinstance(data_list, list):
        print("⚠️ 데이터가 배열이 아닙니다. 배열로 변환합니다.")
        return [data_list] if isinstance(data_list, dict) else []
    
    validated_data = []
    for i, item in enumerate(data_list):
        if not isinstance(item, dict):
            print(f"⚠️ 항목 {i+1}이 객체가 아닙니다. 건너뜁니다.")
            continue
        
        # 모든 필드가 있는지 확인하고 없으면 추가
        for field in EXTRACTION_FIELDS:
            if field not in item:
                item[field] = "N/A"
        
        validated_data.append(item)
    
    print(f"✅ 데이터 검증 완료. {len(validated_data)}개 항목 유효")
    return validated_data

# --- 🚀 Main ---
def main():
    print("--- 🚀 PDF 일괄 처리 및 스프레드시트 입력을 시작합니다 ---")

    # --- Google API 인증 (Gemini 및 Sheets) ---
    try:
        # Gemini API 초기화
        if not API_KEY:
            raise ValueError("GOOGLE_API_KEY 환경변수가 설정되지 않았습니다.")
        
        genai.configure(api_key=API_KEY)
        print("✅ Gemini API 초기화 성공!")

        # Google Sheets 인증
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scopes)
        client = gspread.authorize(creds)
        spreadsheet = client.open(SPREADSHEET_NAME)
        worksheet = spreadsheet.sheet1
        
        # 오류 로그 시트 설정
        try:
            log_worksheet = spreadsheet.worksheet("오류_로그")
        except gspread.exceptions.WorksheetNotFound:
            log_worksheet = spreadsheet.add_worksheet(title="오류_로그", rows="100", cols="10")
            log_worksheet.append_row(["파일 이름", "오류 내용", "처리 시간"])
        
        print("✅ 구글 스프레드시트 연결 성공!")
    except Exception as e:
        print(f"\n❌ 구글 API 연결 실패: {e}")
        return

    # 헤더 설정
    try:
        first_row = worksheet.row_values(1)
        if not first_row:
            print("📝 1행이 비어있어 헤더를 추가합니다...")
            headers = ["파일이름", "행번호"] + EXTRACTION_FIELDS
            worksheet.append_row(headers)
        else:
            print("📝 헤더가 이미 존재합니다.")
    except Exception as e:
        print(f"❌ 헤더 확인 중 오류 발생: {e}")

    # PDF 파일 목록 가져오기
    try:
        pdf_files = [f for f in os.listdir(PDF_FOLDER_PATH) if f.lower().endswith('.pdf')]
        if not pdf_files:
            print(f"❌ '{PDF_FOLDER_PATH}' 폴더에 PDF 파일이 없습니다.")
            return
        print(f"\n📂 총 {len(pdf_files)}개의 PDF 파일을 처리합니다: {pdf_files}")
    except FileNotFoundError:
        print(f"❌ 폴더를 찾을 수 없습니다: '{PDF_FOLDER_PATH}'")
        return

    total_rows_added = 0
    error_count = 0

    # 각 PDF 파일 처리
    for pdf_file in pdf_files:
        try:
            full_path = os.path.join(PDF_FOLDER_PATH, pdf_file)
            print(f"\n🔄 '{pdf_file}' 처리 중...")
            
            # 데이터 추출
            extracted_data_list = extract_data_with_gemini(full_path, GEMINI_PROMPT)
            
            # 데이터 검증 및 수정
            validated_data = validate_and_fix_data(extracted_data_list)
            
            if not validated_data:
                print(f"⚠️ '{pdf_file}'에서 유효한 데이터를 찾지 못했습니다.")
                import datetime
                log_worksheet.append_row([pdf_file, "유효한 데이터 없음", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")])
                continue
            
            # 스프레드시트에 추가할 행들 준비
            rows_to_append = []
            for i, extracted_data in enumerate(validated_data):
                # 첫 번째 행에만 파일 이름 표시, 나머지는 빈 문자열
                file_name_to_log = pdf_file if i == 0 else ""
                row_number = i + 1
                
                data_row = [file_name_to_log, row_number]
                for field in EXTRACTION_FIELDS:
                    value = extracted_data.get(field, 'N/A')
                    if isinstance(value, str):
                        value = value.replace('\n', ' ').replace('\r', ' ')
                    if field in currency_fields:
                        value = clean_currency(str(value))
                    data_row.append(str(value))
                
                rows_to_append.append(data_row)
            
            # 한 번에 모든 행 추가 (효율성 증대)
            if rows_to_append:
                worksheet.append_rows(rows_to_append)
                total_rows_added += len(rows_to_append)
            
            print(f"✅ '{pdf_file}' 처리 완료!")
            print(f"   📊 추출된 데이터: {len(validated_data)}개 항목")
            print(f"   📝 스프레드시트 추가: {len(rows_to_append)}개 행")

        except Exception as e:
            error_message = f"🚨 '{pdf_file}' 처리 중 오류 발생: {e}"
            print(error_message)
            
            # 오류 로그에 기록
            import datetime
            log_worksheet.append_row([pdf_file, str(e), datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")])
            error_count += 1
            continue

    # 최종 결과 출력
    print(f"\n--- ✨ 모든 작업이 완료되었습니다 ---")
    print(f"📊 총 처리된 파일: {len(pdf_files)}개")
    print(f"✅ 성공: {len(pdf_files) - error_count}개")
    print(f"❌ 오류: {error_count}개")
    print(f"📝 총 추가된 행: {total_rows_added}개")
    
    if error_count > 0:
        print(f"🔍 오류 상세 내용은 '오류_로그' 시트를 확인하세요.")

if __name__ == '__main__':
    main()