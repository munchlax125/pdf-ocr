import os
import re
import json
import gspread
from google.oauth2 import service_account
import vertexai
from vertexai.generative_models import GenerativeModel, Part

# --- ⚙️ 1. 사용자 설정 ---
GCP_PROJECT_ID = "pdf-ocr-project-464708"
GCP_LOCATION = "us-central1"
SERVICE_ACCOUNT_FILE = 'pdf-ocr.json'
SPREADSHEET_NAME = 'pdf-ocr'
PDF_FOLDER_PATH = './pdfs/'

# --- 🤖 2. 추출 필드 및 프롬프트 ---
# EXTRACTION_FIELDS를 프롬프트와 일치하도록 간소화합니다.
EXTRACTION_FIELDS = [
    "성명", "생년월일", "안내유형", "기장의무", "추계시 적용경비율",
    "소득종류", "이자", "배당", "근로-단일", "근로-복수",
    "연금", "기타", "종교인 기타소득유무", "중간예납세액", "원천징수세액",
    "국민연금보험료", "개인연금저축", "소기업소상공인공제부금 (노란우산공제)",
    "퇴직연금세액공제", "연금계좌세액공제", "사업자 등록번호", "상호", "수입금액 구분코드",
    "업종 코드", "사업 형태", "기장 의무", "경비율",
    "수입금액", "일반", "자가", "일반(기본)", "자가(초과)"
]
json_example = "[\n" + "  {\n" + ",\n".join([f'    "{field}": "값"' for field in EXTRACTION_FIELDS]) + "\n  }\n]"


GEMINI_PROMPT = f"""
## 역할
당신은 주어진 문서 전체를 종합적으로 분석하여, 여러 다른 위치와 형식의 표나 텍스트에서 데이터를 정확히 추출하고 구조화된 JSON으로 변환하는 OCR 전문가입니다.

## 작업 순서

### 1단계: 전체 문서에서 단일 값 필드 스캔
먼저 문서 전체를 스캔하여 다음 항목들처럼 주로 한 번만 나타나는 값들을 찾습니다. 이 값들은 여러 다른 표나 텍스트 영역에 흩어져 있을 수 있습니다.
- "성명", "생년월일", "안내유형", "기장의무"
- "중간예납세액", "원천징수세액"
- "국민연금보험료", "개인연금저축", "소기업소상공인공제부금 (노란우산공제)" 등

### 2단계: 주(Main) 사업소득 표 처리
'사업장별 수입금액'과 관련된 표를 찾습니다. 이 표는 여러 행(여러 사업 소득)을 포함할 수 있습니다. 각 행에서 다음 필드들을 추출합니다.
- "사업자 등록번호", "상호", "수입종류 구분코드", "업종 코드", "수입금액", "경비율" 등
- "경비을" 또는 "경비율"은 "경비율" 필드에 포함됩니다.
- **[매우 중요]** 하나의 셀 안에 텍스트가 여러 줄로 나뉘어 있을 때, 이 텍스트 덩어리 전체는 **하나의 값**입니다. 예를 들어, 이미지의 '사업소득지급명세\n서 등 결정자료'라는 텍스트는 **전체가 '수입종류 구분코드' 열에 속하는 하나의 값**입니다. 옆 칸인 '상호'가 비어있다고 해서 텍스트의 일부를 '상호'의 값으로 절대 할당해서는 안됩니다.

### 3단계: JSON 객체 생성 및 병합
'주 사업소득 표'의 각 행마다 하나의 JSON 객체를 생성합니다.
1.  먼저 해당 행에서 추출한 값들(예: "업종 코드", "수입금액")으로 객체를 채웁니다.
2.  그 다음, **1단계에서 찾은 모든 단일 값들을 방금 만든 JSON 객체에 추가합니다.** 만약 주 사업소득 표에 행이 여러 개라면, 모든 단일 값들은 모든 JSON 객체에 동일하게 복사되어야 합니다.

### 4단계: 최종 리스트(배열) 완성
3단계에서 생성된 모든 JSON 객체들을 하나의 JSON 리스트(배열)로 묶어 최종 결과를 만듭니다.

## 최종 지시
위의 단계별 지침을 엄격하게 따라서, 문서 전체의 정보를 종합하여 JSON 리스트(배열) 형태로 출력해주세요. 다른 설명은 절대 추가하지 마세요.

### 추출할 항목
{', '.join(EXTRACTION_FIELDS)}

### 출력 형식
결과는 반드시 아래와 같은 **JSON 리스트(배열)** 형식으로만 응답해야 합니다.
{json_example}
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

def extract_data_with_gemini(project_id, location, file_path, prompt, credentials):
    print(f"\n📄 '{os.path.basename(file_path)}' 파일 처리 시작...")
    vertexai.init(project=project_id, location=location, credentials=credentials)
    model = GenerativeModel("gemini-2.5-pro")
    
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"오류: PDF 파일을 찾을 수 없습니다. 경로: {file_path}")

    with open(file_path, "rb") as f:
        pdf_content = f.read()

    pdf_part = Part.from_data(data=pdf_content, mime_type="application/pdf")
    print("🧠 Gemini에게 데이터 추출을 요청합니다...")
    response = model.generate_content([pdf_part, prompt])

    # 👇 [수정됨] JSON 리스트를 찾도록 정규식 변경
    match = re.search(r"\[.*\]", response.text, re.DOTALL)
    if not match:
        # Markdown 코드 블록(```json ... ```)도 찾아보도록 로직 추가
        match_md = re.search(r"```json\s*(\[.*\])\s*```", response.text, re.DOTALL)
        if not match_md:
            raise ValueError(f"Gemini 응답에서 유효한 JSON 리스트를 찾지 못했습니다. 응답: {response.text}")
        response_text = match_md.group(1)
    else:
        response_text = match.group(0)

    print("✅ 데이터 추출 완료.")
    return json.loads(response_text)

# --- 🚀 Main ---
def main():
    print("--- 🚀 PDF 일괄 처리 및 스프레드시트 입력을 시작합니다 ---")

    try:
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/cloud-platform"
        ]
        creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scopes)
        client = gspread.authorize(creds)
        spreadsheet = client.open(SPREADSHEET_NAME)
        worksheet = spreadsheet.sheet1
        print("✅ 구글 스프레드시트 연결 성공!")
    except Exception as e:
        print(f"\n❌ 구글 API 연결 실패: {e}")
        return

    try:
        first_row = worksheet.row_values(1)
        if not first_row:
            print("📝 1행이 비어있어 헤더를 추가합니다...")
            headers = ["파일이름"] + EXTRACTION_FIELDS
            worksheet.append_row(headers)
        else:
            print("📝 헤더가 이미 존재합니다.")
    except Exception as e:
        print(f"❌ 헤더 확인 중 오류 발생: {e}")

    try:
        pdf_files = [f for f in os.listdir(PDF_FOLDER_PATH) if f.lower().endswith('.pdf')]
        if not pdf_files:
            print(f"❌ '{PDF_FOLDER_PATH}' 폴더에 PDF 파일이 없습니다.")
            return
        print(f"\n📂 총 {len(pdf_files)}개의 PDF 파일을 처리합니다: {pdf_files}")
    except FileNotFoundError:
        print(f"❌ 폴더를 찾을 수 없습니다: '{PDF_FOLDER_PATH}'")
        return

    for pdf_file in pdf_files:
        try:
            full_path = os.path.join(PDF_FOLDER_PATH, pdf_file)
            
            # 👇 [수정됨] 이제 결과는 JSON 객체의 '리스트'임
            extracted_data_list = extract_data_with_gemini(
                GCP_PROJECT_ID, GCP_LOCATION, full_path, GEMINI_PROMPT, creds
            )
            
            # 👇 [수정됨] 추출된 데이터 리스트를 순회하며 각 행을 시트에 추가
            rows_to_append = []
            for i, extracted_data in enumerate(extracted_data_list):
                file_name_to_log = pdf_file if i == 0 else "" # 첫 행에만 파일 이름 기록
                
                data_row = [file_name_to_log]
                for field in EXTRACTION_FIELDS:
                    value = extracted_data.get(field, 'N/A')
                    if isinstance(value, str):
                        value = value.replace('\n', ' ')
                    if field in currency_fields:
                        value = clean_currency(str(value))
                    data_row.append(value)
                rows_to_append.append(data_row)
            
            if rows_to_append:
                worksheet.append_rows(rows_to_append) # 여러 행을 한번에 추가하여 효율성 증대
            
            print(f"👍 '{pdf_file}' 처리 완료. {len(extracted_data_list)}개의 행을 스프레드시트에 추가했습니다.")

        except Exception as e:
            print(f"🚨 '{pdf_file}' 처리 중 오류 발생: {e}")
            # 오류 발생 시 시트에 기록
            worksheet.append_row([pdf_file, f"오류 발생: {e}"])
            continue

    print("\n--- ✨ 모든 작업이 완료되었습니다 ---")


if __name__ == '__main__':
    main()