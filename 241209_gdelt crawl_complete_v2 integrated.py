import pandas as pd
from tqdm import tqdm
from datetime import datetime
from firecrawl import FirecrawlApp
import requests

import pandas as pd
from tqdm import tqdm
from datetime import datetime

def process_scrape_ids(input_file, output_file):
    # Firecrawl API 초기화
    Firecrawl = FirecrawlApp(api_key="fc-6c67c44de7ec4f068589ee72a7ba66d5")

    # 데이터 읽어오기
    data = pd.read_json(input_file)

    # Scrape_ID가 null인 값의 개수 세기
    null_scrape_id_count = data['Scrape_ID'].isna().sum()
    print(f"Number of null Scrape_ID values: {null_scrape_id_count}")

    # GKGRECORDID, Scrape_ID, batch_scrape_status 값을 담을 리스트 준비
    results = []

    # tqdm을 사용하여 진행 상황 표시
    for i, (_, row) in enumerate(tqdm(data.iterrows(), total=len(data), desc='Processing Scrape_IDs'), start=1):
        gkgrecordid = row['GKGRECORDID']
        scrape_id = row['Scrape_ID']
        status = None
        if pd.notna(scrape_id):
            try:
                status = Firecrawl.check_batch_scrape_status(scrape_id)
            except requests.exceptions.HTTPError as e:
                print(f"Error occurred for Scrape_ID {scrape_id}: {e}")
                status = "Error: Job not found or other HTTP error"
        results.append({
            'GKGRECORDID': gkgrecordid,
            'Scrape_ID': scrape_id,
            'Batch_Scrape_Status': status
        })

        # 20개마다 결과 저장
        if i % 20 == 0 or i == len(data):
            temp_output_file = f"{output_file.rsplit('.', 1)[0]}_part_{i}.json"
            temp_df = pd.DataFrame(results)
            temp_df.to_json(temp_output_file, orient='records', indent=4)
            print(f"Results saved to {temp_output_file} at record {i}")

    # 최종 데이터 저장 (마지막까지 포함)s
    result_df = pd.DataFrame(results)
    result_df.to_json(output_file, orient='records', indent=4)
    print(f"Final results saved to {output_file}")

    # Batch_Scrape_Status가 오류인 값의 개수 세기
    error_count = result_df['Batch_Scrape_Status'].str.contains('Error: Job not found or other HTTP error').sum()
    print(f"Number of errors in Batch_Scrape_Status: {error_count}")

# 함수 호출 예제
input_filename = 'env_biofuel_processed_urls_2412111153.json'
output_filename = f"{input_filename.rsplit('.', 1)[0]}_completed_{datetime.now().strftime('%Y%m%d_%H%M')}.json"
process_scrape_ids(input_filename, output_filename)
