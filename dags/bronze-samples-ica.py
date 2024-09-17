import icasdk
from icasdk.apis.tags import project_analysis_api
from icasdk.model.analysis_paged_list import AnalysisPagedList
from icasdk.model.problem import Problem
import csv
import pprint
import json
from datetime import datetime

configuration = icasdk.Configuration(
    host = "https://ica.illumina.com/ica/rest",
    #api_key = {'ApiKeyAuth': {ICA_KEY}}
)

current_date = datetime.now().strftime("%Y%m%d")
csv_filename = f"ica_samples_{current_date}.csv"

with icasdk.ApiClient(configuration) as api_client:
    api_instance = project_analysis_api.ProjectAnalysisApi(api_client)

    path_params = {
        'projectId': "87be74d8-dc18-4780-a96a-f976d380cc2e",
    }

    query_params = {
        'pageSize': "1000",  
        'pageOffset': "0"
    }

    with open(csv_filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([
            "id", "timeCreated", "timeModified", "ownerId", "tenantId", "tenantName",
            "reference", "userReference", "pipeline", "status", "startDate", "endDate",
            "summary", "analysisStorage", "tags"
        ])
        
        total_count = 0
        page_num = 1

        while True:
            try:
                print(f"Calling API for page {page_num}...")
                api_response = api_instance.get_analyses(
                    path_params=path_params,
                    query_params=query_params,
                )
                print(f"API call for page {page_num} successful")
                
                if hasattr(api_response, 'body'):
                    analyses = api_response.body
                else:
                    print("Unexpected response structure. 'body' attribute not found.")
                    break

                if isinstance(analyses, dict):
                    items = analyses.get('items', [])
                    print(f"Number of analyses on page {page_num}: {len(items)}")
                    
                    if not items:
                        print("No more items to retrieve.")
                        break

                    for analysis in items:
                        try:
                            tags = json.dumps(analysis.get('tags', {}))  
                            writer.writerow([
                                analysis.get('id', 'N/A'),
                                analysis.get('timeCreated', 'N/A'),
                                analysis.get('timeModified', 'N/A'),
                                analysis.get('ownerId', 'N/A'),
                                analysis.get('tenantId', 'N/A'),
                                analysis.get('tenantName', 'N/A'),
                                analysis.get('reference', 'N/A'),
                                analysis.get('userReference', 'N/A'),
                                analysis.get('pipeline', 'N/A'),
                                analysis.get('status', 'N/A'),
                                analysis.get('startDate', 'N/A'),
                                analysis.get('endDate', 'N/A'),
                                analysis.get('summary', 'N/A'),
                                analysis.get('analysisStorage', 'N/A'),
                                tags
                            ])
                        except Exception as e:
                            print(f"Error writing row: {e}")
                            print("Analysis object:")
                            pprint.pprint(analysis)
                    
                    total_count += len(items)
                    print(f"Retrieved {len(items)} analyses. Total so far: {total_count}")

                    query_params['pageOffset'] = str(total_count)
                    page_num += 1
                else:
                    print("Unexpected response structure. 'analyses' is not a dictionary.")
                    break

            except icasdk.ApiException as e:
                print(f"Exception when calling ProjectAnalysisApi->get_analyses: {e}\n")
                print(f"Error response body: {e.body}")
                break

    print(f"CSV file '{csv_filename}' has been created with {total_count} analyses.")