import warnings
warnings.filterwarnings('ignore')
import requests
import json
import numpy as np
import pandas as pd
from dateutil import parser
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import traceback
import time
from flask import Flask, request, render_template
from apscheduler.schedulers.background import BackgroundScheduler




# Function to run SQL queries and fetch all rows
def run_sql_query_and_get_all_rows(metabase_url, sql_query, database_id, max_retries=3):
    sql_query_endpoint = f'{metabase_url}/api/dataset'
    headers = {
        'x-api-key': 'mb_g8DNSVhG2ykgqLZCMHJpKMIvR87L63riaEu3VBhmz60='
    }
    offset = 0
    limit = 2000
    all_rows = []
    retries = 0

    while True:
        try:
            paginated_sql_query = f"{sql_query} LIMIT {limit} OFFSET {offset}"
            sql_payload = {
                'type': 'native',
                'native': {
                    'query': paginated_sql_query
                },
                'database': database_id
            }
            sql_query_response = requests.post(sql_query_endpoint, headers=headers, json=sql_payload)
            sql_query_response.raise_for_status()
            sql_query_results = sql_query_response.json()
            rows = sql_query_results.get('data', {}).get('rows', [])
            if not rows:
                break
            all_rows.extend(rows)
            offset += limit
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 504 and retries < max_retries:
                retries += 1
                print(f"Retrying ({retries}/{max_retries}) due to 504 error...")
                time.sleep(5 * retries)  # Exponential backoff
            else:
                raise
    return all_rows


# Convert ISO strings to datetime
def dateconvert(date1):
    if date1 is not None:
        return parser.isoparse(date1)
    return None


# Clean data to replace invalid values
def clean_data(data):
    data.replace([np.inf, -np.inf], np.nan, inplace=True)  # Replace infinities with NaN
    data.fillna("Null", inplace=True)  # Replace NaN with "Null"
    return data


# Upload data to Google Sheets
def upload_to_google_sheets(api_key_path, data, spreadsheet_id, sheet_name):
    try:
        # Define the scope and authorize the credentials
        scope = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_dict(api_key_path, scope)
        client = gspread.authorize(creds)

        # Open the spreadsheet and the specific worksheet
        spreadsheet = client.open_by_key(spreadsheet_id)
        worksheet = spreadsheet.worksheet(sheet_name)

        # Prepare the new data for upload
        data = data.applymap(lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if isinstance(x, pd.Timestamp) else x)
        data_to_upload = [data.columns.tolist()] + data.values.tolist()
        worksheet.update('A1', data_to_upload)
        print("Data uploaded to Google Sheets successfully!")
    except Exception as e:
        print("An error occurred during upload to Google Sheets:")
        print(traceback.format_exc())


# Main function





app = Flask(__name__)



def main():
        try:
            path = "metabase-427414-ba937f0f0e41.json"
            with open(path, "r") as file:
                credentials = json.load(file)
            metabase_url = 'https://metabase.joinelevatenow.co.in'
            database_id = 2
            json_keyfile_path = credentials

            # SQL query to fetch the data
            sql_query3 = """
            with CTE1 AS (
    SELECT
      pr.updated_at AS created_at,
      up.full_name AS User_Name,
      up.phone AS User_Phone,
      up.email AS User_email,
      ap.full_name AS EE_Name,
      ap.email AS EE_email,
      up.age AS Age,
      up.Weight AS Starting_Weight,
      up.height AS Height,
      up.starting_bmi AS BMI,
      up.Gender,
      up.address AS Address,
        up.pre_exisiting_metabolic_condition AS Pre_Existing_Condition,
      up.ee_call_summary AS Enrollment_Call_Summary,
      up.special_notes,
      up.city, 
      up.pincode,
      up.state,
      up.payment_date,
      JSON_EXTRACT(pr.payment_details, '$.id') AS Payment_ID,
      pr.status AS Payment_Status,
      pr.payment_link,
      pr.amount AS Plan_Amount,
      pr.payment_details,
      up.plan_enrolled AS Plan_Name,
      pr.payment_mode,
      up.acquisition_channel AS Channel,
      up.preferred_language,
      up.medicine_cost_pitched,
      up.personality_type AS MBTI,
      up.id AS User_ID,
      upa.adminprofile_id,
      pr.plan_id AS plan_id,
      up.updated_at AS profile_create




    FROM
      elevatenow_production.payment_request pr
      LEFT JOIN elevatenow_production.user_profile up  on pr.phone = up.phone
      LEFT JOIN elevatenow_production.admin_profile ap on pr.created_by_id = ap.id
      LEFT Join elevatenow_production.user_profile_admins as upa on up.id = upa.userprofile_id),

      CTE2 AS (
      Select CTE1.created_at, User_Name, User_Phone, User_email, EE_Name, EE_email, Age,
                CTE1.Gender, Address, 
                Pre_Existing_Condition, Enrollment_Call_Summary, special_notes,
                city, pincode, state, payment_date, Payment_ID, Payment_Status,
                payment_link, Plan_Amount, payment_details, Plan_Name,
                payment_mode, Channel, preferred_language, medicine_cost_pitched,
                MBTI, ap.email AS Health_Coach, ap.role, ap.phone AS HC_Phone, User_ID, plan_id,profile_create
      from CTE1 
      left join elevatenow_production.admin_profile ap on CTE1.adminprofile_id = ap.id),

      CTE3 AS (

      Select created_at, User_Name, User_Phone, User_email, EE_Name, EE_email, Age,
                Gender, Address, 
                Pre_Existing_Condition, Enrollment_Call_Summary, special_notes,
                city, pincode, state, payment_date, Payment_ID, Payment_Status,
                payment_link, Plan_Amount, payment_details, Plan_Name,
                payment_mode, Channel, preferred_language, medicine_cost_pitched,
                MBTI, User_ID, plan_id,profile_create,
                max(Case when role = 'health_coach' then Health_Coach end) AS HC_Name,
                max(Case when role = 'doctor' then Health_Coach end) AS Doctor_Name, 
                max(Case when role = 'health_coach' then HC_Phone end) AS HC_phone1,
                max(Case when role = 'doctor' then HC_phone end) AS DC_phone
      from CTE2
      group by  created_at, User_Name, User_Phone, User_email, EE_Name, EE_email, Age,
                Gender, Address, 
                Pre_Existing_Condition, Enrollment_Call_Summary, special_notes,
                city, pincode, state, payment_date, Payment_ID, Payment_Status,
                payment_link, Plan_Amount, payment_details, Plan_Name,
                payment_mode, Channel, preferred_language, medicine_cost_pitched,
                MBTI, User_ID, plan_id,profile_create),



      CTE4 AS (
      SELECT 
            pa.*, 
            up.full_name
        FROM 
            profile_answer pa
        JOIN 
            user_profile up 
        ON 
            pa.user_id = up.id
      ),
      CTE5 AS (
      SELECT 
        full_name,user_id,
        MAX(CASE WHEN `key` = 'weight' THEN answer END) AS Starting_weight,
        MAX(CASE WHEN `key` = 'height' THEN answer END) AS Height,
        MAX(CASE WHEN `key` = 'age' THEN answer END) AS Age1
    FROM 
        CTE4
    WHERE 
        `key` IN ('height', 'weight', 'age') 
    GROUP BY 
        full_name,user_id),
    CTE6 AS (
    Select created_at, User_Name, User_Phone, User_email, EE_Name, EE_email, (Case when Age is null then CTE5.Age1 else Age end) AS Age, 
                CTE5.Starting_weight,CTE5.Height,
                Gender, Address,
                Pre_Existing_Condition, Enrollment_Call_Summary, special_notes,
                city, pincode, state, payment_date, Payment_ID, Payment_Status,
                payment_link, Plan_Amount, payment_details, Plan_Name,
                payment_mode, Channel, preferred_language, medicine_cost_pitched,
                MBTI, HC_Name, HC_Phone1,plan_id, Doctor_Name, DC_phone,profile_create

    from CTE3 
    LEFT JOIN CTE5 on CTE3.User_ID = CTE5.user_id),

    CTE7 AS(
    select pt.quantity, p.id AS plan_id,p.name AS Plan_name, pr.product_name AS Product_name, p.type AS Plan_Type
    from plan_item pt
    LEFT join plan p on pt.plan_id = p.id
    LEFT join product pr on pt.product_id = pr.id),

    CTE8 AS (
    Select plan_id, Plan_name,Plan_Type,
    MAX(Case when Product_name = 'Blood Tests' then quantity end) AS Blood_Test_Quantity,
    MAX(Case when Product_name = '⁠Doctor Consultations' then quantity end) AS Doctor_Consultation_Quantity,
    MAX(Case when Product_name = 'Health Coach Calls' then quantity end) AS HC_Consultation_Quantity,
    MAX(Case when Product_name = 'Fitness Coach Calls' then quantity end) AS FC_Consultation_Quantity,
    MAX(Case when Product_name = 'CGM' then quantity end) AS CGM_Quantity
    from CTE7
    group by Plan_name,plan_id,Plan_Type)

    select created_at, User_Name, User_Phone, User_email, EE_Name, EE_email, Age, Starting_weight,Height,
                Gender, Address, CTE8.Plan_Name, 
                Pre_Existing_Condition, Enrollment_Call_Summary, special_notes,
                city, pincode, state, payment_date, Payment_ID, Payment_Status,
                payment_link, Plan_Amount, payment_details,
                payment_mode, Channel, preferred_language, medicine_cost_pitched,
                MBTI, HC_name, HC_Phone1,Doctor_Name,DC_phone, CTE8.Blood_Test_Quantity,CTE8.Doctor_Consultation_Quantity, CTE8.HC_Consultation_Quantity, 
                CTE8.FC_Consultation_Quantity, CTE8.CGM_Quantity,CTE8.Plan_Type,profile_create
    from CTE6
    left join CTE8 on CTE6.plan_id = CTE8.plan_id

    order by created_at desc
            """
            print("Fetching data from Metabase...")
            all_sql_rows3 = run_sql_query_and_get_all_rows(metabase_url, sql_query3, database_id)
            if not all_sql_rows3:
                print("No data fetched from Metabase.")
                return
            column_names = [
                "created_at", "User_Name", "User_Phone", "User_email", "EE_Name", "EE_email", "Age",
                "Starting_weight", "Height", "Gender", "Address", "Plan_Name",
                "Pre_Existing_Condition", "Enrollment_Call_Summary", "special_notes",
                "city", "pincode", "state", "payment_date", "Payment_ID", "Payment_Status",
                "payment_link", "Plan_Amount", "payment_details",
                "payment_mode", "Channel", "preferred_language", "medicine_cost_pitched",
                "MBTI", "Health_Coach", "HC_Phone", "Doctor", "Doctor_Phone", "Blood_Test_Quantity",
                "Doctor_Consultation_Quantity", "HC_Consultation_Quantity",
                "FC_Consultation_Quantity", "CGM_Quantity", "Plan Type", "profile_create"
            ]
            enroll = pd.DataFrame(all_sql_rows3, columns=column_names)

            # Process data
            print("Data fetched successfully. Cleaning data...")
            enroll['created_at'] = enroll['created_at'].apply(dateconvert)
            enroll['payment_date'] = enroll['payment_date'].apply(dateconvert)
            enroll['profile_create'] = enroll['profile_create'].apply(dateconvert)
            enroll = clean_data(enroll)
            print("Data cleaned. Preview of data:")

            # Upload to Google Sheets
            spreadsheet_id3 = '15zkCzGGD6KZIKFRRjxNylnlHErSYft4Gdm0j9pWSGzU'
            worksheet_name3 = 'enroll'
            print("Uploading data to Google Sheets...")
            upload_to_google_sheets(json_keyfile_path, enroll, spreadsheet_id3, worksheet_name3)
            print("Data uploaded successfully!")

        except Exception as e:
            print("An error occurred during fetch and upload:")
            print(traceback.format_exc())
pass

@app.route('/')
def fetch_and_upload():
    main()  # Call the function directly
    return "Data uploaded successfully!"

scheduler = BackgroundScheduler()

def scheduled_task():
    print("Running scheduled task...")
    main()
    print("Scheduled task completed.")

scheduler.add_job(scheduled_task, 'interval', minutes=15)  # Run every 15 minutes
scheduler.start()


if __name__ == '__main__':
    try:
        app.run(debug=False)
    except KeyboardInterrupt:
        print("Shutting down scheduler...")
        scheduler.shutdown()
