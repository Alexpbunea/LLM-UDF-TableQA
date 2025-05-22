from google import genai
import json
import time
import pandas as pd

with open("./jsons/tables_dataset_three_shot.json", "r", encoding="utf-8") as f:
    data = json.load(f)

paths = ("./orders_of_cars.csv", "./violations_on_food_inspections.csv", "./ratings_of_movies.csv",)
examples_list = []

three_shot_1 =   {
    "unique_id": "topic_extraction_006",
    "db_id": "food_inspection",
    "table_name": "violations",
    "table_schema": "CREATE TABLE `violations` (  `business_id` INTEGER NOT NULL,  `date` DATE NOT NULL,  `violation_type_id` TEXT NOT NULL,  `risk_category` TEXT NOT NULL,  `description` TEXT NOT NULL,  FOREIGN KEY (`business_id`) REFERENCES `businesses` (`business_id`) )",
    "question": "Which businesses have violations indicating unapproved or unmaintained equipment or utensils?",
    "expected_result": [
      10,
      45,
      56,
      61,
      77,
      95,
      108
    ],
    "udf_justification": "Identifying repeated mentions of equipment maintenance issues across multiple descriptions requires semantic topic classification. SQL cannot group variants of this theme; an LLM-based UDF is necessary."
  }
three_shot_2 = {
    "unique_id": "summarization_001",
    "db_id": "car_retails",
    "table_name": "orders",
    "table_schema": "CREATE TABLE orders (   orderNumber  INTEGER   not null     primary key,   orderDate   DATE    not null,   requiredDate  DATE    not null,   shippedDate  DATE,   status     TEXT not null,   comments    TEXT,   customerNumber INTEGER   not null,   foreign key (customerNumber) references customers(customerNumber) )",
    "question": "Summarize each order where the customer tried to renegotiate or cancel due to external offers or pricing concerns.",
    "expected_result": [
      {
        "orderID": 10167,
        "summary": "Customer called to cancel. The warehouse was notified in time and the order didn't ship. They have a new VP of Sales and are shifting their sales model."
      },
      {
        "orderNumber": 10262,
        "summary": "Customer found a better offer and wants to renegotiate the order."
      },
      {
        "orderNumber": 10239,
        "summary": "Customer wants to renegotiate the agreement."
      }
    ],
    "udf_justification": "Summarization UDF is needed to condense negotiation-related comments into short, meaningful descriptions. SQL cannot generate summaries from multi-clause, unstructured comments."
  }
three_shot_3 = {
    "unique_id": "sentiment_004",
    "db_id": "movie_platform",
    "table_name": "ratings",
    "table_schema": "CREATE TABLE ratings (   movie_id        INTEGER,   rating_id        INTEGER,   rating_url       TEXT,   rating_score      INTEGER,   rating_timestamp_utc  TEXT,   critic         TEXT,   critic_likes      INTEGER,   critic_comments     INTEGER,   user_id         INTEGER,   user_trialist      INTEGER,   user_subscriber     INTEGER,   user_eligible_for_trial INTEGER,   user_has_payment_method INTEGER,   foreign key (movie_id) references movies(movie_id),   foreign key (user_id) references lists_users(user_id),   foreign key (rating_id) references ratings(rating_id),   foreign key (user_id) references ratings_users(user_id) )",
    "question": "Which reviews express a highly positive opinion and mention personal emotional impact?",
    "expected_result": [
      8761221,
      14589535,
      14247813,
      14529999,
      6393245
    ],
    "udf_justification": "The review states that the movie 'left a mark on me', indicating a strong emotional reaction. SQL cannot detect sentiment intensity or personal impact from unstructured text. A UDF LLM is required to analyze emotional tone and subjective experience."
  }





for path in paths:
    #print("a")
    df = pd.read_csv(path, engine='python', index_col=False)
    examples_list.append(df)

prompts_and_expected = {}
for d in data:
    if d["db_id"] == "car_retails":
        examples = examples_list[0]
    elif d["db_id"] == "movie_platform":
        examples = examples_list[2]
        #continue
    elif d["db_id"] == "food_inspection":
        examples = examples_list[1]
        #continue
    else:
        raise ValueError(f"Database ID {d['db_id']} not found in examples list")
    expected = d["expected_result"]
    question = d["question"]
    unique_id = d["unique_id"]
    prompt = f"""Here are three examples of how I want you to answer.\n
    Question 1: {three_shot_1['question']}\n
    Expected Result 1: {three_shot_1['expected_result']}\n
    Question 2: {three_shot_2['question']}\n
    Expected Result 2: {three_shot_2['expected_result']}\n
    Question 3: {three_shot_3['question']}\n
    Expected Result 3: {three_shot_3['expected_result']}\n

    Once you undertand this, analize the following table:\n

    {examples.to_string(index=False)}\n

    Now, answer the following question:\n
    
    {question}
    """
    prompts_and_expected[unique_id] = {"prompt": prompt, "expected": expected, "question": question}

results_data = []
output_json_path = "./jsons/responses_three_shot.json"
#print(three_shot_2['expected_result'])
print(f"Processing {len(prompts_and_expected)} items...")

for unique_id, item_data in prompts_and_expected.items():
    prompt_to_send = item_data["prompt"]
    expected_result = item_data["expected"]
    original_question = item_data["question"]

    print(f"Sending prompt for unique_id: {unique_id}...")
    try:
        client = genai.Client(api_key="AIzaSyBd3CbjFZ5UuyPYZPSw_sZa_ooWg2SACr4")
        response = client.models.generate_content(
            model="gemini-2.5-flash-preview-04-17", contents=prompt_to_send
        )
        ai_response_text = response.text
        print("AI Response:")
        print(ai_response_text)
    except Exception as e:
        print(f"Error calling AI for {unique_id}: {e}")
        ai_response_text = f"Error: {e}"
    
    print("Expected Result:")
    print(expected_result, "\n\n\n")

    results_data.append({
        "unique_id": unique_id,
        "question": original_question,
        "prompt_sent": prompt_to_send,
        "expected_result": expected_result,
        "ai_response": ai_response_text
    })
    
    print(f"Waiting for 2 seconds before next request for unique_id: {unique_id}...")
    time.sleep(2)

try:
    with open(output_json_path, "w", encoding="utf-8") as outfile:
        json.dump(results_data, outfile, indent=4, ensure_ascii=False)
    print(f"Successfully wrote AI responses to {output_json_path}")
except Exception as e:
    print(f"Error writing to JSON file: {e}")
