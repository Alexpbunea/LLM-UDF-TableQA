from google import genai
import json
import time
import pandas as pd

with open("./jsons/tables_dataset.json", "r", encoding="utf-8") as f:
    data = json.load(f)

paths = ("./orders_of_cars.csv", "./violations_on_food_inspections.csv", "./ratings_of_movies.csv",)
examples_list = []



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
    prompt = f"""Analize the following table:\n

    {examples.to_string(index=False)}\n

    Now, answer the following question by only giving the correctIDs in a list ([IDs]) and summarize if asked:\n
    
    {question}
    """
    prompts_and_expected[unique_id] = {"prompt": prompt, "expected": expected, "question": question}

results_data = []
output_json_path = "./jsons/responses.json"
#print(examples_list[0])
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
