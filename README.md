# LLM-UDF-TableQA

For the project, four python scripts were build in order to:
1. Read CSVs with data from the BIRD benchmark in order to create the json datasets
2. Send the prompts to the LLM, in this case, **Gemini 2.5 flash**
3. Recieve the results and save them in a json file
4. Calculate the precision

------

## Json_creator.py

This file reads the three CSVs found in the repositoty containing information from tables of the BIRD benchamark. Those tables were:
- "Orders" from the "Car retails" dataset
- "Ratings" from the "Movie platform" dataset
- "Violations" from the "Food_inspection" dataset (the first one, no the second)

After reading those CSVs, stores the information in the following format:

```json
  {
    "unique_id": "...",
    "db_id": "...",
    "table_name": "...",
    "table_schema": "...",
    "question": "...",
    "expected_result": [
        ...
    ],
    "udf_justification": "..."
  }
```

Finaly, it is saved in the ```/jsons``` folder where there are two datasets, one for the *zero_shot* try and the other for the *three_shot*.


## Zero_shot.py

This file reads the *tables_dataset* json, which contains 21 items, and procceds to send a promp to Gemini. When Gemini answers, the information is saved in the *responses_zero_shot* json found in the same directory as the dataset.

## Three_shot.py

This file reads the *tables_dataset_three_shot* json, which contains 18 items, and procceds to send a promp with three examples of expected result to Gemini. When Gemini answers, the information is saved in the *responses_three_shot* json found in the same directory as the dataset.

## Metrics.py

This file reads both responses files and calculates the precision. Here, it is:
- **Precision zero_shot: 0.4362934362934363**
- **Precision three_shot: 0.4051724137931034**


## Conclusion

First, I found it difficult and too much work to go through the whole BIRD benchmark searching for tables that contained possible values that jutified the use of UDFs. Second, when looking for LLMs, *Gemini 2.5 flash* was the only one I found that was resonable good, performace wise, and free. Maybe, if I would have used other models, the results would have been better. Finally, talking about the results, mmmm, I know they are not the best and neither I expected them, but I just did my best. Honestly, I think the part of making the UDFs for the question and results, exhausted me too much, maybe the results are a reflection of that. Damn, it was a lot of work.