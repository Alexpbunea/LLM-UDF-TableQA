import json
import pandas as pd
import ast



class Metrics:
    def __init__(self, path):
        self.path = path
        self.data = self.load_json(path)

        self.TP = 0
        self.FP = 0
        self.TN = 0
        self.FN = 0
        self.all = 0

    def load_json(self, path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)


    def get_precision(self, name):
        # TP / (TP + FP)
        for response in self.data:
            my_result = response["expected_result"]
            try:
                ai_result = ast.literal_eval(response["ai_response"])
            except Exception as e:
                print(f"Skipping response due to parsing error: {e}")
                continue
            
            lista = []    
            if "summarization" in response["unique_id"]:
                for element in my_result:
                    lista.append(list(element.values())[0])
                self._handle_summarization(lista, ai_result, response["unique_id"])
            
            else:
                self._handle_regular_response(my_result, ai_result)
            
        if self.TP + self.FP == 0:
            print("Warning: No valid responses processed (TP + FP = 0)")
            return 0
            
        precision = self.TP / (self.TP + self.FP)
        print(f"Precision {name}: {precision}")
        return precision
        
    def _handle_summarization(self, my_result, ai_result, unique_id):
        """Handle summarization-specific precision calculation"""
        
        for index, item in enumerate(ai_result):
            
            if isinstance(item, list):
                #print(item[0])
                #print(my_result)
                if item[0] and item[0] in my_result :
                    self.TP += 1
                else:
                    self.FP += 1
                
                
            elif isinstance(item, dict):
                #print(list(item.values())[0])
                #print(my_result)
                if list(item.values())[0] in my_result:
                    self.TP += 1
                else:
                    self.FP += 1
            else:
                print(f"[ERROR]: Unrecognized type in summarization: {type(item)}")
    
    def _handle_regular_response(self, my_result, ai_result):
        """Handle regular (non-summarization) precision calculation"""
        for item in ai_result:
            if item in my_result:
                self.TP += 1
            else:
                self.FP += 1


zero_shot = Metrics("./jsons/responses_zero_shot.json")
three_shot = Metrics("./jsons/responses_three_shot.json")

zero_shot.get_precision("zero_shot")
three_shot.get_precision("three_shot")

