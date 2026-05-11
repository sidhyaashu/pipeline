import os
import json

DATA_DIR = "tests/data/penetration"
os.makedirs(DATA_DIR, exist_ok=True)

def save_case(filename, data):
    with open(os.path.join(DATA_DIR, f"{filename}.txt"), "w", encoding="utf-8") as f:
        json.dump(data, f)

# 1. Missing 'Table' key
save_case("Penetration_MissingTable", {"Foo": []})

# 2. 'Table' is not a list
save_case("Penetration_TableNotList", {"Table": "Not a list"})

# 3. Missing Primary Key (e.g., Fincode for Company_master)
save_case("Penetration_MissingPK", {"Table": [{"SCripCode": 123, "COMPNAME": "Test"}]})

# 4. Null Primary Key
save_case("Penetration_NullPK", {"Table": [{"FINCODE": None, "COMPNAME": "Test"}]})

# 5. Invalid Data Types (String instead of Int)
save_case("Penetration_WrongType", {"Table": [{"FINCODE": "NotAnInt", "COMPNAME": "Test"}]})

# 6. Malformed JSON (Handled by JSON loader usually, but let's see)
with open(os.path.join(DATA_DIR, "Penetration_MalformedJSON.txt"), "w") as f:
    f.write('{"Table": [{"FINCODE": 123, "COMPNAME": "Test"}]') # Missing closing brace

# 7. Mass Delete (If more than 40% rows have flag 'D' and count > 50)
rows = [{"FINCODE": i, "COMPNAME": f"Test {i}", "flag": "D"} for i in range(100)]
save_case("Penetration_MassDelete", {"Table": rows})

# 8. Extra Columns (Should be ignored)
save_case("Penetration_ExtraCols", {"Table": [{"FINCODE": 100001, "COMPNAME": "Test", "Extra_Col": "I should be ignored"}]})

# 9. Empty Strings in Numeric Columns
save_case("Penetration_EmptyNumeric", {"Table": [{"FINCODE": 100002, "SCRIPCODE": "   ", "COMPNAME": "Test"}]})

print(f"Generated {len(os.listdir(DATA_DIR))} penetration test cases in {DATA_DIR}")
